#include<tiber/tiber.h>

#include<stdio.h>
#include<stdlib.h>

// the current tiber that this thread is executing get's stored here
__thread tiber* curr_tiber = NULL;

// the context that this tiber must return to is stored here for each of the threads, it is the context of the thread that this tiber must return to after execution
__thread ucontext_t thread_context;

/*
	puts tiber as thread local variable of this thread
	puts tiber in running state
	takes it's context lock
	swaps context running the tiber
	releases it's context lock
	returns

	** only this function takes the context_lock of the tiber
*/
static void* tiber_job_func(void* tb_v)
{
	// set the thread local
	curr_tiber = (tiber*)tb_v;

	// change curr_tiber's state to running
	pthread_spin_lock(&(curr_tiber->state_lock));
	if(curr_tiber->state == TIBER_QUEUED)
		curr_tiber->state = TIBER_RUNNING;
	else
	{
		printf("TIBER BUG: tiber was queued to thread pool but is not in queued state\n");
		exit(-1);
	}
	pthread_spin_unlock(&(curr_tiber->state_lock));

	// run curr_tiber, we only use this lock here and nowhere else
	pthread_spin_lock(&(curr_tiber->context_lock));
	if(-1 == getcontext(&thread_context))
	{
		printf("TIBER BUG: thread_context could not be populated\n");
		exit(-1);
	}
	curr_tiber->context.uc_link = &thread_context;
	if(-1 == swapcontext(&thread_context, &(curr_tiber->context)))
	{
		printf("TIBER BUG: tiber could not context switch into itself\n");
		exit(-1);
	}
	pthread_spin_unlock(&(curr_tiber->context_lock));

	// change curr_tiber's state if running to killed
	// because it returned from the entry function
	pthread_spin_lock(&(curr_tiber->state_lock));
	if(curr_tiber->state == TIBER_RUNNING)
		curr_tiber->state = TIBER_KILLED;
	pthread_spin_unlock(&(curr_tiber->state_lock));

	// reset the thread local
	curr_tiber = NULL;

	return NULL;
}

// this function helps tiber actually have a return value, helpful when joining with the tiber
static void tiber_exec_returner(void* tb_v)
{
	tiber* tb = tb_v;
	tb->return_value = tb->entry_func(tb->input_p);
}

static inline void switch_from_this_tiber_to_caller_thread()
{
	// swap the context out
	if(-1 == swapcontext(&(curr_tiber->context), &thread_context))
	{
		printf("TIBER BUG: tiber could not context switch into it's caller thread\n");
		exit(-1);
	}
}

static inline void queue_tiber_to_runime(tiber* tb)
{
	if(0 == submit_job_executor(tb->runtime->thread_pool, tiber_job_func, tb, NULL, NULL, BLOCKING))
	{
		printf("TIBER BUG: tiber was suppoed to be queued but it failed\n");
		exit(-1);
	}
}

static inline int wake_up_waiting_tiber(tiber* tb)
{
	int result = 0;

	pthread_spin_lock(&(curr_tiber->state_lock));

	if(tb->state == TIBER_WAITING)
	{
		result = 1;

		tb->state = TIBER_QUEUED;

		// discard from tiber_cond->waiting_tibers
		if(tb->waiting_on_tiber_cond)
		{
			pthread_spin_lock(&(tb->waiting_on_tiber_cond->lock));

			remove_from_linkedlist(&(tb->waiting_on_tiber_cond->waiting_tibers), tb);

			pthread_spin_unlock(&(tb->waiting_on_tiber_cond->lock));

			tb->waiting_on_tiber_cond = NULL;
		}

		// discard from tiber_mutex->waiting_tibers
		if(tb->waiting_on_tiber_mutex)
		{
			pthread_spin_lock(&(tb->waiting_on_tiber_mutex->lock));

			remove_from_linkedlist(&(tb->waiting_on_tiber_mutex->waiting_tibers), tb);

			pthread_spin_unlock(&(tb->waiting_on_tiber_mutex->lock));

			tb->waiting_on_tiber_mutex = NULL;
		}

		// discard from timer_queue
		if(tb->is_timer_set)
		{
			pthread_spin_lock(&(tb->runtime->timer_lock));

			remove_from_pheap(&(tb->runtime->timer_queue), tb);

			pthread_spin_unlock(&(tb->runtime->timer_lock));

			tb->is_timer_set = 0;
		}
	}

	pthread_spin_unlock(&(curr_tiber->state_lock));

	queue_tiber_to_runime(tb);

	return result;
}

static inline uint64_t increment_tiber_reference_count(tiber* tb)
{
	pthread_spin_lock(&(tb->reference_count_lock));
	uint64_t reference_count = (++tb->reference_count);
	pthread_spin_unlock(&(tb->reference_count_lock));
	return reference_count;
}

static inline uint64_t decrement_tiber_reference_count(tiber* tb)
{
	pthread_spin_lock(&(tb->reference_count_lock));
	uint64_t reference_count = (--tb->reference_count);
	pthread_spin_unlock(&(tb->reference_count_lock));
	return reference_count;
}

static inline uint64_t fetch_tiber_reference_count(tiber* tb)
{
	pthread_spin_lock(&(tb->reference_count_lock));
	uint64_t reference_count = tb->reference_count;
	pthread_spin_unlock(&(tb->reference_count_lock));
	return reference_count;
}

static int compare_tibers_by_their_abstime_for_wakeup(const void* tb1_v, const void* tb2_v)
{
	return timespec_compare(((const tiber*)tb1_v)->abstime_for_wakeup, ((const tiber*)tb2_v)->abstime_for_wakeup);
}

static uint64_t timer_job_func(void* tr_v)
{
	tiber_runtime* tr = tr_v;

	while(1)
	{
		// tiber that needs to be woken up
		tiber* tb = NULL;
		struct timespec abstime_for_wakeup;

		// read one from the timer_queue
		pthread_spin_lock(&(tr->timer_lock));

		tb = (tiber*) get_top_of_pheap(&(tr->timer_queue));
		if(tb != NULL)
		{
			increment_tiber_reference_count(tb);
			abstime_for_wakeup = tb->abstime_for_wakeup;
		}

		pthread_spin_unlock(&(tr->timer_lock));

		// if tb is NULL, return BLOCKING
		if(tb == NULL)
		{
			decrement_tiber_reference_count(tb);
			return BLOCKING;
		}

		struct timespec now_time;
		clock_gettime(CLOCK_MONOTONIC, &now_time);

		// if the time has not elapsed yet, return the number of microseconds to wake up after
		int timer_elapsed = (timespec_compare(now_time, abstime_for_wakeup) >= 0);

		if(!timer_elapsed)
		{
			uint64_t microseconds_to_wake_up_in = timespec_to_microseconds(timespec_sub(abstime_for_wakeup, now_time));
			if(microseconds_to_wake_up_in > 3) // go to sleep only if it is less than 3 microseconds, else wake it up
			{
				decrement_tiber_reference_count(tb);
				return microseconds_to_wake_up_in;
			}
		}

		// wake the tiber and continue
		wake_up_waiting_tiber(tb);
		decrement_tiber_reference_count(tb);
	}

	return BLOCKING;
}

// tiber runtime functions

tiber_runtime* new_tiber_runtime(uint64_t thread_count, uint64_t stack_size)
{
	tiber_runtime* tr = malloc(sizeof(tiber_runtime));
	if(tr == NULL)
	{
		printf("TIBER BUG: tiber runtime creation failed\n");
		exit(-1);
	}

	pthread_spin_init(&(tr->timer_lock), PTHREAD_PROCESS_PRIVATE);

	initialize_pheap(&(tr->timer_queue), MIN_HEAP, LEFTIST, &simple_comparator(compare_tibers_by_their_abstime_for_wakeup), offsetof(tiber, embed_node_for_tiber_timer_queue));

	tr->thread_pool = new_executor(FIXED_THREAD_COUNT_EXECUTOR, thread_count, JOB_QUEUE_AS_LINKEDLIST, 0, NULL, NULL, NULL, stack_size);
	if(tr->thread_pool == NULL)
	{
		printf("TIBER BUG: tiber runtime, thread_pool creation failed\n");
		exit(-1);
	}

	tr->timer_job = new_alarm_job(timer_job_func, tr);
	if(tr->timer_job == NULL)
	{
		printf("TIBER BUG: tiber runtime, timer_job creation failed\n");
		exit(-1);
	}

	return tr;
}

void delete_tiber_runtime(tiber_runtime* tr_p)
{

}

// tiber mutex functions

int tiber_mutex_init(tiber_mutex* tm)
{
	tm->is_locked = 0;
	pthread_spin_init(&(tm->lock), PTHREAD_PROCESS_PRIVATE);
	initialize_linkedlist(&(tm->waiting_tibers), offsetof(tiber, embed_node_for_tiber_mutex_waiters));
	return 0;
}

int tiber_mutex_destroy(tiber_mutex* tm)
{
	tm->is_locked = 0;
	pthread_spin_destroy(&(tm->lock));
	return 0;
}

int tiber_mutex_trylock(tiber_mutex* tm)
{
	int result;

	pthread_spin_lock(&(tm->lock));

	if(tm->is_locked)
		result = EBUSY;
	else
	{
		result = 0;
		tm->is_locked = 1;
	}

	pthread_spin_unlock(&(tm->lock));

	return result;
}

int tiber_mutex_lock(tiber_mutex* tm)
{
	while(1)
	{
		pthread_spin_lock(&(curr_tiber->state_lock));

		{
			curr_tiber->waiting_on_tiber_mutex = tm;

			pthread_spin_lock(&(tm->lock));

			if(!(tm->is_locked))
			{
				// quickly grab the lock and exit
				tm->is_locked = 1;
				curr_tiber->waiting_on_tiber_mutex = NULL;
				pthread_spin_unlock(&(tm->lock));
				pthread_spin_unlock(&(curr_tiber->state_lock));
				break;
			}

			insert_tail_in_linkedlist(&(tm->waiting_tibers), curr_tiber);

			pthread_spin_unlock(&(tm->lock));
		}

		curr_tiber->state = TIBER_WAITING;

		pthread_spin_unlock(&(curr_tiber->state_lock));

		// switch back to the caller, and do not queue, we are waiting
		switch_from_this_tiber_to_caller_thread();
	}

	return 0;
}

int tiber_mutex_timedlock(tiber_mutex* tm, const struct timespec *abs_time)
{
	const struct timespec abstime_for_wakeup = (*abs_time);

	while(1)
	{
		pthread_spin_lock(&(curr_tiber->state_lock));

		{
			curr_tiber->waiting_on_tiber_mutex = tm;

			pthread_spin_lock(&(tm->lock));

			if(!(tm->is_locked))
			{
				tm->is_locked = 1;
				pthread_spin_unlock(&(tm->lock));
				curr_tiber->waiting_on_tiber_mutex = NULL;
				pthread_spin_unlock(&(curr_tiber->state_lock));
				break;
			}

			// make sure that the timeout has not expired
			{
				struct timespec now_time;
				clock_gettime(CLOCK_MONOTONIC, &now_time);
				if(timespec_compare(now_time, abstime_for_wakeup) >= 0)
				{
					pthread_spin_unlock(&(tm->lock));
					curr_tiber->waiting_on_tiber_mutex = NULL;
					pthread_spin_unlock(&(curr_tiber->state_lock));
					return ETIMEDOUT;
				}
			}

			insert_tail_in_linkedlist(&(tm->waiting_tibers), curr_tiber);

			pthread_spin_unlock(&(tm->lock));
		}

		{
			curr_tiber->is_timer_set = 1;
			curr_tiber->abstime_for_wakeup = abstime_for_wakeup;

			pthread_spin_lock(&(curr_tiber->runtime->timer_lock));

			push_to_pheap(&(curr_tiber->runtime->timer_queue), curr_tiber);

			pthread_spin_unlock(&(curr_tiber->runtime->timer_lock));
		}

		curr_tiber->state = TIBER_WAITING;

		pthread_spin_unlock(&(curr_tiber->state_lock));

		// switch back to the caller, and do not queue, we are waiting
		switch_from_this_tiber_to_caller_thread();
	}

	return 0;
}

int tiber_mutex_unlock(tiber_mutex* tm)
{
	// tiber to be woken up
	tiber* tb = NULL;

	pthread_spin_lock(&(tm->lock));

	tm->is_locked = 0;

	tb = (tiber*) get_head_of_linkedlist(&(tm->waiting_tibers));
	if(tb != NULL)
		increment_tiber_reference_count(tb);

	pthread_spin_unlock(&(tm->lock));

	if(tb != NULL)
	{
		wake_up_waiting_tiber(tb);
		decrement_tiber_reference_count(tb);
	}

	return 0;
}

// tiber condtion variable functions

int tiber_cond_init(tiber_cond* tc)
{
	pthread_spin_init(&(tc->lock), PTHREAD_PROCESS_PRIVATE);
	initialize_linkedlist(&(tc->waiting_tibers), offsetof(tiber, embed_node_for_tiber_cond_waiters));
	return 0;
}

int tiber_cond_destroy(tiber_cond* tc)
{
	pthread_spin_destroy(&(tc->lock));
	return 0;
}

int tiber_cond_wait(tiber_cond* tc, tiber_mutex* tm)
{
	pthread_spin_lock(&(curr_tiber->state_lock));

	{
		curr_tiber->waiting_on_tiber_cond = tc;

		pthread_spin_lock(&(tc->lock));

		insert_tail_in_linkedlist(&(tc->waiting_tibers), curr_tiber);

		pthread_spin_unlock(&(tc->lock));
	}

	curr_tiber->state = TIBER_WAITING;

	pthread_spin_unlock(&(curr_tiber->state_lock));

	// unlock the mutex after putting the self in waiting
	tiber_mutex_unlock(tm);

	// switch back to the caller, and do not queue, we are waiting
	switch_from_this_tiber_to_caller_thread();

	// recapture mutex after comming back from waiting
	tiber_mutex_lock(tm);

	return 0;
}

int tiber_cond_timedwait(tiber_cond* tc, tiber_mutex* tm, const struct timespec *abs_time);

int tiber_cond_signal(tiber_cond* tc)
{
	// tiber to be woken up
	tiber* tb = NULL;

	pthread_spin_lock(&(tc->lock));

	tb = (tiber*) get_head_of_linkedlist(&(tc->waiting_tibers));
	if(tb != NULL)
		increment_tiber_reference_count(tb);

	pthread_spin_unlock(&(tc->lock));

	if(tb != NULL)
	{
		wake_up_waiting_tiber(tb);
		decrement_tiber_reference_count(tb);
	}

	return 0;
}

int tiber_cond_broadcast(tiber_cond* tc)
{
	while(1)
	{
		// tiber to be woken up
		tiber* tb = NULL;

		pthread_spin_lock(&(tc->lock));

		tb = (tiber*) get_head_of_linkedlist(&(tc->waiting_tibers));
		if(tb != NULL)
			increment_tiber_reference_count(tb);

		pthread_spin_unlock(&(tc->lock));

		if(tb != NULL)
		{
			wake_up_waiting_tiber(tb);
			decrement_tiber_reference_count(tb);
		}
		else
			break;
	}

	return 0;
}

// tiber functions

tiber* new_tiber(tiber_runtime* tr, void* (*entry_func)(void* input_p), void* input_p, uint64_t stack_size)
{
	tiber* tb = malloc(sizeof(tiber));
	if(tb == NULL)
		return NULL;

	tb->stack = malloc(stack_size);
	if(tb->stack == NULL)
	{
		free(tb);
		return NULL;
	}

	tb->runtime = tr;

	tb->input_p = input_p;
	tb->entry_func = entry_func;
	tb->return_value = NULL;

	pthread_spin_init(&(tb->context_lock), PTHREAD_PROCESS_PRIVATE);

	getcontext(&(tb->context));
	tb->context.uc_stack.ss_sp = tb->stack;
	tb->context.uc_stack.ss_size = stack_size;
	tb->context.uc_stack.ss_flags = 0;
	makecontext(&(tb->context), (void(*)())tiber_exec_returner, 1, tb);

	pthread_spin_init(&(tb->state_lock), PTHREAD_PROCESS_PRIVATE);
	tb->state = TIBER_QUEUED;

	tb->waiting_on_tiber_cond = NULL;
	initialize_llnode(&(tb->embed_node_for_tiber_cond_waiters));

	tb->waiting_on_tiber_mutex = NULL;
	initialize_llnode(&(tb->embed_node_for_tiber_mutex_waiters));

	tb->is_timer_set = 0;
	tb->abstime_for_wakeup = (struct timespec){};
	initialize_phpnode(&(tb->embed_node_for_tiber_timer_queue));

	pthread_spin_init(&(tb->reference_count_lock), PTHREAD_PROCESS_PRIVATE);
	tb->reference_count = 0;

	queue_tiber_to_runime(tb);

	return tb;
}

int tiber_join(tiber* tb, void** return_value);
int tiber_detach(tiber* tb);

tiber* tiber_self(void)
{
	return curr_tiber;
}

void tiber_exit(void)
{
	// we switched from the tiber to the caller thread in running state so it will definitely kill us
	switch_from_this_tiber_to_caller_thread();
}

void tiber_yield(void)
{
	// first put this tiber in queued state
	pthread_spin_lock(&(curr_tiber->state_lock));
	curr_tiber->state = TIBER_QUEUED;
	pthread_spin_unlock(&(curr_tiber->state_lock));

	// then actually queue it
	queue_tiber_to_runime(curr_tiber);

	// now since we must yield, switch back to the caller thread of the run time
	switch_from_this_tiber_to_caller_thread();
}

void tiber_sleep(const struct timespec *abs_time);