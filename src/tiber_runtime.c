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
		// read one from the timer_queue

		// if tb is NULL, return BLOCKING

		// if the time has not elapsed yet, return the number of microseconds to wake up after

		// wake the tiber and continue
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