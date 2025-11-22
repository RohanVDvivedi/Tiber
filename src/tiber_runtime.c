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
	if(0 == submit_job_executor(tb->runtime->thread_pool, tiber_execute_wrapper, tb, NULL, NULL, BLOCKING))
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

		// discard from tiber_cond->waiters
		if(tb->waiting_on_tiber_cond)
		{
			pthread_spin_lock(&(tb->waiting_on_tiber_cond->lock));

			remove_from_linkedlist(&(tb->waiting_on_tiber_cond->waiters), tb);

			pthread_spin_unlock(&(tb->waiting_on_tiber_cond->lock));

			tb->waiting_on_tiber_cond = NULL;
		}

		// discard from tiber_mutex->waiters
		if(tb->waiting_on_tiber_mutex)
		{
			pthread_spin_lock(&(tb->waiting_on_tiber_mutex->lock));

			remove_from_linkedlist(&(tb->waiting_on_tiber_mutex->waiters), tb);

			pthread_spin_unlock(&(tb->waiting_on_tiber_mutex->lock));

			tb->waiting_on_tiber_mutex = NULL;
		}

		// discard from tiber_mutex->waiters
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