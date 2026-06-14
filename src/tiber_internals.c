#include<tiber/tiber_internals.h>

#include<tiber/tiber_result.h>

#include<stdio.h>

// the current tiber that this thread is executing get's stored here
__thread tiber* curr_tiber = NULL;

/*
	puts tiber as thread local variable of this thread
	puts tiber in running state
	takes it's context lock
	swaps context running the tiber
	releases it's context lock
	returns

	** only this function takes the context_lock of the tiber for a meaningful reason
*/
static void* tiber_job_func(void* tb_v)
{
	// set the thread local
	curr_tiber = (tiber*)tb_v;

	int need_to_delete_tiber = 0;

	// we can transition a tiber in-to or out-of RUNNING state only while the context_lock is held
	pthread_spin_lock(&(curr_tiber->context_lock));
	{
		// change curr_tiber's state to running
		pthread_spin_lock(&(curr_tiber->state_lock));
		curr_tiber->state = TIBER_RUNNING;
		pthread_spin_unlock(&(curr_tiber->state_lock));

		// swap the context to run the tiber
		ucontext_t thread_context;
		curr_tiber->thread_context = &thread_context;
		if(-1 == getcontext(curr_tiber->thread_context))
		{
			printf("TIBER BUG: thread_context could not be populated\n");
			exit(-1);
		}
		if(-1 == swapcontext(curr_tiber->thread_context, &(curr_tiber->context)))
		{
			printf("TIBER BUG: tiber could not context switch into itself\n");
			exit(-1);
		}
		curr_tiber->thread_context = NULL;

		// change curr_tiber's state if running to killed
		// because it returned from the entry function
		pthread_spin_lock(&(curr_tiber->state_lock));
		if(curr_tiber->state == TIBER_RUNNING)
		{
			curr_tiber->state = TIBER_KILLED;
			if(curr_tiber->is_detached)
				need_to_delete_tiber = 1;	// if it was detached while we killed it, we need to delete it too
		}
		pthread_spin_unlock(&(curr_tiber->state_lock));
	}
	pthread_spin_unlock(&(curr_tiber->context_lock));

	// reset the thread local
	curr_tiber = NULL;

	// now we are out of tiber's execution so proceed as if we are a thread, and not a tiber

	if(need_to_delete_tiber)
	{
		// loop continuously while it's reference count does not reach 0
		while(fetch_tiber_reference_count((tiber*)tb_v) > 0){}

		delete_tiber((tiber*)tb_v);
	}

	return NULL;
}

void switch_from_this_tiber_to_caller_thread()
{
	// swap the context out, to start running the caller thread
	if(-1 == swapcontext(&(curr_tiber->context), curr_tiber->thread_context))
	{
		printf("TIBER BUG: tiber could not context switch into it's caller thread\n");
		exit(-1);
	}
}

void queue_tiber_to_runime(tiber* tb)
{
	if(0 == submit_job_executor(tb->runtime->thread_pool, tiber_job_func, tb, NULL, (void (*)(void*))delete_tiber, BLOCKING))
	{
		printf("TIBER BUG: tiber was suppoed to be queued but it failed\n");
		exit(-1);
	}
}

int wake_up_waiting_tiber(tiber* tb)
{
	int result = 0;

	pthread_spin_lock(&(tb->state_lock));

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

	pthread_spin_unlock(&(tb->state_lock));

	if(result)
		queue_tiber_to_runime(tb);

	return result;
}

void increment_tiber_reference_count(tiber* tb)
{
	pthread_spin_lock(&(tb->reference_count_lock));
	++tb->reference_count;
	pthread_spin_unlock(&(tb->reference_count_lock));
}

void decrement_tiber_reference_count(tiber* tb)
{
	pthread_spin_lock(&(tb->reference_count_lock));
	--tb->reference_count;
	pthread_spin_unlock(&(tb->reference_count_lock));
}

uint64_t fetch_tiber_reference_count(tiber* tb)
{
	pthread_spin_lock(&(tb->reference_count_lock));
	uint64_t reference_count = tb->reference_count;
	pthread_spin_unlock(&(tb->reference_count_lock));
	return reference_count;
}