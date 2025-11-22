#include<tiber/tiber.h>

#include<stdio.h>
#include<stdlib.h>

// runtime for this thread is stored here
__thread tiber_runtime* thread_runtime = NULL;

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
static void* tiber_execute_wrapper(void* tb_v)
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