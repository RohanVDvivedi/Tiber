#include<tiber/tiber.h>

#include<tiber/tiber_internals.h>

#include<tiber/tiber_result.h>

#include<stdio.h>
#include<stdlib.h>

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
			return BLOCKING;

		// if the timeout has not elapsed, then go back to waiting
		uint64_t microseconds_to_wake_up_in;
		if(!has_tiber_timeout_elapsed(abstime_for_wakeup, &microseconds_to_wake_up_in))
		{
			decrement_tiber_reference_count(tb);
			return microseconds_to_wake_up_in;
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
	wake_up_alarm_job(tr->timer_job);

	return tr;
}

void delete_tiber_runtime(tiber_runtime* tr)
{
	shutdown_executor(tr->thread_pool, 1);
	wait_for_all_executor_workers_to_complete(tr->thread_pool);
	delete_executor(tr->thread_pool);

	delete_alarm_job(tr->timer_job);

	pthread_spin_destroy(&(tr->timer_lock));

	free(tr);
}

// tiber functions

tiber* new_tiber(tiber_runtime* tr, void* (*entry_func)(void* input_p), void* input_p, uint64_t stack_size, int is_detached)
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
	initialize_tiber_result(&(tb->result));

	pthread_spin_init(&(tb->context_lock), PTHREAD_PROCESS_PRIVATE);

	getcontext(&(tb->context));
	tb->context.uc_stack.ss_sp = tb->stack;
	tb->context.uc_stack.ss_size = stack_size;
	tb->context.uc_stack.ss_flags = 0;
	tb->context.uc_link = NULL; // we ill be migrating stacks to run this tiber so do not use uc_link
	makecontext(&(tb->context), (void(*)())tiber_entry_wrapper, 0);

	pthread_spin_init(&(tb->state_lock), PTHREAD_PROCESS_PRIVATE);
	tb->state = TIBER_QUEUED;
	tb->is_detached = is_detached;

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

void delete_tiber(tiber* tb)
{
	deinitialize_tiber_result(&(tb->result));
	pthread_spin_destroy(&(tb->context_lock));
	pthread_spin_destroy(&(tb->state_lock));
	pthread_spin_destroy(&(tb->reference_count_lock));

	free(tb->stack);
	free(tb);
}

int tiber_join(tiber* tb, void** return_value)
{
	pthread_spin_lock(&(tb->state_lock));
	int can_be_joined = !(tb->is_detached);
	pthread_spin_unlock(&(tb->state_lock));

	if(!can_be_joined)
		return EINVAL;

	// we got the return value, it must now have been killed so delete the tiber
	(*return_value) = get_tiber_result(&(tb->result));

	delete_tiber(tb);

	return 0;
}

int tiber_detach(tiber* tb)
{
	int need_to_delete_tiber = 0;

	pthread_spin_lock(&(tb->state_lock));

	if(!(tb->is_detached)) // move forward only if it was joinable
	{
		tb->is_detached = 1;
		if(tb->state == TIBER_KILLED)
			need_to_delete_tiber = 1;
	}

	pthread_spin_unlock(&(tb->state_lock));

	// let it set its result, then we delete it
	if(need_to_delete_tiber)
	{
		get_tiber_result(&(tb->result));
		delete_tiber(tb);
	}

	return 0;
}

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

void tiber_abs_sleep(const struct timespec *abs_time)
{
	const struct timespec abstime_for_wakeup = (*abs_time);

	int need_to_wake_up_timer_job = 0;

	pthread_spin_lock(&(curr_tiber->state_lock));

	{
		curr_tiber->is_timer_set = 1;
		curr_tiber->abstime_for_wakeup = abstime_for_wakeup;

		pthread_spin_lock(&(curr_tiber->runtime->timer_lock));

		push_to_pheap(&(curr_tiber->runtime->timer_queue), curr_tiber);
		need_to_wake_up_timer_job = (curr_tiber == get_top_of_pheap(&(curr_tiber->runtime->timer_queue)));

		pthread_spin_unlock(&(curr_tiber->runtime->timer_lock));
	}

	curr_tiber->state = TIBER_WAITING;

	pthread_spin_unlock(&(curr_tiber->state_lock));

	// we inserted ourselves to timer_queue, so wake up the timer_job
	if(need_to_wake_up_timer_job)
		wake_up_alarm_job(curr_tiber->runtime->timer_job);

	// switch back to the caller, and do not queue, we are waiting
	switch_from_this_tiber_to_caller_thread();
}

void tiber_sleep(uint64_t seconds)
{
	struct timespec abs_time = timespec_add(tiber_now(), timespec_from_seconds(seconds));

	tiber_abs_sleep(&abs_time);
}

void tiber_msleep(uint64_t milliseconds)
{
	struct timespec abs_time = timespec_add(tiber_now(), timespec_from_milliseconds(milliseconds));

	tiber_abs_sleep(&abs_time);
}

void tiber_usleep(uint64_t microseconds)
{
	struct timespec abs_time = timespec_add(tiber_now(), timespec_from_microseconds(microseconds));

	tiber_abs_sleep(&abs_time);
}

struct timespec tiber_now()
{
	struct timespec now_time;
	clock_gettime(CLOCK_MONOTONIC, &now_time);
	return now_time;
}

int has_tiber_timeout_elapsed(struct timespec abs_time, uint64_t* microseconds_left)
{
	uint64_t microseconds_left_TEMP = 0;
	if(microseconds_left == NULL)
		microseconds_left = &microseconds_left_TEMP;

	(*microseconds_left) = 0;

	// fetch the current time
	struct timespec now_time = tiber_now();

	// if current time >= abs_time, we say the timeout has elapsed
	if(timespec_compare(now_time, abs_time) >= 0)
		return 1;

	// else if the microseconds left is lesser than 3, then too the timer is said to have been elapsed
	(*microseconds_left) = timespec_to_microseconds(timespec_sub(abs_time, now_time));
	if((*microseconds_left) < 3)
		return 1;

	return 0;
}