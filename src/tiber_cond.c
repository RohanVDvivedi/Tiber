#include<tiber/tiber.h>

#include<tiber/tiber_internals.h>

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

int tiber_cond_timedwait(tiber_cond* tc, tiber_mutex* tm, const struct timespec *abs_time)
{
	const struct timespec abstime_for_wakeup = (*abs_time);

	int need_to_wake_up_timer_job = 0;

	pthread_spin_lock(&(curr_tiber->state_lock));

	{
		curr_tiber->waiting_on_tiber_cond = tc;

		pthread_spin_lock(&(tc->lock));

		insert_tail_in_linkedlist(&(tc->waiting_tibers), curr_tiber);

		pthread_spin_unlock(&(tc->lock));
	}

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

	// unlock the mutex after putting the self in waiting
	tiber_mutex_unlock(tm);

	// we inserted ourselves to timer_queue, so wake up the timer_job
	if(need_to_wake_up_timer_job)
		wake_up_alarm_job(curr_tiber->runtime->timer_job);

	// switch back to the caller, and do not queue, we are waiting
	switch_from_this_tiber_to_caller_thread();

	// recapture mutex after comming back from waiting
	tiber_mutex_lock(tm);

	// make sure that the timeout has not expired, if so return ETIMEDOUT
	if(has_tiber_timeout_elapsed(abstime_for_wakeup, NULL))
		return ETIMEDOUT;

	return 0;
}

int tiber_cond_signal(tiber_cond* tc)
{
	int woke_up_some_tiber = 0;

	// loop while we have not woken up any tiber yet
	while(!woke_up_some_tiber)
	{
		pthread_spin_lock(&(tc->lock));

		tiber* tb = (tiber*) get_head_of_linkedlist(&(tc->waiting_tibers));
		if(tb != NULL)
			increment_tiber_reference_count(tb);

		pthread_spin_unlock(&(tc->lock));

		// try to wake it up
		if(tb != NULL)
		{
			woke_up_some_tiber = wake_up_waiting_tiber(tb);
			decrement_tiber_reference_count(tb);
		}
		else // if empty we break
			break;
	}

	return 0;
}

int tiber_cond_broadcast(tiber_cond* tc)
{
	while(1)
	{
		pthread_spin_lock(&(tc->lock));

		tiber* tb = (tiber*) get_head_of_linkedlist(&(tc->waiting_tibers));
		if(tb != NULL)
			increment_tiber_reference_count(tb);

		pthread_spin_unlock(&(tc->lock));

		// try to wake it up
		if(tb != NULL)
		{
			wake_up_waiting_tiber(tb);
			decrement_tiber_reference_count(tb);
		}
		else // if empty we break
			break;
	}

	return 0;
}