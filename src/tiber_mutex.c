#include<tiber/tiber.h>

#include<tiber/tiber_internals.h>

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
				pthread_spin_unlock(&(tm->lock));
				curr_tiber->waiting_on_tiber_mutex = NULL;
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

	int need_to_wake_up_timer_job = 0;

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
			if(has_tiber_timeout_elapsed(abstime_for_wakeup, NULL))
			{
				pthread_spin_unlock(&(tm->lock));
				curr_tiber->waiting_on_tiber_mutex = NULL;
				pthread_spin_unlock(&(curr_tiber->state_lock));
				return ETIMEDOUT;
			}

			insert_tail_in_linkedlist(&(tm->waiting_tibers), curr_tiber);

			pthread_spin_unlock(&(tm->lock));
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

		// we inserted ourselves to timer_queue, so wake up the timer_job
		if(need_to_wake_up_timer_job)
			wake_up_alarm_job(curr_tiber->runtime->timer_job);

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