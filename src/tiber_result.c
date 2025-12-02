#include<tiber/tiber_result.h>

extern __thread tiber* curr_tiber;

void initialize_tiber_result(tiber_result* tres)
{
	pthread_spin_init(&(tres->lock0), PTHREAD_PROCESS_PRIVATE);

	tres->is_result_set = 0;
	tres->result = NULL;

	pthread_mutex_init(&(tres->lock1), NULL);
	pthread_cond_init_with_monotonic_clock(&(tres->wait1));

	tiber_mutex_init(&(tres->lock2));
	tiber_cond_init(&(tres->wait2));
}

void set_tiber_result(tiber_result* tres, void* result)
{
	// set result with lock0 held
	pthread_spin_lock(&(tres->lock0));
	tres->is_result_set = 1;
	tres->result = result;
	pthread_spin_unlock(&(tres->lock0));

	// wake up pthreads
	pthread_mutex_lock(&(tres->lock1));
	pthread_cond_broadcast(&(tres->wait1));
	pthread_mutex_unlock(&(tres->lock1));

	// wake up tibers
	tiber_mutex_lock(&(tres->lock2));
	tiber_cond_broadcast(&(tres->wait2));
	tiber_mutex_unlock(&(tres->lock2));
}

static int _is_tiber_result_set(tiber_result* tres, void** result)
{
	int res = 0;

	pthread_spin_lock(&(tres->lock0));
	if(tres->is_result_set)
	{
		res = 1;
		(*result) = tres->result;
	}
	pthread_spin_unlock(&(tres->lock0));

	return res;
}

void* get_tiber_result(tiber_result* tres)
{
	void* result = NULL;

	if(curr_tiber == NULL) // this is being called from a pthread
	{
		pthread_mutex_lock(&(tres->lock1));
		while(!_is_tiber_result_set(tres, &result))
			pthread_cond_wait(&(tres->wait1), &(tres->lock1));
		pthread_mutex_unlock(&(tres->lock1));
	}
	else // this is being called from a tiber
	{
		tiber_mutex_lock(&(tres->lock2));
		while(!_is_tiber_result_set(tres, &result))
			tiber_cond_wait(&(tres->wait2), &(tres->lock2));
		tiber_mutex_unlock(&(tres->lock2));
	}

	return result;
}

void deinitialize_tiber_result(tiber_result* tres)
{
	tres->is_result_set = 0;
	tres->result = NULL;

	pthread_mutex_destroy(&(tres->lock1));
	pthread_cond_destroy(&(tres->wait1));

	tiber_mutex_destroy(&(tres->lock2));
	tiber_cond_destroy(&(tres->wait2));
}