#include<tiber/tiber_result.h>

extern __thread tiber* curr_tiber;

void initialize_tiber_result(tiber_result* tres)
{
	tres->is_result_set = 0;
	tres->result = NULL;

	pthread_mutex_init(&(tres->lock1), NULL);
	pthread_cond_init_with_monotonic_clock(&(tres->wait1));

	tiber_mutex_init(&(tres->lock2));
	tiber_cond_init(&(tres->wait2));
}

void set_tiber_result(tiber_result* tres, void* result)
{
	pthread_mutex_lock(&(tres->lock1));
	tiber_mutex_lock(&(tres->lock2));

	tres->is_result_set = 1;
	tres->result = result;

	pthread_cond_broadcast(&(tres->wait1));
	tiber_cond_broadcast(&(tres->wait2));

	tiber_mutex_unlock(&(tres->lock2));
	pthread_mutex_unlock(&(tres->lock1));
}

void* get_tiber_result(tiber_result* tres)
{
	void* result = NULL;

	if(curr_tiber == NULL)
	{
		pthread_mutex_lock(&(tres->lock1));
		while(!(tres->is_result_set))
			pthread_cond_wait(&(tres->wait1), &(tres->lock1));
		result = tres->result;
		pthread_mutex_unlock(&(tres->lock1));
	}
	else
	{
		tiber_mutex_lock(&(tres->lock2));
		while(!(tres->is_result_set))
			tiber_cond_wait(&(tres->wait2), &(tres->lock2));
		result = tres->result;
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