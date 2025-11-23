#ifndef TIBER_RESULT_H
#define TIBER_RESULT_H

#include<tiber/tiber.h>

typedef struct tiber_result tiber_result;
struct tiber_result
{
	int is_result_set;
	void* result;

	pthread_mutex_t lock1;
	pthread_cond_t wait1;

	tiber_mutex lock2;
	tiber_cond wait2;
};

void initialize_tiber_result(tiber_result* tres);

void set_tiber_result(tiber_result* tres, void* result);

void* get_tiber_result(tiber_result* tres);

void deinitialize_tiber_result(tiber_result* tres);

#endif