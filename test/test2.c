#include<tiber/tiber.h>

#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>

#define RUNTIME_THREADS_COUNT 	16
#define STACK_SIZE              24*1024

#define INCREMENTS_PER_TASK    10000ULL

#define TASKS_WITHOUT_TIMEOUTS 1000ULL
#define TASKS_WITH_TIMEOUTS    1000ULL

tiber_mutex counter_lock;
unsigned long long int counter = 0;

tiber_mutex missed_counter_lock;
unsigned long long int missed_counter = 0;

void* tb1_func(void* p)
{
	for(unsigned long long int i =0; i < INCREMENTS_PER_TASK; i++)
	{
		tiber_mutex_lock(&counter_lock);
		counter++;
		tiber_mutex_lock(&counter_lock);
	}

	return NULL;
}

void* tb2_func(void* p)
{
	for(unsigned long long int i =0; i < INCREMENTS_PER_TASK; i++)
	{
		tiber_mutex_lock(&counter_lock);
		counter++;
		tiber_mutex_lock(&counter_lock);
	}

	return NULL;
}

int main()
{
	tiber_mutex_init(&counter_lock);
	tiber_mutex_init(&missed_counter_lock);

	tiber_runtime* tr = new_tiber_runtime(RUNTIME_THREADS_COUNT, STACK_SIZE);

	tiber* tb1[TASKS_WITHOUT_TIMEOUTS] = {};
	tiber* tb2[TASKS_WITH_TIMEOUTS] = {};

	for(unsigned long long int i = 0; i < TASKS_WITHOUT_TIMEOUTS; i++)
		tb1[i] = new_tiber(tr, tb1_func, NULL, 4096, 0);

	for(unsigned long long int i = 0; i < TASKS_WITH_TIMEOUTS; i++)
		tb2[i] = new_tiber(tr, tb2_func, NULL, 4096, 0);

	void* result = NULL;

	for(unsigned long long int i = 0; i < TASKS_WITHOUT_TIMEOUTS; i++)
		tiber_join(tb1[i], &result);

	for(unsigned long long int i = 0; i < TASKS_WITH_TIMEOUTS; i++)
		tiber_join(tb2[i], &result);

	delete_tiber_runtime(tr);

	tiber_mutex_destroy(&counter_lock);
	tiber_mutex_destroy(&missed_counter_lock);

	printf("expected increments = %llu\n", (TASKS_WITHOUT_TIMEOUTS + TASKS_WITH_TIMEOUTS) * INCREMENTS_PER_TASK);
	printf("total increments = %llu\n", counter);
	printf("total increments = %llu\n", missed_counter);

	printf("TEST COMPLETE\n");

	return 0;
}