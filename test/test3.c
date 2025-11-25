#include<tiber/tiber.h>

#include<cutlery/value_arraylist.h>

#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>

#define RUNTIME_THREADS_COUNT 	16
#define STACK_SIZE              532*1024

#define OPERATIONS_PER_TASK   10000ULL

#define PRODUCER_TASKS          1000ULL
#define CONSUMER_TASKS          PRODUCER_TASKS

#define TRANSFER_QUEUE_SIZE		1000ULL

#define TOTAL_INTS_SHUFFLED (OPERATIONS_PER_TASK * PRODUCER_TASKS)

data_definitions_value_arraylist(int_queue, int)
declarations_value_arraylist(int_queue, int, static inline) // last parameter can be empty
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(int_queue, int, static inline) // last parameter can be empty

int compare_ints(const void* a, const void* b)
{
	return compare_numbers(((const int*)a), ((const int*)b));
}

int_queue transfer;
int_queue result;
int_queue missed;

tiber_mutex lock;
tiber_cond full_wait;
tiber_cond empty_wait;

int produce_int(int_queue* iq, int v, struct timespec* abstime)
{
	tiber_mutex_lock(&lock);

	int wait_error = 0;
	while(is_full_int_queue(iq) && !wait_error)
	{
		if(abstime == NULL)
			wait_error = tiber_cond_wait(&full_wait, &lock);
		else
			wait_error = tiber_cond_timedwait(&full_wait, &lock, abstime);
	}

	int pushed = push_back_to_int_queue(iq, &v);
	if(pushed)
		tiber_cond_signal(&empty_wait);

	tiber_mutex_unlock(&lock);

	return pushed;
}

int consume_int(int_queue* iq, int* v, struct timespec* abstime)
{
	int popped = 0;

	tiber_mutex_lock(&lock);

	int wait_error = 0;
	while(is_empty_int_queue(iq) && !wait_error)
	{
		if(abstime == NULL)
			wait_error = tiber_cond_wait(&empty_wait, &lock);
		else
			wait_error = tiber_cond_timedwait(&empty_wait, &lock, abstime);
	}

	if(!is_empty_int_queue(iq))
	{
		(*v) = *get_front_of_int_queue(iq);
		popped = pop_front_from_int_queue(iq);
	}

	if(popped)
		tiber_cond_signal(&full_wait);

	tiber_mutex_unlock(&lock);

	return popped;
}

void* producer_func(void* p)
{
	int producer_id = ((uintptr_t)p);
	for(unsigned long long int i = 0; i < OPERATIONS_PER_TASK; i++)
	{
		int to_produce_value = (producer_id * OPERATIONS_PER_TASK + i);

		struct timespec wait_until = timespec_add(tiber_now(), timespec_from_microseconds(30));
		if(!produce_int(&transfer, to_produce_value, &wait_until))
			produce_int(&missed, to_produce_value, NULL);
	}

	return NULL;
}

void* consumer_func(void* p)
{
	for(unsigned long long int i = 0; i < OPERATIONS_PER_TASK; i++)
	{
		int consumed_value;

		struct timespec wait_until = timespec_add(tiber_now(), timespec_from_microseconds(30));
		if(consume_int(&transfer, &consumed_value, &wait_until))
			produce_int(&result, consumed_value, NULL);
	}

	return NULL;
}

int main()
{
	initialize_int_queue(&transfer, TRANSFER_QUEUE_SIZE);
	initialize_int_queue(&result, TOTAL_INTS_SHUFFLED);
	initialize_int_queue(&missed, TOTAL_INTS_SHUFFLED);

	tiber_mutex_init(&lock);
	tiber_cond_init(&full_wait);
	tiber_cond_init(&empty_wait);

	tiber_runtime* tr = new_tiber_runtime(RUNTIME_THREADS_COUNT, STACK_SIZE);

	tiber* ptb[PRODUCER_TASKS] = {};
	tiber* ctb[CONSUMER_TASKS] = {};

	for(unsigned long long int i = 0; i < PRODUCER_TASKS; i++)
		ptb[i] = new_tiber(tr, producer_func, (void*)((uintptr_t)i), 64*1024, 0);

	for(unsigned long long int i = 0; i < CONSUMER_TASKS; i++)
		ctb[i] = new_tiber(tr, consumer_func, NULL, 64*1024, 0);

	void* result_temp = NULL;

	for(unsigned long long int i = 0; i < PRODUCER_TASKS; i++)
		tiber_join(ptb[i], &result_temp);

	for(unsigned long long int i = 0; i < CONSUMER_TASKS; i++)
		tiber_join(ctb[i], &result_temp);

	delete_tiber_runtime(tr);

	tiber_mutex_destroy(&lock);
	tiber_cond_destroy(&full_wait);
	tiber_cond_destroy(&empty_wait);

	printf("result count = %"PRIu_cy_uint"\n", get_element_count_int_queue(&result));
	printf("missed count = %"PRIu_cy_uint"\n", get_element_count_int_queue(&missed));

	while(!is_empty_int_queue(&missed))
	{
		push_back_to_int_queue(&result, get_front_of_int_queue(&missed));
		pop_front_from_int_queue(&missed);
	}

	printf("\n\n");
	printf("result count = %"PRIu_cy_uint"\n", get_element_count_int_queue(&result));
	printf("missed count = %"PRIu_cy_uint"\n", get_element_count_int_queue(&missed));

	heap_sort_int_queue(&result, 0, TOTAL_INTS_SHUFFLED - 1, &simple_comparator(compare_ints));

	for(int i = 0; i < TOTAL_INTS_SHUFFLED; i++)
	{
		if(i == (*get_front_of_int_queue(&result)))
			pop_front_from_int_queue(&result);
		else
			printf("missing %d\n", i);
	}

	printf("TEST COMPLETE\n");

	return 0;
}