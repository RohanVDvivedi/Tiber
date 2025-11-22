#include<tiber/tiber.h>

#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>

#define RUNTIME_THREADS_COUNT 	2
#define STACK_SIZE              24*1024

tiber tb1;
tiber tb2;

void* tb1_func(void* p)
{
	printf("Hello 1\n");

	tiber_yield();

	printf("Rohan 1\n");

	tiber_yield();

	printf("Dvivedi 1\n");

	tiber_yield();

	return NULL;
}

void* tb2_func(void* p)
{
	printf("Hello 2\n");

	tiber_yield();

	printf("Rohan 2\n");

	tiber_yield();

	printf("Dvivedi 2\n");

	return NULL;
}

int MAX_COUNT = 10;
int curr = 0;

tiber tb3;
tiber tb4;

tiber_mutex lock;
tiber_cond wait;

#define WAKEUP tiber_cond_signal

#ifndef WAKEUP
	#define WAKEUP tiber_cond_broadcast
#endif

void* tb3_func(void* p)
{
	while(1)
	{
		tiber_mutex_lock(&lock);

		while(((curr % 2) != 1) && curr < MAX_COUNT)
			tiber_cond_wait(&wait, &lock);

		if(curr >= MAX_COUNT)
		{
			WAKEUP(&wait);
			tiber_mutex_unlock(&lock);
			break;
		}

		printf("Printing %d from %p\n", curr, tb3_func);
		curr++;

		WAKEUP(&wait);

		tiber_mutex_unlock(&lock);
	}

	return NULL;
}

void* tb4_func(void* p)
{
	while(1)
	{
		tiber_mutex_lock(&lock);

		while(((curr % 2) != 0) && curr < MAX_COUNT)
			tiber_cond_wait(&wait, &lock);

		if(curr >= MAX_COUNT)
		{
			WAKEUP(&wait);
			tiber_mutex_unlock(&lock);
			break;
		}

		printf("Printing %d from %p\n", curr, tb4_func);
		curr++;

		WAKEUP(&wait);

		tiber_mutex_unlock(&lock);
	}

	return NULL;
}

int main()
{
	tiber_mutex_init(&lock);
	tiber_cond_init(&wait);

	tiber_runtime* tr = new_tiber_runtime(RUNTIME_THREADS_COUNT, STACK_SIZE);

	tiber* tb1 = new_tiber(tr, tb1_func, NULL, 4096);
	tiber* tb2 = new_tiber(tr, tb2_func, NULL, 4096);
	tiber* tb3 = new_tiber(tr, tb3_func, NULL, 4096);
	tiber* tb4 = new_tiber(tr, tb4_func, NULL, 4096);

	// wait for 5 seconds
	sleep(5);

	tiber_mutex_destroy(&lock);
	tiber_cond_destroy(&wait);

	return 0;
}