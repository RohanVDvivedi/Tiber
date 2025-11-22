#include<tiber/tiber.h>

#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>

#define RUNTIME_THREADS_COUNT 	2
#define STACK_SIZE              24*1024

void* tb1_func(void* p)
{
	printf("Hello 1 from %p\n", tiber_self());

	tiber_yield();

	printf("Rohan 1 from %p\n", tiber_self());

	tiber_yield();

	printf("Dvivedi 1 from %p\n", tiber_self());

	return NULL;
}

void* tb2_func(void* p)
{
	printf("Hello 2 from %p\n", tiber_self());

	tiber_yield();

	printf("Rohan 2 from %p\n", tiber_self());

	tiber_yield();

	printf("Dvivedi 2 from %p\n", tiber_self());

	return NULL;
}

int MAX_COUNT = 10;
int curr = 0;

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

		printf("Printing %d from %p\n", curr, tiber_self());
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

		printf("Printing %d from %p\n", curr, tiber_self());
		curr++;

		WAKEUP(&wait);

		tiber_mutex_unlock(&lock);
	}

	return NULL;
}

uint64_t millis_since_start()
{
    static struct timespec start = {0};

    if (start.tv_sec == 0 && start.tv_nsec == 0)
        timespec_get(&start, CLOCK_MONOTONIC);

    struct timespec now;
    timespec_get(&now, CLOCK_MONOTONIC);

    return (now.tv_sec - start.tv_sec) * 1000LL +
           (now.tv_nsec - start.tv_nsec) / 1000000LL;
}

void* tb5_func(void* p)
{
	for(int i = 0; i < 4; i++)
	{
		printf("Hola 5 @ %lu from %p\n", millis_since_start(), tiber_self());
		tiber_sleep((i % 2) + 1);
	}
	printf("Hola 5 @ %lu from %p\n", millis_since_start(), tiber_self());

	return NULL;
}

void* tb6_func(void* p)
{
	for(int i = 0; i < 10; i++)
	{
		printf("Hola 6 @ %lu from %p\n", millis_since_start(), tiber_self());
		tiber_msleep(((i % 2) + 1) * 3 * 100);
	}
	printf("Hola 6 @ %lu from %p\n", millis_since_start(), tiber_self());

	return NULL;
}

int main()
{
	millis_since_start();

	tiber_mutex_init(&lock);
	tiber_cond_init(&wait);

	tiber_runtime* tr = new_tiber_runtime(RUNTIME_THREADS_COUNT, STACK_SIZE);

	tiber* tb1 = new_tiber(tr, tb1_func, NULL, 4096);
	tiber* tb2 = new_tiber(tr, tb2_func, NULL, 4096);
	tiber* tb3 = new_tiber(tr, tb3_func, NULL, 4096);
	tiber* tb4 = new_tiber(tr, tb4_func, NULL, 4096);
	tiber* tb5 = new_tiber(tr, tb5_func, NULL, 4096);
	tiber* tb6 = new_tiber(tr, tb6_func, NULL, 4096);

	// wait for 5 seconds
	sleep(10);

	tiber_mutex_destroy(&lock);
	tiber_cond_destroy(&wait);

	return 0;
}