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
	if(timespec_is_zero(start))
		start = tiber_now();

	return timespec_to_milliseconds(timespec_sub(tiber_now(), start));
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

tiber_mutex lock1;
tiber_cond wait1;

void* tb7_func(void* p)
{
	tiber_mutex_lock(&lock1);
	printf("Locked for task 7 @ %lu\n", millis_since_start());

	tiber_msleep(3000);

	tiber_mutex_unlock(&lock1);
	printf("Unlocked for task 7 @ %lu\n", millis_since_start());

	tiber_msleep(10);

	tiber_msleep(3000);

	tiber_mutex_lock(&lock1);
	WAKEUP(&wait1);
	tiber_mutex_unlock(&lock1);
	printf("WAKEUP from task 7 @ %lu\n", millis_since_start());

	return NULL;
}

void* tb8_func(void* p)
{
	tiber_msleep(10);

	{
		struct timespec wait_until = timespec_add(tiber_now(), timespec_from_milliseconds(2000));

		int result = tiber_mutex_timedlock(&lock1, &wait_until);
		if(result == ETIMEDOUT)
			printf("Lock timedout for task 8 @ %lu\n", millis_since_start());
		else
		{
			printf("ERROR, this must timeout for task 8, instead got %d @ %lu\n", result, millis_since_start());
			exit(-1);
		}
	}

	{
		struct timespec wait_until = timespec_add(tiber_now(), timespec_from_milliseconds(2000));

		int result = tiber_mutex_timedlock(&lock1, &wait_until);
		if(result == ETIMEDOUT)
		{
			printf("ERROR, this must not timeout for task 8 @ %lu\n", millis_since_start());
			exit(-1);
		}
		else
			printf("Locked for task 8 @ %lu\n", millis_since_start());
	}

	{
		struct timespec wait_until = timespec_add(tiber_now(), timespec_from_milliseconds(2000));

		int result = tiber_cond_timedwait(&wait1, &lock1, &wait_until);
		printf("Woken up with ((result == ETIMEDOUT) -> %d, %d) for task 8 @ %lu\n", (result == ETIMEDOUT), result, millis_since_start());
	}

	{
		struct timespec wait_until = timespec_add(tiber_now(), timespec_from_milliseconds(2000));

		int result = tiber_cond_timedwait(&wait1, &lock1, &wait_until);
		printf("Woken up with ((result == ETIMEDOUT) -> %d, %d) for task 8 @ %lu\n", (result == ETIMEDOUT), result, millis_since_start());
	}

	tiber_mutex_unlock(&lock1);
	printf("Unlocked for task 8 @ %lu\n", millis_since_start());

	return NULL;
}

int main()
{
	millis_since_start();

	tiber_mutex_init(&lock);
	tiber_cond_init(&wait);

	tiber_mutex_init(&lock1);
	tiber_cond_init(&wait1);

	tiber_runtime* tr = new_tiber_runtime(RUNTIME_THREADS_COUNT, STACK_SIZE);

	tiber* tb1 = new_tiber(tr, tb1_func, NULL, 4096, 0);
	tiber* tb2 = new_tiber(tr, tb2_func, NULL, 4096, 0);
	tiber* tb3 = new_tiber(tr, tb3_func, NULL, 4096, 0);
	tiber* tb4 = new_tiber(tr, tb4_func, NULL, 4096, 0);
	tiber* tb5 = new_tiber(tr, tb5_func, NULL, 4096, 0);
	tiber* tb6 = new_tiber(tr, tb6_func, NULL, 4096, 0);
	tiber* tb7 = new_tiber(tr, tb7_func, NULL, 4096, 0);
	tiber* tb8 = new_tiber(tr, tb8_func, NULL, 4096, 0);

	// wait for 5 seconds
	sleep(10);

	tiber_mutex_destroy(&lock);
	tiber_cond_destroy(&wait);

	tiber_mutex_destroy(&lock1);
	tiber_cond_destroy(&wait1);

	return 0;
}