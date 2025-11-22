#ifndef TIBER_H
#define TIBER_H

#include<cutlery/pheap.h>
#include<cutlery/linkedlist.h>
#include<boompar/executor.h>
#include<boompar/alarm_job.h>

#include<posixutils/timespec_utils.h>

#include<ucontext.h>

#include<pthread.h>

// tiber runtime

typedef struct tiber_runtime tiber_runtime;
struct tiber_runtime
{
	executor* thread_pool;

	alarm_job* timer_job;

	pthread_spinlock_t timer_lock;

	pheap timer_queue;
};

tiber_runtime* new_tiber_runtime(uint64_t thread_count, uint64_t stack_size);

void delete_tiber_runtime(tiber_runtime* tr);

// tiber mutex

typedef struct tiber_mutex tiber_mutex;
struct tiber_mutex
{
	pthread_spinlock_t lock;

	linkedlist waiting_tibers;

	int is_locked;
};

int tiber_mutex_init(tiber_mutex* tm);
int tiber_mutex_destroy(tiber_mutex* tm);
int tiber_mutex_lock(tiber_mutex* tm);
int tiber_mutex_trylock(tiber_mutex* tm);
int tiber_mutex_timedlock(tiber_mutex* tm, const struct timespec *abs_time);
int tiber_mutex_unlock(tiber_mutex* tm);

// tiber condtion variable

typedef struct tiber_cond tiber_cond;
struct tiber_cond
{
	pthread_spinlock_t lock;

	linkedlist waiting_tibers;
};

int tiber_cond_init(tiber_cond* tc);
int tiber_cond_destroy(tiber_cond* tc);
int tiber_cond_wait(tiber_cond* tc, tiber_mutex* tm);
int tiber_cond_timedwait(tiber_cond* tc, tiber_mutex* tm, const struct timespec *abs_time);
int tiber_cond_signal(tiber_cond* tc);
int tiber_cond_broadcast(tiber_cond* tc);

// tiber itself

typedef enum tiber_state tiber_state;
enum tiber_state
{
	TIBER_QUEUED,	// this is the initial state, with the tiber already pushed into the thread_pool
	TIBER_RUNNING,
	TIBER_WAITING,
	TIBER_KILLED,
};

typedef struct tiber tiber;
struct tiber
{
	// this is the runtime this tiber will be executed on, it is static it will no change
	tiber_runtime* runtime;

	// stack space for the tiber, it is static it will no change
	// free this stack memory on TIBER_KILLED state
	void* stack;

	// the input function of the tiber that actuall returns the value
	void* input_p;
	void* (*entry_func)(void* input_p);
	void* return_value;

	// tiber contexts, while the thread's context it works with is in its thread local (check the source tiber_runtime.c)
	pthread_spinlock_t context_lock;
	ucontext_t context;

	// tiber's state
	pthread_spinlock_t state_lock;
	tiber_state state;

	// for tiber_cond
	tiber_cond* waiting_on_tiber_cond;			// protected by the tiber_state_lock and tiber_cond->lock (having just 1 lock allows us to safely read it, though)
	llnode embed_node_for_tiber_cond_waiters;	// protected by the tiber_cond->lock

	// for tiber_mutex
	tiber_mutex* waiting_on_tiber_mutex;		// protected by the tiber_state_lock and tiber_mutex->lock (having just 1 lock allows us to safely read it, though)
	llnode embed_node_for_tiber_mutex_waiters;	// protected by the tiber_mutex->lock

	// this will be set for a timer based waiting tiber
	int is_timer_set;							// protected by the tiber_state_lock and tiber_runtime->timer_lock (having just 1 lock allows us to safely read it, though)
	struct timespec abstime_for_wakeup;			// protected by the tiber_state_lock and tiber_runtime->timer_lock (having just 1 lock allows us to safely read it, though)
	phpnode embed_node_for_tiber_timer_queue;	// protected by the tiber_runtime->timer_lock

	// this is the internal reference count for the internal usage of the tiber functions (other than the tiber_cond, tiber_mutex and timer_queue)
	// this allows us to not kill it before the tiber is released by all the internal functions
	pthread_spinlock_t reference_count_lock;
	uint64_t reference_count;					// protected by the reference_count_lock

	// you may free the tiber's struct with the state_lock held only if
	/*
		state == possibly KILLED
		&& waiting_on_tiber_cond == NULL
		&& waiting_on_tiber_mutex == NULL
		&& is_timer_set == 0
		&& reference_count == 0

		the attributes act as reference counting for the tiber struct
	*/
};

tiber* new_tiber(tiber_runtime* tr, void* (*entry_func)(void* input_p), void* input_p, uint64_t stack_size);

// only the below 2 functions actually delete the tiber object
int tiber_join(tiber* tb, void** return_value);
int tiber_detach(tiber* tb);

tiber* tiber_self(void);

void tiber_exit(void);
void tiber_yield(void);

void tiber_sleep(const struct timespec *abs_time);


#endif