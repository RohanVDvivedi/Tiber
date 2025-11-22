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
	executor* tiber_executor;

	alarm_job* tiber_timer_job;

	pthread_spinlock_t tiber_timer_lock;

	pheap tiber_timer_queue;
};

tiber_runtime* new_tiber_runtime(uint64_t thread_count, uint64_t stack_size);

void delete_tiber_runtime(tiber_runtime* tr_p);

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
	// free this stack memory on TIBER_KILLED state
	void* stack;

	// tiber's contexts
	pthread_spinlock_t tiber_context_lock;
	ucontext_t tiber_context;

	// tiber's state
	pthread_spinlock_t tiber_state_lock;
	tiber_state state;
	void* return_value;

	// for tiber_cond
	tiber_cond* waiting_on_tiber_cond;
	llnode embed_node_for_tiber_cond_waiters;

	// for tiber_mutex
	tiber_mutex* waiting_on_tiber_mutex;
	llnode embed_node_for_tiber_mutex_waiters;

	// this will be set of the 
	struct timespec abstime_for_wakeup;
	phpnode embed_node_for_tiber_timer_queue;
	int is_timer_set;
};

// if tr_p is NULL, the tiber is created for the current runtime
tiber* new_tiber(tiber_runtime* tr_p, void* (*entry_func)(void* input_p), void* input_p, uint64_t stack_size);

// only the below 2 functions actually delete the tiber object
int tiber_join(tiber* tb, void** return_value);
int tiber_detach(tiber* tb);

tiber* tiber_self(void);

void tiber_exit(void);
void tiber_yield(void);

void tiber_sleep(const struct timespec *abs_time);


#endif