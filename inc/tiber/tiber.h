#ifndef TIBER_H
#define TIBER_H

#include<cutlery/pheap.h>
#include<cutlery/linkedlist.h>
#include<boompar/executor.h>
#include<boompar/alarm_job.h>

#include<posixutils/timespec_utils.h>

#include<ucontext.h>

#include<pthread.h>

typedef struct tiber_runtime tiber_runtime;
struct tiber_runtime
{
	executor* tiber_executor;

	alarm_job* tiber_timer_job;

	pthread_spinlock_t tiber_timer_lock;

	pheap tiber_timer_queue;
};

typedef struct tiber_mutex tiber_mutex;
struct tiber_mutex
{
	pthread_spinlock_t lock;

	linkedlist waiting_tibers;

	int is_locked;
};

typedef struct tiber_cond tiber_cond;
struct tiber_cond
{
	pthread_spinlock_t lock;

	linkedlist waiting_tibers;
};

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

#endif