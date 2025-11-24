#ifndef TIBER_INTERNALS_H
#define TIBER_INTERNALS_H

#include<tiber/tiber.h>

// the current tiber that this thread is executing get's stored here
extern __thread tiber* curr_tiber;

// the context that this tiber must return to is stored here for each of the threads, it is the context of the thread that this tiber must return to after execution
extern __thread ucontext_t thread_context;

// return back to the caller thread, to either wait, die or queue
void switch_from_this_tiber_to_caller_thread();

// queues the tiber to it's runtime, please set the state to QUEUED for this
void queue_tiber_to_runime(tiber* tb);

// returns true if the tiber was woken up
int wake_up_waiting_tiber(tiber* tb);

// always increment or decrement the reference count of the tiber while you have a pointer to it 
// (not needed for tiber to just be existing in the tiber_cond's or tiber_mutex's list)
// but you must increment the reference counter as soon as you copy it's pointer out and plan to do any thing with it like waking it up
void increment_tiber_reference_count(tiber* tb);
void decrement_tiber_reference_count(tiber* tb);

// only delete the tiber, or destroy only it's things when this reference counter reached 0
uint64_t fetch_tiber_reference_count(tiber* tb);

#endif