#ifndef TIBER_RESULT_H
#define TIBER_RESULT_H

#include<tiber/tiber.h>

void initialize_tiber_result(tiber_result* tres);

// must be called from the within of a tiber, while it is being executed, before you do swapcontext back to the caller thread
void set_tiber_result(tiber_result* tres, void* result);

// can be done from any thread or tiber
void* get_tiber_result(tiber_result* tres);

void deinitialize_tiber_result(tiber_result* tres);

#endif