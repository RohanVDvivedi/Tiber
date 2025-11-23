#ifndef TIBER_RESULT_H
#define TIBER_RESULT_H

#include<tiber/tiber.h>

void initialize_tiber_result(tiber_result* tres);

void set_tiber_result(tiber_result* tres, void* result);

void* get_tiber_result(tiber_result* tres);

void deinitialize_tiber_result(tiber_result* tres);

#endif