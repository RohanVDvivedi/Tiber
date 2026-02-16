#ifndef TIBER_CHANNEL_H
#define TIBER_CHANNEL_H

#include<cutlery/dpipe.h>

#include<tiber/tiber.h>

/*
	tiber_channel is an abstraction of a concurrent byte buffer queue that can be used for communication agnostically,
	without knowing whether the communicating entities are tibers or pthreads or both
*/

#define UNBOUNDED_TIBER_CHANNEL_CAPACITY CY_UINT_MAX

typedef struct tiber_channel tiber_channel;
struct tiber_channel
{
	dpipe channel;

	// if equals UNBOUNDED_TIBER_CHANNEL_CAPACITY
	// the it is unbounded
	cy_uint max_capacity;

	tiber_mutex lock;

	tiber_cond writers_wait;

	tiber_cond readers_wait;
};

/*
	initialize and deinitialize functions are not concurrency safe (obviously)
*/

int initialize_tiber_channel(tiber_channel* tch, cy_uint max_capacity);

void deinitialize_tiber_channel(tiber_channel* tch);

/*
	the remianing 4 functions are safe to be called from any of pthread or tiber
*/

// the writes may fail due to timeout OR the channel being closed
// so if the write fails do check if the channel is closed
cy_uint write_to_tiber_channel(tiber_channel* tch, const void* data, cy_uint data_size, dpipe_operation_type op_type, uint64_t timeout_in_microseconds);

// the reads may fail due to timeout
cy_uint read_from_tiber_channel(tiber_channel* tch, void* data, cy_uint data_size, dpipe_operation_type op_type, uint64_t timeout_in_microseconds);

// number of bytes in the channel
cy_uint get_bytes_readable_tiber_channel(tiber_channel* tch);

// close and is_closed can be called by anyone, reader or writer
void close_tiber_channel(tiber_channel* tch);
int is_closed_tiber_channel(tiber_channel* tch);

#endif