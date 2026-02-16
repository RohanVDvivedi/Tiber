#include<tiber/tiber_channel.h>

extern __thread tiber* curr_tiber;

int initialize_tiber_channel(tiber_channel* tch, cy_uint max_capacity)
{
	if(max_capacity == 0)
		return 0;

	if(!initialize_dpipe(&(tch->channel), 0))
		return 0;
	tch->max_capacity = max_capacity;
	tch->is_closed = 0;
	if(!tiber_mutex_init(&(tch->lock)))
		return 0;
	if(!tiber_cond_init(&(tch->full_wait)))
		return 0;
	if(!tiber_cond_init(&(tch->empty_wait)))
		return 0;

	return 1;
}

void deinitialize_tiber_channel(tiber_channel* tch)
{
	deinitialize_dpipe(&(tch->channel));
	tch->max_capacity = 0;
	tch->is_closed = 0;
	tiber_mutex_destroy(&(tch->lock));
	tiber_cond_destroy(&(tch->full_wait));
	tiber_cond_destroy(&(tch->empty_wait));
}

cy_uint write_to_tiber_channel(tiber_channel* tch, const void* data, cy_uint data_size, dpipe_operation_type op_type, uint64_t timeout_in_microseconds)
{
	if(data_size == 0)
		return 0;

	// make this call pthread safe
	if(curr_tiber == NULL)
		exit(-1);

	tiber_mutex_lock(&(tch->lock));

	cy_uint bytes_written = 0;

	while(1)
	{
		// check if closed, if yes break

		// check if enough bytes are writable, if yes write and break

		// check to expand, if yes, expand, write and break

		// wait for timeout
	}

	if(bytes_written > 0)
	{
		// wake up waiting sleeping readers
	}

	tiber_mutex_unlock(&(tch->lock));

	return bytes_written;
}

cy_uint read_from_tiber_channel(tiber_channel* tch, void* data, cy_uint data_size, dpipe_operation_type op_type, uint64_t timeout_in_microseconds)
{
	if(data_size == 0)
		return 0;

	// make this call pthread safe
	if(curr_tiber == NULL)
		exit(-1);

	tiber_mutex_lock(&(tch->lock));

	cy_uint bytes_read = 0;

	while(1)
	{
		// check if enough bytes are readable, if yes read and break

		// wait for timeout on 
	}

	if(bytes_read > 0)
	{
		// size too big, shrink

		// wake up waiting sleeping writers
	}

	tiber_mutex_unlock(&(tch->lock));

	return bytes_read;
}

cy_uint get_bytes_readable_tiber_channel(tiber_channel* tch)
{
	// make this call pthread safe
	if(curr_tiber == NULL)
		exit(-1);

	tiber_mutex_lock(&(tch->lock));

	cy_uint bytes_readable = get_bytes_readable_in_dpipe(&(tch->channel));

	tiber_mutex_unlock(&(tch->lock));

	return bytes_readable;
}

void close_tiber_channel(tiber_channel* tch)
{
	// make this call pthread safe
	if(curr_tiber == NULL)
		exit(-1);

	tiber_mutex_lock(&(tch->lock));

	tch->is_closed = 1;

	tiber_cond_broadcast(&(tch->full_wait));
	tiber_cond_broadcast(&(tch->empty_wait));

	tiber_mutex_unlock(&(tch->lock));
}

int is_closed_tiber_channel(tiber_channel* tch)
{
	// make this call pthread safe
	if(curr_tiber == NULL)
		exit(-1);

	tiber_mutex_lock(&(tch->lock));

	int is_closed = !!(tch->is_closed);

	tiber_mutex_unlock(&(tch->lock));

	return is_closed;
}
