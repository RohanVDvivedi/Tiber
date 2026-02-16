#include<tiber/tiber_channel.h>

#include<tiber/tiber_cond_utils.h>

extern __thread tiber* curr_tiber;

int initialize_tiber_channel(tiber_channel* tch, cy_uint max_capacity)
{
	if(max_capacity == 0)
		return 0;

	if(!initialize_dpipe(&(tch->channel), 0))
		return 0;
	tch->max_capacity = max_capacity;
	if(!tiber_mutex_init(&(tch->lock)))
		return 0;
	if(!tiber_cond_init(&(tch->writers_wait)))
		return 0;
	if(!tiber_cond_init(&(tch->readers_wait)))
		return 0;

	return 1;
}

void deinitialize_tiber_channel(tiber_channel* tch)
{
	deinitialize_dpipe(&(tch->channel));
	tch->max_capacity = 0;
	tiber_mutex_destroy(&(tch->lock));
	tiber_cond_destroy(&(tch->writers_wait));
	tiber_cond_destroy(&(tch->readers_wait));
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
		// if closed break
		if(is_dpipe_closed(&(tch->channel)))
			break;

		// check if anything is writable, if success, break
		if((bytes_written = write_to_dpipe(&(tch->channel), data, data_size, op_type)))
			break;

		// now we know it is capacity issue

		// check to expand, if yes, expand, write and break
		{
			cy_uint required_capacity = data_size + get_bytes_readable_in_dpipe(&(tch->channel));
			cy_uint new_capacity = max((required_capacity * 2), tch->max_capacity);
			if(resize_dpipe(&(tch->channel), new_capacity) && (bytes_written = write_to_dpipe(&(tch->channel), data, data_size, op_type)))
				break;
		}

		// wait for timeout, and break on wait_error
		if(tiber_cond_timedwait_for_microseconds(&(tch->writers_wait), &(tch->lock), &timeout_in_microseconds))
			break;
	}

	if(bytes_written > 0)
	{
		// wake up waiting sleeping readers
		tiber_cond_broadcast(&(tch->readers_wait));
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
		// check if anything is readable, if success, break
		if((bytes_read = read_from_dpipe(&(tch->channel), data, data_size, op_type)))
			break;

		// wait for timeout
		if(tiber_cond_timedwait_for_microseconds(&(tch->readers_wait), &(tch->lock), &timeout_in_microseconds))
			break;
	}

	if(bytes_read > 0)
	{
		// size too big (thrice the required capacity), shrink
		if(get_bytes_readable_in_dpipe(&(tch->channel)) < (get_capacity_dpipe(&(tch->channel)) / 3))
			resize_dpipe(&(tch->channel), get_bytes_readable_in_dpipe(&(tch->channel)));

		// wake up waiting sleeping writers
		tiber_cond_broadcast(&(tch->writers_wait));
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

	close_dpipe(&(tch->channel));

	// wake up everyone
	tiber_cond_broadcast(&(tch->writers_wait));
	tiber_cond_broadcast(&(tch->readers_wait));

	tiber_mutex_unlock(&(tch->lock));
}

int is_closed_tiber_channel(tiber_channel* tch)
{
	// make this call pthread safe
	if(curr_tiber == NULL)
		exit(-1);

	tiber_mutex_lock(&(tch->lock));

	int is_closed = is_dpipe_closed(&(tch->channel));

	tiber_mutex_unlock(&(tch->lock));

	return is_closed;
}
