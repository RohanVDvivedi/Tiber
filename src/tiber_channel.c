#include<tiber/tiber_channel.h>

int initialize_tiber_channel(tiber_channel* tch, cy_uint max_capacity)
{
	if(max_capacity == 0)
		return 0;

	if(!initialize_dpipe(&(tch->channel), 0))
		return 0;
	tch->max_capacity = max_capacity;
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
	tiber_mutex_destroy(&(tch->lock));
	tiber_cond_destroy(&(tch->full_wait));
	tiber_cond_destroy(&(tch->empty_wait));
}

cy_uint write_to_tiber_channel(tiber_channel* tch, const void* data, cy_uint data_size, dpipe_operation_type op_type, uint64_t timeout_in_microseconds);

cy_uint read_from_tiber_channel(tiber_channel* tch, void* data, cy_uint data_size, dpipe_operation_type op_type, uint64_t timeout_in_microseconds);

cy_uint get_bytes_tiber_channel(const tiber_channel* tch);

void close_tiber_channel(tiber_channel* tch);

int is_closed_tiber_channel(const tiber_channel* tch);