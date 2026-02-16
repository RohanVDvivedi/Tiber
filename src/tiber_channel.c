#include<tiber/tiber_channel.h>

int initialize_tiber_channel(tiber_channel* tch, cy_uint max_capacity);

void deinitialize_tiber_channel(tiber_channel* tch);

cy_uint write_to_tiber_channel(tiber_channel* tch, const void* data, cy_uint data_size, dpipe_operation_type op_type, uint64_t timeout_in_microseconds);

cy_uint read_from_tiber_channel(tiber_channel* tch, void* data, cy_uint data_size, dpipe_operation_type op_type, uint64_t timeout_in_microseconds);

cy_uint get_bytes_tiber_channel(const tiber_channel* tch);

void close_tiber_channel(tiber_channel* tch);

int is_closed_tiber_channel(const tiber_channel* tch);