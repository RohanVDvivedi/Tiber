#include<tiber/tiber_net_io.h>

tiber_net_io tg_net;

typedef struct tiber_net_io_fd tiber_net_io_fd;
struct tiber_net_io_fd
{
	int fd;

	uint64_t reference_count;

	tiber_mutex lock;

	tiber_cond read_wait;
	tiber_cond write_wait;
	tiber_cond read_and_write_wait;
};