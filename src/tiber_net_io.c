#include<tiber/tiber_net_io.h>

#include<stdlib.h>
#include<unistd.h>

tiber_net_io tg_net = {};

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

void initialize_tiber_net_io();

void deinitialize_tiber_net_io();

int register_fd_with_tiber_net_io(int fd);

int tiber_accept(int socket, struct sockaddr* addr, socklen_t* addr_len);

int tiber_connect(int sockfd, const struct sockaddr* addr, socklen_t addr_len);

ssize_t tiber_read(int fd, void* buf, size_t count);

ssize_t tiber_write(int fd, const void* buf, size_t count);

int tiber_close(int fd);