#ifndef TIBER_NET_IO_H
#define TIBER_NET_IO_H

#include<tiber/tiber.h>

#include<cutlery/hashmap.p>

typedef struct tiber_net_io tiber_net_io;
struct tiber_net_io
{
	// epoll file descriptor
	int epoll_fd;

	// protects tiber_net_io_fd and their reference_count
	pthread_spinlock_t lock;

	// fd -> tiber_net_io_fd
	hashmap tiber_net_io_fds;
};

void initialize_tiber_net_io();

void deinitialize_tiber_net_io();

int register_fd_with_tiber_net_io(int fd);

int tiber_accept(int socket, struct sockaddr* addr, socklen_t* addr_len);

int tiber_connect(int sockfd, const struct sockaddr* addr, socklen_t addr_len);

ssize_t tiber_read(int fd, void* buf, size_t count);

ssize_t tiber_write(int fd, const void* buf, size_t count);

int tiber_close(int fd);

#endif