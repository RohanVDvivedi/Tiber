#ifndef TIBER_IO_H
#define TIBER_IO_H

#include<tiber/tiber.h>

#include<cutlery/hashmap.h>

#include<sys/types.h>
#include<sys/socket.h>

typedef struct tiber_io tiber_io;
struct tiber_io
{
	// epoll file descriptor
	int epoll_fd;

	// protects tiber_net_io_fd and their reference_count counter and marked_for_deletion flags
	pthread_spinlock_t lock;

	// fd -> tiber_io_wt
	hashmap tiber_io_wts;

	// runtime for the epoll loop, it will only have 1 pthread, for running the epoll loop thats all
	tiber_runtime* io_runtime;

	// tiber that runs the epoll_loop
	tiber* io_loop;
};

// this should be the first function in your application, the first line in you main function
void initialize_tiber_io();

// this should be the last line in your main function, right before you return
void deinitialize_tiber_io();

int register_fd_with_tiber_io(int fd);

int tiber_accept(int sockfd, struct sockaddr* addr, socklen_t* addr_len);

int tiber_connect(int sockfd, const struct sockaddr* addr, socklen_t addr_len);

ssize_t tiber_read(int fd, void* buf, size_t count);

ssize_t tiber_write(int fd, const void* buf, size_t count);

int tiber_close(int fd);

#endif