#include<tiber/tiber_io.h>

#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>

#include<sys/epoll.h>

#include<cutlery/bst.h>

tiber_io global_tiber_io = {};

typedef struct tiber_io_wt tiber_io_wt;
struct tiber_io_wt
{
	int fd;

	uint64_t reference_count;

	tiber_mutex lock;

	tiber_cond read_wait;
	tiber_cond write_wait;
	tiber_cond read_and_write_wait;

	bstnode embed_node_for_tiber_io_wts;
};

static cy_uint hash_tiber_io_wt(const void* wt)
{
	return hash_randomizer(((const tiber_io_wt*)wt)->fd);
}

static int compare_tiber_io_wt(const void* wt1, const void* wt2)
{
	return compare_numbers(((const tiber_io_wt*)wt1)->fd, ((const tiber_io_wt*)wt2)->fd);
}

static void* tiber_io_epoll_loop(void* _t)
{
	return NULL;
}

void initialize_tiber_io()
{
	pthread_spin_init(&(global_tiber_io.lock), PTHREAD_PROCESS_PRIVATE);

	if(!initialize_hashmap(&(global_tiber_io.tiber_io_wts), ELEMENTS_AS_RED_BLACK_BST, 256, &simple_hasher(hash_tiber_io_wt), &simple_comparator(compare_tiber_io_wt), offsetof(tiber_io_wt, embed_node_for_tiber_io_wts)))
	{
		printf("TIBER BUG: tiber io creation failed, could not initialize tiber_io_wts\n");
		exit(-1);
	}

	// single threaded io runtime
	global_tiber_io.io_runtime = new_tiber_runtime(1, 3 * 1024 * 1024);

	global_tiber_io.epoll_fd = epoll_create1(0);
	if(global_tiber_io.epoll_fd == -1)
	{
		printf("TIBER BUG: tiber io creation failed, could not get an epoll file descriptor\n");
		exit(-1);
	}

	// tiber that runs the epoll_loop
	global_tiber_io.io_loop = new_tiber(global_tiber_io.io_runtime, tiber_io_epoll_loop, NULL, 1024 * 1024, 0);
}

void deinitialize_tiber_io();

int register_fd_with_tiber_io(int fd);

int tiber_accept(int sockfd, struct sockaddr* addr, socklen_t* addr_len)
{
	while(1)
	{
		int res = accept(sockfd, addr, addr_len);
		if(res != -1)
			return res;
	}
}

int tiber_connect(int sockfd, const struct sockaddr* addr, socklen_t addr_len)
{
	while(1)
	{
		int res = connect(sockfd, addr, addr_len);
		if(res != -1)
			return res;
	}
}

ssize_t tiber_read(int fd, void* buf, size_t count)
{
	while(1)
	{
		ssize_t res = read(fd, buf, count);
		if(res != -1)
			return res;
	}
}

ssize_t tiber_write(int fd, const void* buf, size_t count)
{
	while(1)
	{
		ssize_t res = write(fd, buf, count);
		if(res != -1)
			return res;
	}
}

int tiber_close(int fd);