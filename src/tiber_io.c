#include<tiber/tiber_io.h>

#include<stdlib.h>
#include<unistd.h>

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
};

static cy_uint hash_tiber_io_wt(const void* wt)
{
	return hash_randomizer(((const tiber_io_wt*)wt)->fd);
}

static cy_uint compare_tiber_io_wt(const void* wt1, const void* wt2)
{
	return compare_numbers(((const tiber_io_wt*)wt1)->fd, ((const tiber_io_wt*)wt2)->fd);
}

void initialize_tiber_io();

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