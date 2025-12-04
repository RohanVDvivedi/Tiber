#include<tiber/tiber_io.h>

#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<errno.h>
#include<fcntl.h>

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

static tiber_io_wt* create_wt(int fd)
{
	tiber_io_wt* wt = NULL;

	pthread_spin_lock(&(global_tiber_io.lock));

		if(NULL == find_equals_in_hashmap(&(global_tiber_io.tiber_io_wts), &((const tiber_io_wt){.fd = fd})))
		{
			wt = malloc(sizeof(tiber_io_wt));
			wt->fd = fd;
			wt->reference_count = 1; // one if it's reference is in the epoll events
			tiber_mutex_init(&(wt->lock));
			tiber_cond_init(&(wt->read_wait));
			tiber_cond_init(&(wt->write_wait));
			tiber_cond_init(&(wt->read_and_write_wait));
			initialize_bstnode(&(wt->embed_node_for_tiber_io_wts));
			insert_in_hashmap(&(global_tiber_io.tiber_io_wts), wt);
		}

	pthread_spin_unlock(&(global_tiber_io.lock));

	return wt;
}

// increments reference count and returns the pointer to a tiber_io_wt
static tiber_io_wt* fetch_reference_wt(int fd)
{
	pthread_spin_lock(&(global_tiber_io.lock));

		tiber_io_wt* wt = (tiber_io_wt*) find_equals_in_hashmap(&(global_tiber_io.tiber_io_wts), &((const tiber_io_wt){.fd = fd}));
		if(wt != NULL)
			wt->reference_count++;

	pthread_spin_unlock(&(global_tiber_io.lock));

	return wt;
}

// decrements reference count and discards it if the reference count reaches 0
static void discard_reference_wt(tiber_io_wt* wt)
{
	pthread_spin_lock(&(global_tiber_io.lock));

		wt->reference_count--;
		if(wt->reference_count == 0)
		{
			remove_from_hashmap(&(global_tiber_io.tiber_io_wts), wt);
			tiber_mutex_destroy(&(wt->lock));
			tiber_cond_destroy(&(wt->read_wait));
			tiber_cond_destroy(&(wt->write_wait));
			tiber_cond_destroy(&(wt->read_and_write_wait));
			free(wt);
		}

	pthread_spin_unlock(&(global_tiber_io.lock));
}

static void discard_reference_wt2(int fd)
{
	pthread_spin_lock(&(global_tiber_io.lock));

		tiber_io_wt* wt = (tiber_io_wt*) find_equals_in_hashmap(&(global_tiber_io.tiber_io_wts), &((const tiber_io_wt){.fd = fd}));
		wt->reference_count--;
		if(wt->reference_count == 0)
		{
			remove_from_hashmap(&(global_tiber_io.tiber_io_wts), wt);
			tiber_mutex_destroy(&(wt->lock));
			tiber_cond_destroy(&(wt->read_wait));
			tiber_cond_destroy(&(wt->write_wait));
			tiber_cond_destroy(&(wt->read_and_write_wait));
			free(wt);
		}

	pthread_spin_unlock(&(global_tiber_io.lock));
}

static void* tiber_io_epoll_loop(void* _t)
{
	while(1)
	{
		#define MAX_EVENTS 1024
		struct epoll_event events[MAX_EVENTS];

		int events_count = epoll_wait(global_tiber_io.epoll_fd, events, MAX_EVENTS, 1000); // timeout is 1 second
		if(events_count == -1)
			break;

		for(int i = 0; i < events_count; i++)
		{
			tiber_io_wt* wt = events[i].data.ptr;

			tiber_mutex_lock(&(wt->lock));

			if(events[i].events & (EPOLLIN | EPOLLERR | EPOLLHUP))
				tiber_cond_broadcast(&(wt->read_wait));

			if(events[i].events & (EPOLLOUT | EPOLLERR | EPOLLHUP))
				tiber_cond_broadcast(&(wt->write_wait));

			if(events[i].events & (EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP))
				tiber_cond_broadcast(&(wt->read_and_write_wait));

			tiber_mutex_unlock(&(wt->lock));
		}
	}

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

void deinitialize_tiber_io()
{
	// close epoll_fd
	close(global_tiber_io.epoll_fd);

	// wait for tiber to finish
	void* result;
	tiber_join(global_tiber_io.io_loop, &result);

	// delete the tiber runtime
	delete_tiber_runtime(global_tiber_io.io_runtime);

	// destroy all elements of the hashmap tiber_io_wts
	// TODO:

	pthread_spin_destroy(&(global_tiber_io.lock));
}

int register_fd_with_tiber_io(int fd)
{
	// make the file descriptor non-blocking
	{
		int flags = fcntl(fd, F_GETFL, 0);
		if(flags == -1)
		{
			printf("TIBER BUG: not able to get flags for the file descriptor\n");
			exit(-1);
		}

		flags |= O_NONBLOCK;

		if(fcntl(fd, F_SETFL, flags) == -1)
		{
			printf("TIBER BUG: not able to make the file descriptor non-blocking\n");
			exit(-1);
		}
	}

	// try to create a new registeration of the wt for this fd
	tiber_io_wt* wt = create_wt(fd);

	// if successfull send it to epoll
	if(wt != NULL)
	{
		struct epoll_event event;
		event.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLET;
		event.data.ptr = wt;
		epoll_ctl(global_tiber_io.epoll_fd, EPOLL_CTL_ADD, fd, &event);;
	}

	return (wt != NULL);
}

int tiber_accept(int sockfd, struct sockaddr* addr, socklen_t* addr_len)
{
	while(1)
	{
		int res = accept(sockfd, addr, addr_len);
		if(res != -1) // if success, return right away
			return res;

		// we can only handle these errors non blockingly
		if(errno != EAGAIN && errno != EWOULDBLOCK)
			return res;

		tiber_io_wt* wt = fetch_reference_wt(sockfd);
		if(wt == NULL) // if a wait handle could not found, return right away
			return -1;

		tiber_mutex_lock(&(wt->lock));

		res = accept(sockfd, addr, addr_len);
		if(res != -1) // if success, return right away
		{
			tiber_mutex_unlock(&(wt->lock));
			discard_reference_wt(wt);
			return res;
		}

		// we can only handle these errors non blockingly
		if(errno != EAGAIN && errno != EWOULDBLOCK)
		{
			tiber_mutex_unlock(&(wt->lock));
			discard_reference_wt(wt);
			return res;
		}

		tiber_cond_wait(&(wt->read_and_write_wait), &(wt->lock));

		tiber_mutex_unlock(&(wt->lock));
		discard_reference_wt(wt);
	}
}

int tiber_connect(int sockfd, const struct sockaddr* addr, socklen_t addr_len)
{
	while(1)
	{
		int res = connect(sockfd, addr, addr_len);
		if(res != -1) // if success, return right away
			return res;

		// we can only handle these errors non blockingly
		if(errno != EINPROGRESS)
			return res;

		tiber_io_wt* wt = fetch_reference_wt(sockfd);
		if(wt == NULL) // if a wait handle could not found, return right away
			return -1;

		tiber_mutex_lock(&(wt->lock));

		res = connect(sockfd, addr, addr_len);
		if(res != -1) // if success, return right away
		{
			tiber_mutex_unlock(&(wt->lock));
			discard_reference_wt(wt);
			return res;
		}

		// we can only handle these errors non blockingly
		if(errno != EINPROGRESS)
		{
			tiber_mutex_unlock(&(wt->lock));
			discard_reference_wt(wt);
			return res;
		}

		tiber_cond_wait(&(wt->read_and_write_wait), &(wt->lock));

		tiber_mutex_unlock(&(wt->lock));
		discard_reference_wt(wt);
	}
}

ssize_t tiber_read(int fd, void* buf, size_t count)
{
	while(1)
	{
		ssize_t res = read(fd, buf, count);
		if(res != -1) // if success, return right away
			return res;

		// we can only handle these errors non blockingly
		if(errno != EAGAIN && errno != EWOULDBLOCK)
			return res;

		tiber_io_wt* wt = fetch_reference_wt(fd);
		if(wt == NULL) // if a wait handle could not found, return right away
			return -1;

		tiber_mutex_lock(&(wt->lock));

		res = read(fd, buf, count);
		if(res != -1) // if success, return right away
		{
			tiber_mutex_unlock(&(wt->lock));
			discard_reference_wt(wt);
			return res;
		}

		// we can only handle these errors non blockingly
		if(errno != EAGAIN && errno != EWOULDBLOCK)
		{
			tiber_mutex_unlock(&(wt->lock));
			discard_reference_wt(wt);
			return res;
		}

		tiber_cond_wait(&(wt->read_wait), &(wt->lock));

		tiber_mutex_unlock(&(wt->lock));
		discard_reference_wt(wt);
	}
}

ssize_t tiber_write(int fd, const void* buf, size_t count)
{
	while(1)
	{
		ssize_t res = write(fd, buf, count);
		if(res != -1) // if success, return right away
			return res;

		// we can only handle these errors non blockingly
		if(errno != EAGAIN && errno != EWOULDBLOCK)
			return res;

		tiber_io_wt* wt = fetch_reference_wt(fd);
		if(wt == NULL) // if a wait handle could not found, return right away
			return -1;

		tiber_mutex_lock(&(wt->lock));

		res = write(fd, buf, count);
		if(res != -1) // if success, return right away
		{
			tiber_mutex_unlock(&(wt->lock));
			discard_reference_wt(wt);
			return res;
		}

		// we can only handle these errors non blockingly
		if(errno != EAGAIN && errno != EWOULDBLOCK)
		{
			tiber_mutex_unlock(&(wt->lock));
			discard_reference_wt(wt);
			return res;
		}

		tiber_cond_wait(&(wt->write_wait), &(wt->lock));

		tiber_mutex_unlock(&(wt->lock));
		discard_reference_wt(wt);
	}
}

int tiber_close(int fd)
{
	// stop all events comming from the epoll for this fd
	epoll_ctl(global_tiber_io.epoll_fd, EPOLL_CTL_DEL, fd, NULL);

	// then discard the reference count held by the internals if epoll
	discard_reference_wt2(fd);

	return close(fd);
}