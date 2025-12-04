#include<tiber/tiber.h>
#include<tiber/tiber_io.h>

#include<sys/socket.h>
#include<netinet/in.h>

#include<stdio.h>
#include<string.h>
#include<unistd.h>
#include<errno.h>

int process(char* buffer)
{
	printf("received : %s length %lu\n", buffer, strlen(buffer));

	if(strcmp(buffer, "exit\r\n") == 0 || strcmp(buffer, "exit\n") == 0 || strcmp(buffer, "exit") == 0)
	{
		strcpy(buffer, "xit\r\n");
		return -1;
	}
	else if(strcmp(buffer, "ping\r\n") == 0 || strcmp(buffer, "ping\n") == 0 || strcmp(buffer, "ping") == 0)
	{
		strcpy(buffer, "pong\r\n");
	}
	else if(strcmp(buffer, "pong\r\n") == 0 || strcmp(buffer, "pong\n") == 0 || strcmp(buffer, "pong") == 0)
	{
		strcpy(buffer, "ping\r\n");
	}

	return 0;
}

void* serve(void* fd_v)
{
	int fd = (intptr_t)fd_v;

	printf("TCP Connection : %d\n", fd);
	char buffer[1000];

	int buffreadlength = -1;
	int buffsentlength = -1;

	while(1)
	{
		buffreadlength = tiber_read(fd, buffer, 999);
		if(buffreadlength == -1 || buffreadlength == 0)
		{
			printf("read -> %d\n", buffreadlength);
			break;
		}
		if(buffreadlength != 30)
		{
			printf("read %d bytes read instead of 30\n", buffreadlength);
			exit(-1);
		}

		buffer[buffreadlength] = '\0';

		// process the buffer here
		int process_result = process(buffer);

		buffreadlength = strlen(buffer);
		buffsentlength = tiber_write(fd, buffer, buffreadlength);
		if(buffsentlength == -1 || buffsentlength == 0)
		{
			printf("write -> %d\n", buffsentlength);
			break;
		}

		if(process_result != 0)
		{
			break;
		}
	}

	tiber_close(fd);
}

int tiber_main()
{
	// phase 1
	// file descriptor to socket
	int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
	if(listen_fd == -1)
	{
		printf("error creating socket\n");
		return -1;
	}

	// set socket options so that it allows you to reuse the address and port right after using it once
	setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));
	setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int));

	// phase 2
	// bind server address struct with the file descriptor
	struct sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_port = htons(9099);
	addr.sin_addr.s_addr = INADDR_ANY;
	int err = bind(listen_fd, ((const struct sockaddr *)(&addr)), sizeof(addr));
	if(err)
	{
		printf("error binding\n");
		close(listen_fd);
		return -1;
	}

	// phase 3
	// listenning on the socket file discriptor 
	err = listen(listen_fd, 10);
	if(err == -1)
	{
		printf("error listening\n");
		return err;
	}

	struct sockaddr_in client_addr;
	socklen_t client_len = sizeof(client_addr);

	register_fd_with_tiber_io(listen_fd);

	while(1)
	{
		// phase 4
		// accept uses backlog queue connection and de-queues them 
		err = tiber_accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
		if(err == -1)
		{
			printf("error accepting\n");
			// break the listenning loop, if the listen_fd file discriptor is closed
			if(errno == EBADF || errno == ECONNABORTED || errno == EINVAL || errno == ENOTSOCK || errno == EPERM)
				break;
		}
		int conn_fd = err;

		register_fd_with_tiber_io(conn_fd);

		new_tiber(NULL, serve, (void*)((intptr_t)conn_fd), 512*1024, 1);
	}

	tiber_close(listen_fd);

	return 0;
}