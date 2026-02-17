#include<tiber/tiber.h>
#include<tiber/tiber_io.h>

#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>

#include<stdio.h>
#include<string.h>
#include<unistd.h>
#include<errno.h>

ssize_t tiber_read_full(int fd, void* buf, size_t count)
{
	ssize_t total_bytes_read = 0;
	while(total_bytes_read < count)
	{
		ssize_t bytes_read = tiber_read(fd, buf + total_bytes_read, count - total_bytes_read);
		if(bytes_read == -1)
			return -1;
		if(bytes_read == 0)
			return total_bytes_read;
		total_bytes_read += bytes_read;
	}
	return total_bytes_read;
}

ssize_t tiber_write_full(int fd, const void* buf, size_t count)
{
	ssize_t total_bytes_written = 0;
	while(total_bytes_written < count)
	{
		ssize_t bytes_written = tiber_write(fd, buf + total_bytes_written, count - total_bytes_written);
		if(bytes_written == -1)
			return -1;
		total_bytes_written += bytes_written;
	}
	return total_bytes_written;
}

void* make_1000_requests(void* _t)
{

	// then we try to set up socket and retrieve the file discriptor that is returned
	int err = socket(AF_INET, SOCK_STREAM, 0);
    if(err == -1)
    {
		printf("error creating socket\n");
    	exit(-1);
    }
    int fd = err;

    register_fd_with_tiber_io(fd);

	// next we try and attempt to connect the socket formed whose file discriptor we have
	// to connect using the address that we have in sockaddr_in struct in server_addr
	// connect call is noop for a udp socket
	struct sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_port = htons(9099);
	inet_pton(AF_INET, "127.0.0.1", &(addr.sin_addr));
	err = tiber_connect(fd, ((const struct sockaddr *)(&addr)), sizeof(addr));
	if(err == -1)
	{
		printf("error connecting\n");
    	exit(-1);
	}

	char wbuffer[1000];
	char rbuffer[1000];

	for(int i = 0; i < 1000; i++)
	{
		for(int i = 0; i < 30; i++)
		{
			wbuffer[i] = 'a' + (rand() % 26);
		}
		wbuffer[30] = '\0';

		int buffsentlength = tiber_write_full(fd, wbuffer, strlen(wbuffer));
		if(buffsentlength == -1 || buffsentlength == 0)
		{
			printf("premature server connection closed for write, %d, @ %d\n", buffsentlength, i);
			break;
		}
		if(buffsentlength != 30)
		{
			printf("sent %d bytes read instead of 30\n", buffsentlength);
			exit(-1);
		}
		wbuffer[buffsentlength] = '\0';

		int buffreadlength = tiber_read_full(fd, rbuffer, 30);
		if(buffreadlength == -1 || buffreadlength == 0)
		{
			printf("premature server connection closed for read, %d @ %d\n", buffreadlength, i);
			break;
		}
		rbuffer[buffreadlength] = '\0';
		if(buffreadlength != 30)
		{
			printf("read %d bytes read instead of 30\n", buffreadlength);
			exit(-1);
		}

		if(0 != strcmp(rbuffer, wbuffer))
		{
			printf("response of %s was %s\n", wbuffer, rbuffer);
			exit(-1);
		}
	}

	tiber_close(fd);

	return NULL;
}

int tiber_main()
{
	tiber* tb[9500];

	for(int i = 0; i < sizeof(tb)/sizeof(tb[0]); i++)
		tb[i] = new_tiber(NULL, make_1000_requests, NULL, 512*1024, 0, NULL, NULL);

	void* result = NULL;
	for(int i = 0; i < sizeof(tb)/sizeof(tb[0]); i++)
		tiber_join(tb[i], &result);

	return 0;
}