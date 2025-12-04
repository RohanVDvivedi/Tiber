#include<tiber/tiber.h>
#include<tiber/tiber_io.h>

#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>

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

		int buffsentlength = tiber_write(fd, wbuffer, strlen(wbuffer));
		if(buffsentlength == -1 || buffsentlength == 0)
		{
			printf("premature server connection closed for write, %d\n", buffsentlength);
			break;
		}
		wbuffer[buffsentlength] = '\0';

		tiber_msleep(10);

		int buffreadlength = tiber_read(fd, rbuffer, 999);
		if(buffreadlength == -1 || buffreadlength == 0)
		{
			printf("premature server connection closed for read, %d\n", buffreadlength);
			break;
		}
		rbuffer[buffreadlength] = '\0';

		if(0 != strcmp(rbuffer, wbuffer))
		{
			printf("response of %s was %s\n", wbuffer, rbuffer);
			exit(-1);
		}

		tiber_msleep(10);
	}

	tiber_close(fd);

	return NULL;
}

int tiber_main()
{
	tiber* tb[5000];

	for(int i = 0; i < sizeof(tb)/sizeof(tb[0]); i++)
		tb[i] = new_tiber(NULL, make_1000_requests, NULL, 64*1024, 0);

	void* result = NULL;
	for(int i = 0; i < sizeof(tb)/sizeof(tb[0]); i++)
		tiber_join(tb[i], &result);

	return 0;
}