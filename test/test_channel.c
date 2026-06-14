#include<tiber/tiber.h>
#include<tiber/tiber_channel.h>

#include<stdio.h>
#include<string.h>

tiber_channel tch;

tiber_channel completed;

void* producer(void* t)
{
	char buffer[128];
	for(int i = 0; i < 10; i++)
	{
		sprintf(buffer, "p-%d-%s", i, (tiber_self() ? "tiber77" : "pthread"));
		write_to_tiber_channel(&tch, buffer, strlen(buffer), ALL_OR_NONE, 1000);
	}

	if(tiber_self()) // tiber will send a completion byte on completion
	{
		uint8_t completion_byte = 1;
		write_to_tiber_channel(&completed, &completion_byte, 1, ALL_OR_NONE, BLOCKING);
	}
	else // pthread will wait to receive it and once received close the tiber finally, to prevent any more writes
	{
		uint8_t completion_byte;
		read_from_tiber_channel(&completed, &completion_byte, 1, ALL_OR_NONE, BLOCKING);
		close_tiber_channel(&tch);
	}

	printf("PRODUCER COMPLETED %p %ld\n", tiber_self(), pthread_self());

	return NULL;
}

void* consumer(void* t)
{
	char buffer[128];
	while((get_bytes_readable_tiber_channel(&tch) > 0) || !is_closed_tiber_channel(&tch)) // keep on reading while the channel is open or has pending bytes to be read
	{
		cy_uint bytes = read_from_tiber_channel(&tch, buffer, 11, ALL_OR_NONE, 1000);
		printf("c -> %.*s -> %p %ld\n", ((int)bytes), buffer, tiber_self(), pthread_self());
	}

	printf("CONSUMER COMPLETED %p %ld\n", tiber_self(), pthread_self());

	return NULL;
}

int tiber_main()
{
	int res = initialize_tiber_channel(&tch, /*UNBOUNDED_TIBER_CHANNEL_CAPACITY*/ 32);
	printf("init channel = %d\n", res);

	res = initialize_tiber_channel(&completed, 1);
	printf("init completion channel = %d\n", res);

	pthread_t ppt;
	pthread_create(&ppt, NULL, producer, NULL);
	tiber* pt = new_tiber(NULL, producer, NULL, 2 * 1024 * 1024, 0, NULL, NULL);

	pthread_t cpt;
	pthread_create(&cpt, NULL, consumer, NULL);
	tiber* ct = new_tiber(NULL, consumer, NULL, 2 * 1024 * 1024, 0, NULL, NULL);

	pthread_join(ppt, NULL);
	tiber_join(pt, NULL);
	pthread_join(cpt, NULL);
	tiber_join(ct, NULL);

	deinitialize_tiber_channel(&tch);
	deinitialize_tiber_channel(&completed);

	printf("TEST COMPLETE\n");

	return 0;
}