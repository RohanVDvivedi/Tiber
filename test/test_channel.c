#include<tiber/tiber.h>
#include<tiber/tiber_channel.h>

#include<stdio.h>
#include<string.h>

tiber_channel tch;

void* producer(void* t)
{
	char buffer[128];
	for(int i = 0; i < 10; i++)
	{
		sprintf(buffer, "p-%d-%s", i, (tiber_self() ? "tiber77" : "pthread"));
		write_to_tiber_channel(&tch, buffer, strlen(buffer), ALL_OR_NONE, 1000);
	}

	printf("PRODUCER COMPLETED %p %ld\n", tiber_self(), pthread_self());

	return NULL;
}

void* consumer(void* t)
{
	char buffer[128];
	for(int i = 0; i < 10; i++)
	{
		cy_uint bytes = read_from_tiber_channel(&tch, buffer, 11, ALL_OR_NONE, 1000);
		printf("c -> %.*s -> %p %ld\n", ((int)bytes), buffer, tiber_self(), pthread_self());
	}

	printf("CONSUMER COMPLETED %p %ld\n", tiber_self(), pthread_self());

	return NULL;
}

int tiber_main()
{
	initialize_tiber_channel(&tch, /*UNBOUNDED_TIBER_CHANNEL_CAPACITY*/ 32);

	pthread_t ppt;
	pthread_create(&ppt, NULL, producer, NULL);
	tiber* pt = new_tiber(NULL, producer, NULL, 2 * 1024 * 1024, 0);

	pthread_t cpt;
	pthread_create(&cpt, NULL, consumer, NULL);
	tiber* ct = new_tiber(NULL, consumer, NULL, 2 * 1024 * 1024, 0);

	void* return_value;
	pthread_join(ppt, &return_value);
	tiber_join(pt, &return_value);
	pthread_join(cpt, &return_value);
	tiber_join(ct, &return_value);

	deinitialize_tiber_channel(&tch);
	return 0;
}