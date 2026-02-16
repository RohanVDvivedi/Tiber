#include<tiber/tiber.h>
#include<tiber/tiber_channel.h>

#include<stdio.h>

tiber_channel tch;

void* producer(void* t)
{
	printf("PRODUCER COMPLETED %p %ld\n", tiber_self(), pthread_self());

	return NULL;
}

void* consumer(void* t)
{
	printf("CONSUMER COMPLETED %p %ld\n", tiber_self(), pthread_self());

	return NULL;
}

int tiber_main()
{
	initialize_tiber_channel(&tch, UNBOUNDED_TIBER_CHANNEL_CAPACITY);

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