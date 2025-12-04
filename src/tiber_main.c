#include<tiber/tiber.h>
#include<tiber/tiber_io.h>

#include<unistd.h>

/*
	this source is only useful for beginners, it provides you with a ready-to-go main
	it creates a global tiber_runtime and calls the int tiber_main(), as the new derieved main function

	check out test/test.c, test/test2.c and test/test3.c to figure out how to use tiber_main() instead of main()
	if you use this main function you need your own int tiber_main(argc, argv). just like in the tests

	tibers are designed to pick the runtime from the current tiber, if an explicit tiber_runtime is passed as NULL
*/

int global_argc = 0;
char** global_argv = NULL;
int global_return = 0;

int tiber_main();

void* tiber_main_wrapper(void* t)
{
	global_return = tiber_main(global_argc, global_argv);
	return NULL;
}

int main(int argc, char** argv)
{
	// global inputs to tiber
	global_argc = argc;
	global_argv = argv;

	initialize_tiber_io();

	// create runtime for 8 threads each with 3 MB of stack space
	tiber_runtime* tr = new_tiber_runtime(sysconf(_SC_NPROCESSORS_ONLN), 3 * 1024 * 1024);

	tiber* tb = new_tiber(tr, tiber_main_wrapper, NULL, 3 * 1024 * 1024, 0);

	void* result = NULL;
	tiber_join(tb, &result);

	delete_tiber_runtime(tr);

	deinitialize_tiber_io();

	return global_return;
}