#include<tiber/tiber.h>
#include<tiber/tiber_io.h>

#include<unistd.h>

/*
	this source is useful for, it provides you with a ready-to-go main, and a default runtime for all new tibers
	it creates a global tiber_runtime and calls the int tiber_main(), as the new derieved main function

	check out test/test.c, test/test2.c and test/test3.c and other tests to figure out how to use tiber_main() instead of main()
	if you use this main function you need your own int tiber_main(argc, argv). just like in the tests

	tibers are designed to pick the runtime from the current tiber, if an explicit tiber_runtime is passed as NULL
	and if the tiber is spawned from a pthread, then global_runtime is used, which is set by this (tiber library's) main
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

tiber_runtime* global_runtime = NULL;

// amount of stack space for the threads running in the global runtime
#define GLOBAL_RUNTIME_THREAD_POOL_STACK_SIZE (3 * 1024 * 1024)

// stack size of the main tiber, big enough for most use cases, and to fit on the processor created main thread
// and not too big to not fit on main thread
#define MAIN_TIBER_STACK_SIZE (3 * 1024 * 1024)

int main(int argc, char** argv)
{
	// global inputs to tiber
	global_argc = argc;
	global_argv = argv;

	initialize_tiber_io();

	// create runtime for sufficient threads each with 3 MB of stack space
	global_runtime = new_tiber_runtime(sysconf(_SC_NPROCESSORS_ONLN), GLOBAL_RUNTIME_THREAD_POOL_STACK_SIZE);

	// maka a main running tiber on this very stack
	tiber main_tiber;
	void* main_tiber_stack = alloca(MAIN_TIBER_STACK_SIZE); // give it 3MB stack from the main thread's stack
	new_tiber(global_runtime, tiber_main_wrapper, NULL, MAIN_TIBER_STACK_SIZE, 0, &main_tiber, main_tiber_stack);

	// then join with it, return not caught
	tiber_join(&main_tiber, NULL);

	delete_tiber_runtime(global_runtime);

	deinitialize_tiber_io();

	return global_return;
}