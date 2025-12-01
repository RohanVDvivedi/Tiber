#include<tiber/tiber.h>

#include<unistd.h>

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

	// create runtime for 8 threads each with 3 MB of stack space
	tiber_runtime* tr = new_tiber_runtime(sysconf(_SC_NPROCESSORS_ONLN), 3 * 1024 * 1024);

	tiber* tb = new_tiber(tr, tiber_main_wrapper, NULL, 3 * 1024 * 1024, 0);

	void* result = NULL;
	tiber_join(tb, &result);

	delete_tiber_runtime(tr);

	return global_return;
}