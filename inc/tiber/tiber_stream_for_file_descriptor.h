#ifndef TIBER_STREAM_FOR_FILE_DESCRIPTOR_H
#define TIBER_STREAM_FOR_FILE_DESCRIPTOR_H

#include<cutlery/stream.h>

/*
	This stream is designed to be used with unix/linux like file descriptors, it makes the file_descriptor non-blocking, before giving you back the stream.
*/

int initialize_tiber_stream_for_fd(stream* strm, int fd);

#endif