#include<tiber/tiber.h>
#include<tiber/tiber_io.h>
#include<tiber/tiber_stream_for_file_descriptor.h>

#include<connman/comm_address.h>
#include<connman/stacked_stream.h>
#include<connman/ssl_ctx_helper.h>
#include<connman/ssl_stream.h>

#include<httpparser/http_request.h>
#include<httpparser/http_response.h>
#include<httpparser/http_header_util.h>

#include<jsonparser/json_parser.h>

const dstring baseuri = get_dstring_pointing_to_literal_cstring("https://api.dictionaryapi.dev/api/v2/entries/en/");

SSL_CTX* ssl_ctx = NULL;

comm_address server_address;

comm_address get_comm_address(const dstring* uri_dstr);

void* get_meaning(void* word);

int tiber_main(int argc, char** argv)
{
	if(argc <= 1)
	{
		printf("mechanism to use this binary: ./test_http_request_for_meaning.out word1 word2 word3\n");
		return -1;
	}

	// initialize openssl and create an ssl context
	ssl_lib_init();
	ssl_ctx = get_ssl_ctx_for_client(NULL, NULL);

	// server address to communicate with
	server_address = get_comm_address(&baseuri);

	tiber** tb = malloc(sizeof(tiber*) * argc);

	for(int i = 1; i < argc; i++)
		tb[i] = new_tiber(NULL, get_meaning, argv[i], 512*1024, 0, NULL, NULL);

	for(int i = 1; i < argc; i++)
	{
		char* meaning;
		tiber_join(tb[i], (void**)(&meaning));
		printf("%s : %s\n\n", argv[i], meaning);
		free(meaning);
	}

	free(tb);

	if(ssl_ctx != NULL)
		destroy_ssl_ctx(ssl_ctx);

	return 0;
}

#include<cutlery/deferred_callbacks.h>

// returns file-discriptor to the socket, through which client connection has been made
int make_connection(comm_address* server_addr_p, comm_address* client_addr_p, uint64_t timeout_in_milliseconds)
{
	// then we try to set up socket and retrieve the file discriptor that is returned
	int err = socket(server_addr_p->ADDRESS.sa_family, server_addr_p->PROTOCOL, 0);
    if(err == -1)
    	return err;
    int fd = err;

    // register this fd with tiber_io
	register_fd_with_tiber_io(fd);

	// bind client address struct with the file descriptor, if a specific client address is provided
	if(client_addr_p)
	{
		int err = bind(fd, &(client_addr_p->ADDRESS), get_sockaddr_size(client_addr_p));
		if(err)
		{
			tiber_close(fd);
			return err;
		}
	}

	// next we try and attempt to connect the socket formed whose file discriptor we have
	err = tiber_connect(fd, &(server_addr_p->ADDRESS), get_sockaddr_size(server_addr_p));
	if(err == -1)
	{
		tiber_close(fd);
		return err;
	}

	if(timeout_in_milliseconds != BLOCKING && timeout_in_milliseconds != NON_BLOCKING)
	{
		struct timeval tv = timespec_to_timeval(timespec_from_milliseconds(timeout_in_milliseconds));
		setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
	}

	return fd;
}

void* get_meaning(void* word)
{
	char* result = malloc(4096);result[0] = '\0';
	NEW_DEFERRED_CALLS(16);

	int fd = make_connection(&server_address, NULL, 6000); // 6 milliseconds timeout

	stream* raw_stream;

	stream fd_stream;
	initialize_tiber_stream_for_fd(&fd_stream, fd);
	raw_stream = &fd_stream;

	stream ssl_stream;
	if(ssl_ctx != NULL)
	{
		initialize_stream_for_ssl_client_over_stream(&ssl_stream, ssl_ctx, "api.dictionaryapi.dev", &fd_stream);
		raw_stream = &ssl_stream;
	}

	http_request_head hrq;
	if(!init_http_request_head_from_uri(&hrq, &baseuri))
		goto EXIT;
	DEFER(deinit_http_request_head, &hrq);
	hrq.method = GET;
	if(!concatenate_dstring(&(hrq.path), &get_dstring_pointing_to_cstring(word)))
		goto EXIT;
	if(!insert_literal_cstrings_in_dmap(&(hrq.headers), "accept", "*/*"))
		goto EXIT;
	if(!insert_literal_cstrings_in_dmap(&(hrq.headers), "accept-encoding", "gzip,deflate"))
		goto EXIT;

	http_response_head hrp;
	if(!init_http_response_head(&hrp))
		goto EXIT;
	DEFER(deinit_http_response_head, &hrp);

	int error = 0;

	if(HTTP_NO_ERROR != serialize_http_request_head(raw_stream, &hrq))
	{
		sprintf(result, "error serializing http request head\n");
		goto EXIT;
	}
	flush_all_from_stream(raw_stream, &error);
	if(error)
	{
		sprintf(result, "%d error flushing request head\n", error);
		goto EXIT;
	}
	if(HTTP_NO_ERROR != parse_http_response_head(raw_stream, &hrp))
	{
		sprintf(result, "error parsing http response head\n");
		goto EXIT;
	}

	stacked_stream sstrm;
	initialize_stacked_stream(&sstrm);
	DEFER(deinitialize_stacked_stream, &sstrm);

	if(0 > intialize_http_body_and_decoding_streams_for_reading(&sstrm, raw_stream, &(hrp.headers)))
	{
		sprintf(result, "error initializing one of body or decoding streams\n");
		goto EXIT;
	}
	DEFER(close_deinitialize_free_all_from_READER_stacked_stream, &sstrm);

	int json_parse_error = JSON_NO_ERROR;
	json_node* js_resp = parse_json(get_top_of_stacked_stream(&sstrm, READ_STREAMS), 2048, 64, &json_parse_error);
	if(json_parse_error)
	{
		sprintf(result, "error parsing json %d, status code was %d\n", json_parse_error, hrp.status);
		goto EXIT;
	}
	DEFER(delete_json_node, js_resp);

	// make sure that the js_resp has the result
	int meaning_found = 0;
	int non_existing = 0;
	json_node* js = get_json_node_from_json_node(js_resp, STATIC_JSON_ACCESSOR(
		JSON_ARRAY_INDEX(0),
		JSON_OBJECT_KEY_literal("meanings")
	), &non_existing);
	if(!non_existing && js != NULL && js->type == JSON_ARRAY)
	{
		for(cy_uint i = 0; i < get_element_count_arraylist(&(js->json_array)) && !meaning_found; i++)
		{
			int non_existing = 0;
			json_node* partOfSpeech = get_json_node_from_json_node(js, STATIC_JSON_ACCESSOR(
				JSON_ARRAY_INDEX(i),
				JSON_OBJECT_KEY_literal("partOfSpeech")
			), &non_existing);
			if(non_existing)
				continue;
			if(partOfSpeech == NULL || partOfSpeech->type != JSON_STRING || 0 != compare_dstring(&(partOfSpeech->json_string), &get_dstring_pointing_to_literal_cstring("noun")))
				continue;

			json_node* definition0 = get_json_node_from_json_node(js, STATIC_JSON_ACCESSOR(
				JSON_ARRAY_INDEX(i),
				JSON_OBJECT_KEY_literal("definitions"),
				JSON_ARRAY_INDEX(0),
				JSON_OBJECT_KEY_literal("definition")
			), &non_existing);
			if(non_existing)
				continue;
			if(definition0 != NULL && definition0->type == JSON_STRING)
			{
				sprintf(result, printf_dstring_format "\n", printf_dstring_params(&(definition0->json_string)));
				meaning_found = 1;
			}
		}
	}

	if(!meaning_found)
		sprintf(result, "NO MEANING_FOUND\n");

	EXIT:;
	CALL_ALL_DEFERRED();
	if(ssl_ctx != NULL)
	{
		close_stream(&ssl_stream, &error);
		deinitialize_stream(&ssl_stream);
	}
	close_stream(&fd_stream, &error);
	deinitialize_stream(&fd_stream);
	return result;
}

#include<httpparser/uri_parser.h>

const char* port_80 = "80";
const char* port_443 = "443";

const dstring HTTP_dstr = get_dstring_pointing_to_literal_cstring("http");
const dstring HTTPS_dstr = get_dstring_pointing_to_literal_cstring("https");

comm_address get_comm_address(const dstring* uri_dstr)
{
	uri uriv;
	if(!init_uri(&uriv))
		return (comm_address){};

	if(URI_NO_ERROR != parse_uri(&uriv, uri_dstr))
		goto ERROR;

	// host must not be empty
	if(is_empty_dstring(&(uriv.host)))
		goto ERROR;

	// make host and port c compatible strings
	if(get_unused_capacity_dstring(&(uriv.host)) == 0 && !expand_dstring(&(uriv.host), 1))
		goto ERROR;
	get_byte_array_dstring(&(uriv.host))[get_char_count_dstring(&(uriv.host))] = '\0';

	if(get_unused_capacity_dstring(&(uriv.port)) == 0 && !expand_dstring(&(uriv.port), 1))
		goto ERROR;
	get_byte_array_dstring(&(uriv.port))[get_char_count_dstring(&(uriv.port))] = '\0';

	const char* hostname = get_byte_array_dstring(&(uriv.host));
	const char* port = get_byte_array_dstring(&(uriv.port));

	if(compare_dstring(&(uriv.scheme), &HTTP_dstr) == 0)
	{
		ssl_ctx = NULL;
		if(is_empty_dstring(&(uriv.port)))
			port = port_80;
	}
	else if(compare_dstring(&(uriv.scheme), &HTTPS_dstr) == 0)
	{
		if(is_empty_dstring(&(uriv.port)))
			port = port_443;
	}
	else
		goto ERROR;

	comm_address server_address;
	if(0 == lookup_by_name(hostname, port, SOCK_STREAM, AF_UNSPEC, &server_address, 1)) // if lookup fails, then fail
		goto ERROR;

	ERROR:;
	deinit_uri(&uriv);
	return server_address;
}