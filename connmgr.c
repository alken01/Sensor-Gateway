/**
 * \author Alken Rrokaj
 */

 // #define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <poll.h>
#include "lib/dplist.h"
#include "connmgr.h"
#include "config.h"

typedef struct pollfd pollfd_t;

typedef struct{
	pollfd_t file_d;
	sensor_id_t sensor_id;
	tcpsock_t* socket_id;
	sensor_ts_t last_modified;
} poll_info_t;

// dpl_create functions
void* element_copy(void* element);
void element_free(void** element);
int element_compare(void* x, void* y);

// helper functions
void element_print(sensor_data_t sensor_data);
int dpl_check_unique(dplist_t* list, sensor_id_t* data_id);
void log_event(char* sequence_number, char* log_event, int sensor_id);


//create a dplist with the connections
dplist_t* dpl_connections;

void connmgr_listen(int port_number){
	// initiate dpl_connections
	dpl_connections = dpl_create(element_copy, element_free, element_compare);

	// open file in binary and in text format
	FILE* fp_sensor_data_recv = fopen("sensor_data_recv", "wb");
	FILE* fp_sensor_data_text = fopen("sensor_data_recv_text", "w");

	//open tcp socket
	tcpsock_t* socket;
	if(tcp_passive_open(&socket, port_number) != TCP_NO_ERROR) printf("CANNOT CREATE SERVER\n"), exit(EXIT_FAILURE);
	// get the socket descriptor
	pollfd_t pollfd;
	if(tcp_get_sd(socket, &(pollfd.fd)) != TCP_NO_ERROR) printf("SOCKET NOT BOUND\n"), exit(EXIT_FAILURE);

	// start server
	poll_info_t poll_server = {
		.last_modified = time(NULL), // last event in server
		.socket_id = socket,
		.file_d = pollfd,
	};

	// only listen to incoming 
	poll_server.file_d.events = POLLIN;

	// add the poll_server as the first index 
	dpl_connections = dpl_insert_at_index(dpl_connections, &poll_server, 0, true);

	while(1){
		// get size of list
		int list_size = dpl_size(dpl_connections);

		// iterate through the list
		for(int index = 0; index < list_size; index++){

			poll_info_t* poll_at_index = dpl_get_element_at_index(dpl_connections, index);

			if(poll(&(poll_at_index->file_d), 1, 0) > 0 && poll_at_index->file_d.revents == POLLIN){

				// in the first index we will only get notified about new connections 
				if(index == 0){
					// get socket
					tcpsock_t* new_socket;
					pollfd_t new_fd;
					if(tcp_wait_for_connection(poll_at_index->socket_id, &new_socket) != TCP_NO_ERROR) exit(EXIT_FAILURE);
					if(tcp_get_sd(new_socket, &(new_fd.fd)) != TCP_NO_ERROR) exit(EXIT_FAILURE);

					// initialise the sensor
					poll_info_t insert_sensor = {
						.last_modified = time(NULL),
						.socket_id = new_socket,
						.file_d = new_fd
					};

					//also listen if the sensor quits
					insert_sensor.file_d.events = POLLIN | POLLHUP;

					// insert the sensor in the list
					dpl_connections = dpl_insert_at_index(dpl_connections, &insert_sensor, dpl_size(dpl_connections), true);

				} else{
					// create a sensor_data
					sensor_data_t sensor_data;

					// get the buf_size
					int sit = (int) sizeof(sensor_id_t);
					int sdt = (int) sizeof(sensor_value_t);
					int stt = (int) sizeof(sensor_ts_t);

					//reveive the data
					tcp_receive(poll_at_index->socket_id, &sensor_data.id, &sit);
					tcp_receive(poll_at_index->socket_id, &sensor_data.value, &sdt);
					tcp_receive(poll_at_index->socket_id, &sensor_data.ts, &stt);

					// update the ID and log_event if this is the first data from this sensor
					if(poll_at_index->sensor_id != sensor_data.id){

						//check if the sensor is unique
						if(!dpl_check_unique(dpl_connections, &sensor_data.id)){
							log_event("1c", "NON-UNIQUE SERVER OPENED ID:", sensor_data.id);
							tcp_close(&(poll_at_index->socket_id));
							dpl_connections = dpl_remove_at_index(dpl_connections, index, true);
							list_size = dpl_size(dpl_connections);
							break;
						}

						// update the sensor ID and log the event
						poll_at_index->sensor_id = sensor_data.id;
						log_event("1a", "NEW CONNECTION SENSOR ID:", poll_at_index->sensor_id);
					}
					// fix a bug that kept repeating the last value
					else if(poll_at_index->last_modified == sensor_data.ts){
						tcp_close(&poll_at_index->socket_id);
						dpl_connections = dpl_remove_at_index(dpl_connections, index, true);
						goto terminate;
					}

					//update the poll_at_index time
					poll_at_index->last_modified = time(NULL);

					// write the data in the binary file
					fwrite(&sensor_data.id, sizeof(sensor_id_t), 1, fp_sensor_data_recv);
					fwrite(&sensor_data.value, sizeof(sensor_data_t), 1, fp_sensor_data_recv);
					fwrite(&sensor_data.ts, sizeof(sensor_ts_t), 1, fp_sensor_data_recv);

					// print it in the text file
					element_print(sensor_data);
					fprintf(fp_sensor_data_text, "ID: %u   VAL: %f   TIME: %ld\n", sensor_data.id, sensor_data.value, sensor_data.ts);
				}
			}

			// if the sensor at index has not sent data in TIMEOUT seconds
			// or it has sent a POLLHUP
			// close that connecion and remove it from the list

			if(((poll_at_index->last_modified + TIMEOUT) < time(NULL) && index > 0) || poll_at_index->file_d.revents > 1){
				printf("CLOSED CONNECTION SENSOR ID:%d\n", poll_at_index->sensor_id);
				log_event("1b", "CLOSED CONNECTION SENSOR ID:", poll_at_index->sensor_id);
				tcp_close(&(poll_at_index->socket_id));
				dpl_connections = dpl_remove_at_index(dpl_connections, index, true);
				poll_server.last_modified = time(NULL);
				list_size = dpl_size(dpl_connections);
			}

			// in case there is no sensor in the list and TIMOUT seconds have passed
			// close the files and stop connmgr_listen()
			if(list_size == 1 && (poll_server.last_modified + TIMEOUT) < time(NULL)){
				tcp_close(&(poll_at_index->socket_id));
				goto terminate;
			}
		}
	}
terminate:
	log_event("1d", "CLOSED CONNECTION MANAGER:", port_number);
	tcp_close(&(poll_server.socket_id));
	connmgr_free();
	fclose(fp_sensor_data_recv);
	fclose(fp_sensor_data_text);
}

void connmgr_free(){
	dpl_free(&dpl_connections, true);
}

void* element_copy(void* element){
	poll_info_t* copy = malloc(sizeof(poll_info_t));
	copy->file_d = ((poll_info_t*) element)->file_d;
	copy->sensor_id = ((poll_info_t*) element)->sensor_id;
	copy->socket_id = ((poll_info_t*) element)->socket_id;
	copy->last_modified = ((poll_info_t*) element)->last_modified;
	return copy;
}

void element_free(void** element){
	free(*element);
}

int element_compare(void* x, void* y){
	sensor_id_t x_id = *(sensor_id_t*) x;
	sensor_id_t y_id = ((poll_info_t*) y)->sensor_id;
	if(x_id == y_id) return 0;
	if(x_id > y_id) return 1;
	if(x_id < y_id) return -1;
	return 0;
}

// print data in the terminal
void element_print(sensor_data_t sensor_data){
	printf("ID: %u   VAL: %f   TIME: %ld\n", sensor_data.id, sensor_data.value, sensor_data.ts);
}

// log event
void log_event(char* sequence_number, char* log_event, int sensor_id){
	// open gateway in append mode
	FILE* fp_log = fopen("gateway.log", "a");
	fprintf(fp_log, "\nSEQ_NR: %s   TIME: %ld\n%s %d\n", sequence_number, time(NULL), log_event, sensor_id);
	fclose(fp_log);
}

// check if the sensor_id is unique, else return fasle
int dpl_check_unique(dplist_t* list, sensor_id_t* data_id){
	int list_size = dpl_size(list);
	if(list_size <= 1) return true;
	for(int i = 0; i < dpl_size(list); i++)
		if(element_compare(data_id, dpl_get_element_at_index(list, i)) == 0)
			return false;

	return true;
}
