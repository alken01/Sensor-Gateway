/**
 * \author Alken Rrokaj
 */

#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <poll.h>
#include "lib/dplist.h"
#include "connmgr.h"
#include "config.h"
#include "sbuffer.h"

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
static void log_event(char* log_event, int sensor_id);

// global variables
static dplist_t* dpl_connections;
// multithreading variables
pthread_cond_t* data_cond;
pthread_mutex_t* datamgr_lock;
int* data_mgr;

pthread_cond_t* db_cond;
pthread_mutex_t* db_lock;
int* data_sensor_db;

pthread_rwlock_t* connmgr_lock;
int* connmgr_working;

pthread_mutex_t* fifo_mutex;
int* fifo_fd;

void connmgr_init(config_thread_t* config_thread){
	data_cond = config_thread->data_cond;
	datamgr_lock = config_thread->datamgr_lock;
	data_mgr = config_thread->data_mgr;

	db_cond = config_thread->db_cond;
	db_lock = config_thread->db_lock;
	data_sensor_db = config_thread->data_sensor_db;

	connmgr_lock = config_thread->connmgr_lock;
	connmgr_working = config_thread->connmgr_working;

	fifo_fd = config_thread->fifo_fd;
	fifo_mutex = config_thread->fifo_mutex;
	
	*data_mgr = 0;
	*data_sensor_db = 0;
	*connmgr_working = 1;
}


void connmgr_listen(int port_number, sbuffer_t** buffer){
	// create and initialize dpl_connections
	dpl_connections = dpl_create(element_copy, element_free, element_compare);

	// open file 
	FILE* fp_sensor_data_text = fopen("sensor_data_recv", "w");

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
	
	
	int list_size;
	while(1){
		// get size of list
		list_size = dpl_size(dpl_connections);

		// iterate through the list
		for(int index = 0; index < list_size; index++){

			poll_info_t* poll_at_index = dpl_get_element_at_index(dpl_connections, index);

			if(poll(&(poll_at_index->file_d), 1, 0) > 0 && poll_at_index->file_d.revents == POLLIN){

				// in the first index we will only get notified about new connections 
				if(index == 0){
					// get socket
#ifdef DEBUG
					printf(PURPLE_CLR);
					printf("CONNMGR: NEW CONNECTION DETECTED.\n");
					printf(OFF_CLR);
#endif
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
#ifdef DEBUG
					printf(PURPLE_CLR);
					printf("CONNMGR: NEW CONNECTION INCOMING.\n");
					printf(OFF_CLR);
#endif
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
#ifdef DEBUG
					printf(PURPLE_CLR);
					printf("CONNMGR: NEW DATA RECEIVED.\n");
					printf(OFF_CLR);
#endif
					// update the ID and log_event if this is the first data from this sensor
					if(poll_at_index->sensor_id != sensor_data.id){
						//check if it is a duplicate sensor
						if(!dpl_check_unique(dpl_connections, &sensor_data.id)){
							log_event("NON-UNIQUE SERVER OPENED ID:", sensor_data.id);
							tcp_close(&(poll_at_index->socket_id));
							dpl_connections = dpl_remove_at_index(dpl_connections, index, true);
							list_size = dpl_size(dpl_connections);
							break;
						}
						// update the sensor ID and log the event
						poll_at_index->sensor_id = sensor_data.id;
						log_event("NEW CONNECTION SENSOR ID:", poll_at_index->sensor_id);
#ifdef DEBUG
						printf(PURPLE_CLR);
						printf("NEW CONNECTION SENSOR ID: %d\n", poll_at_index->sensor_id);
						printf(OFF_CLR);
#endif
					}
					//add it in the buffer
					sensor_data_t insert_data = {
						.id = sensor_data.id,
						.value = sensor_data.value,
						.ts = sensor_data.ts
				};
					// insert it in the buffer
					sbuffer_insert(*buffer, &insert_data);

					// let the other threads know there is data to read
					pthread_mutex_lock(datamgr_lock);
					data_mgr++;
					pthread_mutex_unlock(datamgr_lock);
					
					pthread_mutex_lock(db_lock);
					data_sensor_db++;
					pthread_mutex_unlock(db_lock);
					
					pthread_cond_broadcast(data_cond);
					pthread_cond_broadcast(db_cond);

					//update the poll_at_index time
					poll_at_index->last_modified = time(NULL);

					// print it in the text file
					fprintf(fp_sensor_data_text, "ID: %u   VAL: %f   TIME: %ld\n", sensor_data.id, sensor_data.value, sensor_data.ts);
#ifdef DEBUG
					printf(PURPLE_CLR);
					printf("CONNMGR: ID: %u   VAL: %f   TIME: %ld\n", sensor_data.id, sensor_data.value, sensor_data.ts);
					printf(OFF_CLR);
#endif
					// } else if(result == TCP_CONNECTION_CLOSED){
					// 	poll_at_index->file_d.revents = POLLHUP;
			}
		}

			// #ifdef DEBUG
			// 			printf("TIMEOUT IN: %ld\n", TIMEOUT + poll_at_index->last_modified - time(NULL));
			// #endif

						// if the sensor at index has not sent data in TIMEOUT seconds or it has sent a POLLHUP
						// close that connecion and remove it from the list
			if(((poll_at_index->last_modified + TIMEOUT) < time(NULL) && index > 0) || poll_at_index->file_d.revents > 1){
				printf(PURPLE_CLR);
				printf("CLOSED CONNECTION SENSOR ID:%d\n", poll_at_index->sensor_id);
				printf(OFF_CLR);
				log_event("CLOSED CONNECTION SENSOR ID:", poll_at_index->sensor_id);
				tcp_close(&(poll_at_index->socket_id));
				dpl_connections = dpl_remove_at_index(dpl_connections, index, true);
				poll_server.last_modified = time(NULL);
				list_size = dpl_size(dpl_connections);
			}

	}
		if(list_size == 1 && (poll_server.last_modified + TIMEOUT) < time(NULL)){
			break;
		}
}
	pthread_rwlock_wrlock(connmgr_lock);
	*connmgr_working = 0;
	pthread_rwlock_unlock(connmgr_lock);

	log_event("CLOSED CONNECTION MANAGER:", port_number);
	tcp_close(&(poll_server.socket_id));
	connmgr_free();
	fclose(fp_sensor_data_text);
#ifdef DEBUG
	printf(PURPLE_CLR);
	printf("CLOSING CONNMGR.\n");
	printf(OFF_CLR);
#endif
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
	if(x_id > y_id) return 1;
	if(x_id < y_id) return -1;
	return 0;
}


// log event
static void log_event(char* log_event, int sensor_id){
	// open gateway in append mode
	FILE* fp_log = fopen("gateway.log", "a");
	fprintf(fp_log, "\nSEQ_NR: xxx  TIME: %ld\n%s %d\n", time(NULL), log_event, sensor_id);
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

