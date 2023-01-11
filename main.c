#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h> 
#include <poll.h>
#include <errno.h>
#include <inttypes.h>
#include <time.h>

#include "config.h"
#include "sbuffer.h"
#include "connmgr.h"
#include "datamgr.h"
#include "sensor_db.h"

#include "lib/tcpsock.h"
#include "lib/dplist.h"

#define MAIN_PROCESS_THREAD_NR 3
// define as 1 to drop existing table, 0 to keep existing table
#define DB_FLAG 1

// functions
void* connmgr_th(void* arg);
void* datamgr_th(void* arg);
void* sensor_db_th(void* arg);

int print_help();

// thread variables
pthread_cond_t data_cond;
pthread_mutex_t datamgr_lock;
int* data_mgr;

pthread_cond_t db_cond;
pthread_mutex_t db_lock;
int* data_sensor_db;

pthread_rwlock_t connmgr_lock;
bool* connmgr_working;

pthread_mutex_t fifo_mutex;

pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
int* fifo_fd = 0;
int log_sequence_number = 0;


sbuffer_t* buffer;

int main(int argc, char* argv[]){
    // check if port_number arguments passed
    if(argv[1] == NULL) return print_help();

    //get the port number
    int port_number = atoi(argv[1]);

    // fork into two processes
    // int pid = 1;
    // if(pid == -1) return -1;

    // // Create the FIFO if it doesn't already exist
    // if (mkfifo(FIFO_NAME, 0660) < 0) {
    //     if (errno != EEXIST) {
    //         perror("Could not create log FIFO");
    //         exit(1);
    //     }
    // }

    // // the child handles the log process
    // if(pid == 0){
    //     int log_fifo;

    //     // Open the FIFO for reading
    //     log_fifo = open(FIFO_NAME, O_RDONLY);
    //     if(log_fifo < 0){
    //         perror("Could not open log FIFO");
    //         exit(1);
    //     }

    //     while(1){
    //         char log_event_info[256];
    //         int bytes_read;

    //         // Read a log event from the FIFO
    //         bytes_read = read(log_fifo, log_event_info, sizeof(log_event_info));
    //         if(bytes_read < 0){
    //             perror("Error reading from log FIFO");
    //             exit(1);
    //         } else if(bytes_read == 0){
    //             // End of file
    //             break;
    //         }

    //         // Write the log event to the log file
    //         pthread_mutex_lock(&log_mutex);
    //         FILE* log_file = fopen(LOG_FILE, "a");
    //         if(log_file == NULL){
    //             perror("Could not open log file");
    //             exit(1);
    //         }
    //         time_t now;
    //         time(&now);
    //         fprintf(log_file, "%d %s %s\n", log_sequence_number++, ctime(&now), log_event_info);
    //         fclose(log_file);
    //         printf("LOGGED");
    //         pthread_mutex_unlock(&log_mutex);
    //     }

    //     close(log_fifo);
    //     exit(EXIT_SUCCESS);
    // }
    
#ifdef DEBUG
    printf("INITIALIZING SENSOR GATEWAY\n");
#endif
    
    // initialize all the variables
    data_mgr = malloc(sizeof(int));
    data_sensor_db = malloc(sizeof(int));
    connmgr_working = malloc(sizeof(bool));
    // fifo_fd = malloc(sizeof(int));

    *data_mgr = 0;
	*data_sensor_db = 0;
	*connmgr_working = true;

    // initialize the buffer
    sbuffer_init(&buffer);

    // initialize the pthreads
    pthread_cond_init(&data_cond, NULL);
    pthread_mutex_init(&datamgr_lock, NULL);
    
    pthread_cond_init(&db_cond, NULL);
    pthread_mutex_init(&db_lock, NULL);

    pthread_rwlock_init(&connmgr_lock, NULL);    
    pthread_mutex_init(&fifo_mutex, NULL);

#ifdef DEBUG
    printf("INITIALIZING THREADS\n");
#endif
    // create the threads
    pthread_t threads[MAIN_PROCESS_THREAD_NR];
    // connmgr thread
    pthread_create(&threads[0], NULL, &connmgr_th, &port_number);
    // database thread
    READ_TH_ENUM DBT = DB_THREAD;
    pthread_create(&threads[1], NULL, &sensor_db_th, &DBT);
    // datamgr thread
    READ_TH_ENUM DMT = DATAMGR_THREAD;
    pthread_create(&threads[2], NULL, &datamgr_th, &DMT);

    // join all the threads after they are done
    for(int i = 0; i < MAIN_PROCESS_THREAD_NR; i++)
        pthread_join(threads[i], NULL);

#ifdef DEBUG
    printf("JOINED THREADS\n");
#endif

    // destroy the threads
    pthread_cond_destroy(&data_cond);
    pthread_mutex_destroy(&datamgr_lock);
    
    pthread_cond_destroy(&db_cond);
    pthread_mutex_destroy(&db_lock);

    pthread_rwlock_destroy(&connmgr_lock);    
    pthread_mutex_destroy(&fifo_mutex);

    // free the threads
    free(data_mgr);
    free(data_sensor_db);
    free(connmgr_working);


    sbuffer_free(&buffer);

#ifdef DEBUG
    printf("CLOSING SENSOR GATEWAY\n");
#endif
    return 0;
}

void main_init_thread(config_thread_t* config_thread){
    config_thread->data_cond = &data_cond;
    config_thread->datamgr_lock = &datamgr_lock;
    config_thread->data_mgr = data_mgr;
    
    config_thread->db_cond = &db_cond;
    config_thread->db_lock = &db_lock;
    config_thread->data_sensor_db = data_sensor_db;

    config_thread->connmgr_lock = &connmgr_lock;
    config_thread->connmgr_working = connmgr_working;

    config_thread->fifo_mutex = &fifo_mutex;
    config_thread->fifo_fd = fifo_fd;
    config_thread->log_mutex = &log_mutex;
}

void* connmgr_th(void* arg){
    int port_number = *((int*) arg);
    config_thread_t connmgr_config_thread;
    main_init_thread(&connmgr_config_thread);

    connmgr_init(&connmgr_config_thread);
    connmgr_listen(port_number, &buffer);

#ifdef DEBUG
    printf(RED_CLR"CLOSING CONNMGR_THR\n"OFF_CLR);
#endif
    return NULL;
}

void* datamgr_th(void* arg){
    FILE* fp_sensor_map = fopen("room_sensor.map", "r");
    config_thread_t datamgr_config_thread;
    main_init_thread(&datamgr_config_thread);

    datamgr_init(&datamgr_config_thread);
    datamgr_parse_sensor_files(fp_sensor_map, &buffer);
    datamgr_free();
    fclose(fp_sensor_map);
    
#ifdef DEBUG
    printf(RED_CLR"CLOSING DATAMGR_THR\n"OFF_CLR);
#endif
    return NULL;
}

void* sensor_db_th(void* arg){
    // initialize the variables for the sensor_db thread
    config_thread_t sensor_db_config_thread;
    main_init_thread(&sensor_db_config_thread);

    sensor_db_init(&sensor_db_config_thread);
    DBCONN* conn = init_connection(DB_FLAG);
    sensor_db_listen(conn, &buffer);
    disconnect(conn);
#ifdef DEBUG
    printf(RED_CLR"CLOSING DB_THR\n"OFF_CLR);
#endif
    return NULL;
}

int print_help(){
    printf("USE THIS PROGRAMME WITH A COMMAND LINE OPTION: \n");
    printf("\t%-15s : TCP SERVER PORT NUMBER\n", "\'SERVER PORT\'");
    return -1;
}