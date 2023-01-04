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


// functions
void* connmgr_th(void* arg);
void* datamgr_th(void* arg);
void* sensor_db_th(void* arg);

int print_help();

// variables
pthread_cond_t data_cond;
pthread_mutex_t datamgr_lock;
int* data_mgr;

pthread_cond_t db_cond;
pthread_mutex_t db_lock;
int* data_sensor_db;

pthread_rwlock_t connmgr_lock;
int* connmgr_working;

pthread_mutex_t fifo_mutex;
int* fifo_fd;

sbuffer_t* buffer;

int main(int argc, char* argv[]){
    printf("start");
    // check if port_number arguments passed
    if(argv[1] == NULL) return print_help();

    //get the port number
    int port_number = atoi(argv[1]);

    // fork into two processes
    // int pid = fork();
    // if(pid == -1) return -1;

    // make a FIFO special file 
    // if(mkfifo("log.FIFO", 0777) == -1){
    //     if(errno != EEXIST){
    //         printf("Could not create fifo file\n");
    //         return -1;
    //     }
    // }

    // initialize fifo file descriptor
    // TODO: chekck if putting this lower in the program works
    // int* fifo_fd;

    // // the child handles the log process
    // if(pid == 0){
    //     //open the log file
    //     FILE* gateway_log = fopen("gateway.log", "w");

    //     //open the log FIFO
    //     *main_thread->fifo_fd = open("log.FIFO", O_RDONLY);

    //     //read from fifo
    //     //TODO: CHANGE THIS
    //     // while(read(fifo_fd, &str_recv, MAX_BUFFER_SIZE) > 0){
    //     //     fprintf(gateway_log, "%s", str_recv);
    //     //     printf("wrote on file %s \n", str_recv);
    //     // }
    //     fclose(gateway_log);
    //     close(*main_thread->fifo_fd);
    //     exit(EXIT_SUCCESS);
    // }

    // fifo_fd = malloc(sizeof(int));
    // *fifo_fd = open("log.FIFO", O_WRONLY);

#ifdef DEBUG
    printf("INITIALIZING SENSOR GATEWAY\n");
#endif

    data_mgr = malloc(sizeof(int));
    data_sensor_db = malloc(sizeof(int));
    connmgr_working = malloc(sizeof(int));
    fifo_fd = malloc(sizeof(int));
#ifdef DEBUG
    printf("INITIALIZED INTS\n");
#endif

    sbuffer_init(&buffer);
#ifdef DEBUG
    printf("INITIALIZED SBUFFER\n");
#endif

    pthread_cond_init(&data_cond, NULL);
    pthread_mutex_init(&datamgr_lock, NULL);
    
    pthread_cond_init(&db_cond, NULL);
    pthread_mutex_init(&db_lock, NULL);

    pthread_rwlock_init(&connmgr_lock, NULL);    
    pthread_mutex_init(&fifo_mutex, NULL);
#ifdef DEBUG
    printf("INITIALIZED PTHREADS\n");
#endif

    // create three threads
    pthread_t threads[3];
    READ_TH_ENUM DMT = DATAMGR_THREAD;
    READ_TH_ENUM DBT = DB_THREAD;
    pthread_create(&threads[0], NULL, &connmgr_th, &port_number);
    pthread_create(&threads[1], NULL, &datamgr_th, &DMT);
    pthread_create(&threads[2], NULL, &sensor_db_th, &DBT);
#ifdef DEBUG
    printf("CREATED THREADS\n");
    printf("SENSOR GATEWAY STARTED\n");
#endif
    // join all the threads
    for(int i = 0; i < 3; i++)
        pthread_join(threads[i], NULL);

#ifdef DEBUG
    printf("JOINED THREADS\n");
#endif
    // free and destroy everything
    pthread_cond_destroy(&data_cond);
    pthread_mutex_destroy(&datamgr_lock);
    
    pthread_cond_destroy(&db_cond);
    pthread_mutex_destroy(&db_lock);

    pthread_rwlock_destroy(&connmgr_lock);    
    pthread_mutex_destroy(&fifo_mutex);

    free(data_mgr);
    free(data_sensor_db);
    free(connmgr_working);
    free(fifo_fd);
    // close(*fifo_fd);
    // free(fifo_fd);
    sbuffer_free(&buffer);

#ifdef DEBUG
    printf("CLOSING SENSOR GATEWAY\n");
#endif
    return 0;
}


void* connmgr_th(void* arg){
    int port_number = *((int*) arg);
    config_thread_t connmgr_config_thread = {
        .data_cond = &data_cond,
        .datamgr_lock = &datamgr_lock,
        .data_mgr = data_mgr,
        
        .db_cond = &db_cond,
        .db_lock = &db_lock,
        .data_sensor_db = data_sensor_db,

        .connmgr_lock = &connmgr_lock,
        .connmgr_working = connmgr_working,

        .fifo_mutex = &fifo_mutex,
        .fifo_fd = fifo_fd
    };

    connmgr_init(&connmgr_config_thread);
    connmgr_listen(port_number, &buffer);
    connmgr_free();
    return (void*) 0;
}

void* datamgr_th(void* arg){
    FILE* fp_sensor_map = fopen("room_sensor.map", "r");
    config_thread_t datamgr_config_thread =  {
         .data_cond = &data_cond,
        .datamgr_lock = &datamgr_lock,
        .data_mgr = data_mgr,
        
        .db_cond = &db_cond,
        .db_lock = &db_lock,
        .data_sensor_db = data_sensor_db,

        .connmgr_lock = &connmgr_lock,
        .connmgr_working = connmgr_working,

        .fifo_mutex = &fifo_mutex,
        .fifo_fd = fifo_fd
    };

    datamgr_init(&datamgr_config_thread);
    datamgr_parse_sensor_files(fp_sensor_map, &buffer);
    datamgr_free();
    fclose(fp_sensor_map);
    return 0;
}

void* sensor_db_th(void* arg){
    // initialize the variables for the sensor_db thread
    config_thread_t sensor_db_config_thread =  {
        .data_cond = &data_cond,
        .datamgr_lock = &datamgr_lock,
        .data_mgr = data_mgr,
        
        .db_cond = &db_cond,
        .db_lock = &db_lock,
        .data_sensor_db = data_sensor_db,

        .connmgr_lock = &connmgr_lock,
        .connmgr_working = connmgr_working,

        .fifo_mutex = &fifo_mutex,
        .fifo_fd = fifo_fd
    };
    
    sensor_db_init(&sensor_db_config_thread);
    DBCONN* conn = init_connection(1);
    sensor_db_listen(conn, &buffer);
    disconnect(conn);
    return 0;
}

int print_help(){
    printf("USE THIS PROGRAMME WITH A COMMAND LINE OPTION: \n");
    printf("\t%-15s : TCP SERVER PORT NUMBER\n", "\'SERVER PORT\'");
    return -1;
}