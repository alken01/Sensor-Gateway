/**
 * \author Alken Rrokaj
 */

#ifndef _CONFIG_H_
#define _CONFIG_H_

#include <stdint.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include <poll.h>
#include "lib/dplist.h"
#include "lib/tcpsock.h"
#include <stdbool.h>

#ifdef DEBUG
#define OFF_CLR     "\033[0m"
#define BLACK_CLR   "\033[0;30m"
#define RED_CLR     "\033[0;31m"
#define GREEN_CLR   "\033[0;32m"
#define YELLOW_CLR  "\033[0;33m"
#define BLUE_CLR    "\033[0;34m"
#define PURPLE_CLR  "\033[0;35m"
#define CYAN_CLR    "\033[0;36m"
#define WHITE_CLR   "\033[0;37m"
#endif


#ifndef RUN_AVG_LENGTH
#define RUN_AVG_LENGTH 5
#endif
 /*
  * Use ERROR_HANDLER() for handling memory allocation problems, invalid sensor IDs, non-existing files, etc.
  */
#define ERROR_HANDLER(condition, ...)    \
do {                       \
    if (condition) {                              \
    printf("\nError: in %s - function %s at line %d: %s\n", __FILE__, __func__, __LINE__, __VA_ARGS__); \
    exit(EXIT_FAILURE);                         \
    }                                             \
} while(0)


typedef uint16_t sensor_id_t;
typedef uint16_t room_id_t;
typedef double sensor_value_t;
typedef time_t sensor_ts_t;
typedef struct pollfd pollfd_t;

// structure to hold sensors
typedef struct{
    sensor_id_t sensor_id;
    room_id_t room_id;
    sensor_value_t running_avg;
    sensor_ts_t last_modified;
    sensor_value_t data_buffer[RUN_AVG_LENGTH]; //circular buffer to hold the variables
    bool take_avg;
    uint16_t buffer_position;
}sensor_t; //element t in the dplist

// structure to hold sensor_data
typedef struct {
    sensor_id_t id;         /** < sensor id */
    sensor_value_t value;   /** < sensor value */
    sensor_ts_t ts;         /** < sensor timestamp */
} sensor_data_t;

// structure for multi-threading
typedef struct {
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
} config_thread_t;

#endif /* _CONFIG_H_ */
