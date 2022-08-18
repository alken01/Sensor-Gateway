/**
 * \author Alken Rrokaj
 */

#ifndef _CONFIG_H_
#define _CONFIG_H_

#include <stdint.h>
#include <time.h>
#include "lib/dplist.h"
#include "lib/tcpsock.h"
#include <poll.h>

typedef uint16_t sensor_id_t;
typedef uint16_t room_id_t;
typedef double sensor_value_t;
typedef time_t sensor_ts_t;         
typedef struct pollfd pollfd_t; 

// structure to hold sensors
typedef struct {
    sensor_id_t sensor_id;
    room_id_t room_id;
    sensor_value_t running_avg;
    sensor_ts_t last_modified;
    dplist_t* sensor_data;
} sensor_t;

// structure to hold sensor_data
typedef struct {
    sensor_id_t id;         /** < sensor id */
    sensor_value_t value;   /** < sensor value */
    sensor_ts_t ts;         /** < sensor timestamp */
} sensor_data_t;

#endif /* _CONFIG_H_ */
