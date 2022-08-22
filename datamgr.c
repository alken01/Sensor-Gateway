/**
 * \author Alken Rrokaj
 */
#include <stdlib.h>
#include <stdio.h>
#include "config.h"
#include "sbuffer.h"
#include "lib/dplist.h"
#include "datamgr.h"

 /*
  * definition of error codes
  */
#define DPLIST_NO_ERROR 0
#define DPLIST_MEMORY_ERROR 1 // error due to mem alloc failure
#define DPLIST_INVALID_ERROR 2 //error due to a list operation applied on a NULL list 

  // used in log_event
typedef enum {
    COLD, HOT, ERROR
} DATAMGR_CASE;

static void log_event(sensor_id_t id, sensor_value_t temp, DATAMGR_CASE check);

// methods for dpl_create
void sensor_free(void** element);
void* sensor_copy(void* element);
int sensor_compare(void* x, void* y);

void data_free(void** element);
void* data_copy(void* element);
int data_compare(void* x, void* y);

// global variables
static dplist_t* sensor_list;

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

void datamgr_init(config_thread_t* config_thread){
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
}

void datamgr_parse_sensor_files(FILE* fp_sensor_map, sbuffer_t** sbuffer){
    //maybe give out an error TODO??
    if(fp_sensor_map == NULL) return;

    sensor_list = dpl_create(sensor_copy, sensor_free, sensor_compare);

    // read fist the sesnor_map
    while(!feof(fp_sensor_map)){
        // copy each line in str unless empty (returns NULL)
        char str[8];
        if(fgets(str, 8, fp_sensor_map) == NULL) continue;

        int j, i = 0;
        char sensor_id[8];
        char room_id[8];
        bool found = false;
        // parse each line
        while(str[i] != '\0'){
            //when space is found
            if(str[i] == ' '){ room_id[i] = '\0'; found = true; j = i; }
            //before the space we add the chars to room_id
            if(!found) room_id[i] = str[i];
            //after the space we add the chars to sensor_id
            else sensor_id[i - j] = str[i];
            i++;
        }

        // copy the parsed data in a sensor
        sensor_t sens = {
            .room_id = atoi(room_id),  .sensor_id = atoi(sensor_id),
            .running_avg = 0.0,     .last_modified = 0
        };

        sensor_list = dpl_insert_at_index(sensor_list, &sens, 0, true);
#ifdef DEBUG
        printf(GREEN_CLR);
        printf("DATAMGR: NEW SENSOR ID: %d  ROOM ID: %d\n", sens.sensor_id, sens.room_id);
        printf(OFF_CLR);
#endif
    }
    // we know have initalized dplist with the room and id

    // now we parse sensor_data, and insert each sensor_data_t to the appropriate sensor_t element in sensor_list
    while(*connmgr_working != 0){
        pthread_mutex_lock(datamgr_lock);
        while(data_mgr <= 0){
#ifdef DEBUG
            printf(GREEN_CLR);
            printf("DATAMGR: WAITING FOR DATA.\n");
            printf(OFF_CLR);
#endif
            pthread_cond_wait(data_cond, datamgr_lock);
        }
        // create a sensor_data variable and copy the data at the head of the buffer in the variable
        sensor_data_t new_data;
        sbuffer_remove(*sbuffer, &new_data, DATAMGR_THREAD);

        // adding empty ts
        if(new_data.ts == 0) continue;
        // find the sensor in the list where sensor_id = buffer_id
        // add it to the element
        for(int i = 0; i < dpl_size(sensor_list);i++){
            sensor_t* sns = dpl_get_element_at_index(sensor_list, i);
            if(sns->sensor_id == new_data.id){
                // create a dplist if first data value
                if(sns->sensor_data == NULL)
                    sns->sensor_data = dpl_create(data_copy, data_free, data_compare);
                //add the new data point
                sns->sensor_data = dpl_insert_at_index(sns->sensor_data, &new_data, 0, true);
                //update the timestamp
                sns->last_modified = new_data.ts;

                //update the running average
                //if less than RUN_AVG_LENGTH, should be 0
                if(dpl_size(sns->sensor_data) < RUN_AVG_LENGTH)
                    sns->running_avg = 0.0;
                else{
                    sensor_value_t avg = 0.0;
                    for(int i = 0; i < RUN_AVG_LENGTH; i++)
                        avg = avg + ((sensor_data_t*) dpl_get_element_at_index(sns->sensor_data, i))->value;
                    sns->running_avg = (avg / RUN_AVG_LENGTH);

                    // log in case it is an extreme
                    if(sns->running_avg > SET_MAX_TEMP) log_event(sns->sensor_id, sns->running_avg, HOT);
                    if(sns->running_avg < SET_MIN_TEMP) log_event(sns->sensor_id, sns->running_avg, COLD);
#ifdef DEBUG
                    printf(GREEN_CLR);
                    printf("data_connmgr: %d\n", *data_mgr);
                    printf("CONNMGR: ID: %u ROOM: %d  AVG: %f   NR: %d   TIME: %ld\n",
                        sns->sensor_id, sns->room_id, sns->running_avg, dpl_size(sns->sensor_data), sns->last_modified);
                    printf(OFF_CLR);
#endif
                }
            }
        }
        data_mgr--;
        pthread_mutex_unlock(datamgr_lock);
    }
}


void datamgr_free(){
    for(int i = 0; i < dpl_size(sensor_list); i++)
        dpl_free(&((sensor_t*) dpl_get_element_at_index(sensor_list, i))->sensor_data, true);
    dpl_free(&sensor_list, true);
}


room_id_t datamgr_get_room_id(sensor_id_t sensor_id){
    for(int i = 0; i < dpl_size(sensor_list); i++){
        sensor_t* sensor = (sensor_t*) dpl_get_element_at_index(sensor_list, i);
        if(sensor->sensor_id == sensor_id) return sensor->room_id;
    }
    return 0;
}


sensor_value_t datamgr_get_avg(sensor_id_t sensor_id){
    for(int i = 0; i < dpl_size(sensor_list); i++){
        sensor_t* sensor = (sensor_t*) dpl_get_element_at_index(sensor_list, i);
        if(sensor->sensor_id == sensor_id) return sensor->running_avg;
    }
    return 0;
}


time_t datamgr_get_last_modified(sensor_id_t sensor_id){
    for(int i = 0; i < dpl_size(sensor_list); i++){
        sensor_t* sensor = (sensor_t*) dpl_get_element_at_index(sensor_list, i);
        if(sensor->sensor_id == sensor_id) return sensor->last_modified;
    }
    return 0;
}


int datamgr_get_total_sensors(){
    return dpl_size(sensor_list);
}


// helper methods
void sensor_free(void** element){
    free(((sensor_t*) (*element))->sensor_data);
    ((sensor_t*) (*element))->sensor_data = NULL;
    free(*element);
    *element = NULL;
}

void* sensor_copy(void* element){
    sensor_t* sensor = (sensor_t*) element;
    sensor_t* copy = malloc(sizeof(sensor_t));
    copy->sensor_id = sensor->sensor_id;
    copy->room_id = sensor->room_id;
    copy->running_avg = sensor->running_avg;
    copy->last_modified = sensor->running_avg;
    return (void*) copy;
}

int sensor_compare(void* x, void* y){
    sensor_t* sensor_y = (sensor_t*) y;
    sensor_t* sensor_x = (sensor_t*) x;
    if(sensor_y->sensor_id == sensor_x->sensor_id) return 0;
    if(sensor_y->sensor_id < sensor_x->sensor_id) return 1;
    if(sensor_y->sensor_id > sensor_x->sensor_id) return -1;
    else return -1;
}

void data_free(void** element){
    free(*element);
    *element = NULL;
}

void* data_copy(void* element){
    sensor_data_t* data = (sensor_data_t*) element;
    sensor_data_t* copy = malloc(sizeof(sensor_data_t));
    copy->value = data->value;
    copy->ts = data->ts;
    return (void*) copy;
}

int data_compare(void* x, void* y){
    sensor_data_t* data_x = (sensor_data_t*) y;
    sensor_data_t* data_y = (sensor_data_t*) x;
    if(data_y->value == data_x->value) return 0;
    if(data_y->value < data_x->value) return 1;
    if(data_y->value > data_x->value) return -1;
    else return -1;
}


// log event
static void log_event(sensor_id_t id, sensor_value_t temp, DATAMGR_CASE check){
    FILE* fp_log = fopen("gateway.log", "a");
    switch(check){
    case COLD:
        fprintf(fp_log, "\nSEQ_NR: xxx  TIME: %ld\nSENSOR ID: %d TOO HOT! (AVG_TEMP = %f)\n", time(NULL), id, temp);
    case HOT:
        fprintf(fp_log, "\nSEQ_NR: xxx  TIME: %ld\nSENSOR ID: %d TOO HOT! (AVG_TEMP = %f)\n", time(NULL), id, temp);
    case ERROR:
        fprintf(fp_log, "\nSEQ_NR: xxx  TIME: %ld\nSENSOR DATA FROM INVALID SENSOR ID: %d\n", time(NULL), id);
    }
    fclose(fp_log);
}
