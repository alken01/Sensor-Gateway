/**
 * \author Alken Rrokaj
 */
#include <stdlib.h>
#include <stdio.h>
#include "config.h"
#include "lib/dplist.h"
#include "datamgr.h"

/*
 * definition of error codes
 */
#define DPLIST_NO_ERROR 0
#define DPLIST_MEMORY_ERROR 1 // error due to mem alloc failure
#define DPLIST_INVALID_ERROR 2 //error due to a list operation applied on a NULL list 

// TODO FIX FREE
void element_free(void** element){
    free(((sensor_t*)(*element))->sensor_data);
    ((sensor_t*)(*element))->sensor_data=NULL;
    free(*element);
    *element = NULL;
}
void* element_copy(void* element){
    sensor_t *sensor = (sensor_t *) element;
    sensor_t *copy = malloc(sizeof(sensor_t));
    copy->sensor_id = sensor->sensor_id;
    copy->room_id = sensor->room_id;
    copy->running_avg = sensor->running_avg;
    copy->last_modified = sensor->running_avg;
    return (void*)copy;
}
int element_compare(void* x, void* y){
    sensor_t * sensor_y = (sensor_t *)y;
    sensor_t * sensor_x = (sensor_t *)x;
    if(sensor_y->sensor_id==sensor_x->sensor_id) return 0;
    if(sensor_y->sensor_id<sensor_x->sensor_id) return 1;
    if(sensor_y->sensor_id>sensor_x->sensor_id) return -1;
    else return -1;
}
  
// TODO FIX FREE
void data_free(void** element){
    free(*element);
    *element = NULL;
}
void* data_copy(void* element){
    sensor_data_t *data = (sensor_data_t *) element;
    sensor_data_t *copy = malloc(sizeof(sensor_data_t));
    copy->value = data->value;
    copy->ts = data->ts;
    return (void*)copy;
}
int data_compare(void* x, void* y){
    sensor_data_t * data_x = (sensor_data_t *)y;
    sensor_data_t * data_y = (sensor_data_t *)x;
    if(data_y->value==data_x->value) return 0;
    if(data_y->value<data_x->value) return 1;
    if(data_y->value>data_x->value) return -1;
    else return -1;
}

dplist_t *sensor_list;
//stderr print messages
void datamgr_parse_sensor_files(FILE *fp_sensor_map, FILE *fp_sensor_data){
    if( (fp_sensor_map == NULL) || (fp_sensor_data == NULL) ) return;
    
    sensor_list = dpl_create(element_copy, element_free,element_compare); 
    
    while(!feof(fp_sensor_map)){
        char str[8] = {0};
        if(fgets(str, 8, fp_sensor_map)==NULL) continue; //copy each line in str unless empty (returns NULL)
       
        int j, i=0;
        char s_id[8], r_id[8];
        bool found = false;
        while(str[i]!='\0'){
            if(str[i] == ' ') { r_id[i] = '\0'; found = true; j = i; } //when space is found
            if(!found) r_id[i] = str[i];    //before the space
            else s_id[i-j] = str[i];        //after the space
            i++;
        }

        sensor_t sens = {
            .room_id = atoi(r_id),  .sensor_id = atoi(s_id),
            .running_avg = 0.0,     .last_modified = 0
            // .sensor_data = dpl_create(data_copy, data_free, data_compare)
        };

        sensor_list = dpl_insert_at_index(sensor_list,&sens,0,true);
    }
    // we know have initalized dplist with the room and id
    
    // now we parse sensor_data, and insert each sensor_data_t 
    // to the appropriate sensor_t element in sensor_list
    while(!feof(fp_sensor_data)){

        sensor_id_t buffer_id[1];   //2 bytes
        double buffer_val[1];       //8 bytes
        sensor_ts_t buffer_ts[1];   //8 bytes

        if(fread(buffer_id,sizeof(buffer_id),1,fp_sensor_data)==0) continue; //avoids duplicate in end
        fread(buffer_val,sizeof(buffer_val),1,fp_sensor_data); 
        fread(buffer_ts,sizeof(buffer_ts),1,fp_sensor_data); 

        sensor_data_t dat = {
            .value = buffer_val[0],
            .ts = buffer_ts[0],
        };

        //find element in list where sensor_id = buffer_id and add the element
        for(int i = 0; i <dpl_size(sensor_list);i++){
            sensor_t* sns = dpl_get_element_at_index(sensor_list,i);
            if(sns->sensor_id == buffer_id[0]) {

                if(sns->sensor_data == NULL) sns->sensor_data = dpl_create(data_copy, data_free, data_compare);
                //add the new data point
                sns->sensor_data = dpl_insert_at_index(sns->sensor_data,&dat,0,true);
                //update the timestamp
                sns->last_modified = dat.ts;

                //update the running average
                if(dpl_size(sns->sensor_data)<RUN_AVG_LENGTH) { //if less than RUN_AVG_LENGTH, should be 0
                    sns->running_avg = 0.0;
                    // printf("s_id: %d\t r_id: %hu\t ts: %ld\t    r_avg: %f\t s_data: %d\n",
                    //         sns->sensor_id, sns->room_id, sns->last_modified, sns->running_avg, dpl_size(sns->sensor_data));
                }
                else{
                    sensor_value_t avg = 0; //double 
                    for(int i=0; i<RUN_AVG_LENGTH; i++){
                        avg = avg +((sensor_data_t*)dpl_get_element_at_index(sns->sensor_data,i))->value;
                    }
                    sns->running_avg = (avg/RUN_AVG_LENGTH);
                    if(sns->running_avg>SET_MAX_TEMP || sns->running_avg<SET_MIN_TEMP) fprintf(stderr, "Temp: %f at %u\n",sns->running_avg, sns->sensor_id);
                }
                // printf("s_id: %d\t r_id: %hu\t ts: %ld\t    r_avg: %f\t s_data: %d\n", 
                //         sns->sensor_id, sns->room_id, sns->last_modified, sns->running_avg, dpl_size(sns->sensor_data));                
            }
        }
    }
}


void datamgr_free(){
    for(int i=0; i<dpl_size(sensor_list); i++) 
        dpl_free( &((sensor_t*) dpl_get_element_at_index(sensor_list,i))->sensor_data,true);
    dpl_free(&sensor_list, true);
}


room_id_t datamgr_get_room_id(sensor_id_t sensor_id){
    for(int i=0; i<dpl_size(sensor_list); i++){
        sensor_t *sensor = (sensor_t*) dpl_get_element_at_index(sensor_list,i);
        if(sensor->sensor_id == sensor_id) return sensor->room_id;
    }
    return 0;
}


sensor_value_t datamgr_get_avg(sensor_id_t sensor_id){
    for(int i=0; i<dpl_size(sensor_list); i++){
        sensor_t *sensor = (sensor_t*) dpl_get_element_at_index(sensor_list,i);
        if(sensor->sensor_id == sensor_id) return sensor->running_avg;
    }
    return 0;
}


time_t datamgr_get_last_modified(sensor_id_t sensor_id){
    for(int i=0; i<dpl_size(sensor_list); i++){
        sensor_t *sensor = (sensor_t*) dpl_get_element_at_index(sensor_list,i);
        if(sensor->sensor_id == sensor_id) return sensor->last_modified;
    }
    return 0;
}


int datamgr_get_total_sensors(){
    return dpl_size(sensor_list);
}


#ifdef DEBUG
int main(void){
    FILE * fp_sensor_map = fopen("room_sensor.map","r");
    FILE * fp_sensor_data = fopen("sensor_data_recv","rb"); // r for read, b for binary
    datamgr_parse_sensor_files(fp_sensor_map,fp_sensor_data);
    fclose(fp_sensor_map);
    fclose(fp_sensor_data);
    datamgr_free();
}
#endif

