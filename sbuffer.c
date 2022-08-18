/**
 * \author Alken Rrokaj
 */

#include <stdlib.h>
#include <stdio.h>
#include "sbuffer.h"

#define REDR_THRD 2

 // basic node for the buffer, these nodes are linked together to create the buffer
typedef struct sbuffer_node {
    struct sbuffer_node* next;  // a pointer to the next node
    sensor_data_t data;         // a structure containing the data
    int reader_threads[REDR_THRD];                // max two threads can read at the same time
} sbuffer_node_t;

// a structure to keep track of the buffer
struct sbuffer {
    sbuffer_node_t* head;       // a pointer to the first node in the buffer
    sbuffer_node_t* tail;       // a pointer to the last node in the buffer 
    pthread_rwlock_t* lock; // rwlock -> multiple threads can read at the same time
};

int sbuffer_init(sbuffer_t** buffer){
    *buffer = malloc(sizeof(sbuffer_t));
    if(*buffer == NULL) return SBUFFER_FAILURE;
    (*buffer)->head = NULL;
    (*buffer)->tail = NULL;
    (*buffer)->lock = malloc(sizeof(pthread_rwlock_t));;
    pthread_rwlock_init((*buffer)->lock, NULL);
    return SBUFFER_SUCCESS;
}

int sbuffer_free(sbuffer_t** buffer){
    sbuffer_node_t* dummy;

    // dummy node
    pthread_rwlock_t* dummylock;
    dummylock = (*buffer)->lock;
    pthread_rwlock_wrlock(dummylock);

    if((buffer == NULL) || (*buffer == NULL)) return SBUFFER_FAILURE;
    while((*buffer)->head){
        dummy = (*buffer)->head;
        (*buffer)->head = (*buffer)->head->next;
        free(dummy);
    }
    free(*buffer);
    *buffer = NULL;
    return SBUFFER_SUCCESS;
}

int sbuffer_remove(sbuffer_t* buffer, sensor_data_t* data){
    sbuffer_node_t* dummy;
    if(buffer == NULL) return SBUFFER_FAILURE;
    if(buffer->head == NULL) return SBUFFER_NO_DATA;
    *data = buffer->head->data;
    dummy = buffer->head;

    // if buffer has only one node
    if(buffer->head == buffer->tail) buffer->head = buffer->tail = NULL;
    //if it has mutliple nodes
    else buffer->head = buffer->head->next;
    free(dummy);
    return SBUFFER_SUCCESS;
}

int sbuffer_insert(sbuffer_t* buffer, sensor_data_t* data){
    sbuffer_node_t* dummy;
    if(buffer == NULL) return SBUFFER_FAILURE;
    dummy = malloc(sizeof(sbuffer_node_t));
    if(dummy == NULL) return SBUFFER_FAILURE;

    dummy->data = *data;
    dummy->next = NULL;
    dummy->reader_threads[REDR_THRD] = 0;

    pthread_rwlock_rwlock(buffer->lock);
    // buffer empty (buffer->head should also be NULL)
    if(buffer->tail == NULL) buffer->head = buffer->tail = dummy;
    // buffer not empty
    else{
        buffer->tail->next = dummy;
        buffer->tail = buffer->tail->next;
    }
    pthread_rwlock_unlock(buffer->lock);
    return SBUFFER_SUCCESS;
}
