/**
 * \author Alken Rrokaj
 */
#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include "sbuffer.h"
#include "config.h"

#define READ 1
#define UNREAD 2
 // basic node for the buffer, these nodes are linked together to create the buffer
typedef struct sbuffer_node {
    struct sbuffer_node* next;      // a pointer to the next node
    sensor_data_t data;             // a structure containing the data
    int reader_threads[READ_TH];    // max two threads can read at the same time
} sbuffer_node_t;

// a structure to keep track of the buffer
struct sbuffer {
    sbuffer_node_t* head;       // a pointer to the first node in the buffer
    sbuffer_node_t* tail;       // a pointer to the last node in the buffer 
    pthread_rwlock_t* rwlock;   
};

int sbuffer_init(sbuffer_t** buffer){
    *buffer = malloc(sizeof(sbuffer_t));
    if(*buffer == NULL) return SBUFFER_FAILURE;
    (*buffer)->head = NULL;
    (*buffer)->tail = NULL;
    (*buffer)->rwlock = malloc(sizeof(pthread_rwlock_t));
    pthread_rwlock_init((*buffer)->rwlock, NULL);
    return SBUFFER_SUCCESS;
}

int sbuffer_free(sbuffer_t** buffer){
    if((buffer == NULL) || (*buffer == NULL)) return SBUFFER_FAILURE;
    // lock the buffer
    pthread_rwlock_t* rwlock;
    pthread_rwlock_wrlock((*buffer)->rwlock);

    // could also use sbuffer_remove here
    // however an additional function would then be needed to read the head
    while((*buffer)->head){
        sbuffer_node_t* dummy;
        dummy = (*buffer)->head;
        (*buffer)->head = (*buffer)->head->next;
        free(dummy);
    }
    rwlock = (*buffer)->rwlock;
    free(*buffer);
    *buffer = NULL;
    
    // unlock the buffer and destroy the lock
    pthread_rwlock_unlock(rwlock);
    pthread_rwlock_destroy(rwlock);

    return SBUFFER_SUCCESS;
}

// TODO EITHER FIX THIS OR MAKE ANOTHER METHOD
// should not remove it immediately
// make it thread safe
int sbuffer_remove(sbuffer_t* buffer, sensor_data_t* data, READ_TH_ENUM check){
    if(buffer == NULL) return SBUFFER_FAILURE;
    if(buffer->head == NULL) return SBUFFER_NO_DATA;

    // read the data
    pthread_rwlock_rdlock(buffer->rwlock);
    *data = buffer->head->data;
    pthread_rwlock_unlock(buffer->rwlock);

    // here we check if both reader threads (sensor_db and datamgr) have read the file
    switch(check){
        case DB_THREAD: 
            buffer->head->reader_threads[0] = READ;
            break;
        case DATAMGR_THREAD: 
            buffer->head->reader_threads[1] = READ;
            break;
    }

    // if all reader threads have not read it do not go further
    for(int i = 0; i < READ_TH; i++)
        if((buffer->head)->reader_threads[i] == UNREAD)
            return SBUFFER_SUCCESS;
    
    // lock to write/remove
    pthread_rwlock_wrlock(buffer->rwlock);

    // if both read it, remove the file
    sbuffer_node_t* dummy = buffer->head;

    // if buffer has only one node
    if(buffer->head == buffer->tail)
        buffer->head = buffer->tail = NULL;
    else
        buffer->head = buffer->head->next;
    free(dummy);
    // unlock sbuffer
    pthread_rwlock_unlock(buffer->rwlock);
    return SBUFFER_SUCCESS;
}

int sbuffer_insert(sbuffer_t* buffer, sensor_data_t* data){
    if(buffer == NULL) return SBUFFER_FAILURE;

    sbuffer_node_t* dummy = malloc(sizeof(sbuffer_node_t));
    if(dummy == NULL) return SBUFFER_FAILURE;

    dummy->data = *data;
    dummy->next = NULL;
    dummy->reader_threads[0] = UNREAD;
    dummy->reader_threads[1] = UNREAD;

    // lock the buffer
    pthread_rwlock_wrlock(buffer->rwlock);

    // buffer empty (buffer->head should also be NULL)
    if(buffer->tail == NULL)
        buffer->head = buffer->tail = dummy;
    // buffer not empty
    else{
        buffer->tail->next = dummy;
        buffer->tail = dummy;
    }

    // after inserting the data, unlock the buffer
    pthread_rwlock_unlock(buffer->rwlock);
    return SBUFFER_SUCCESS;
}
