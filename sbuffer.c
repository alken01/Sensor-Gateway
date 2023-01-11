/**
 * \author Alken Rrokaj
 */
#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include "sbuffer.h"
#include "config.h"

#define READ true
#define UNREAD false
 // basic node for the buffer, these nodes are linked together to create the buffer
typedef struct sbuffer_node {
    struct sbuffer_node* next;      // a pointer to the next node
    sensor_data_t data;             // a structure containing the data
    bool reader_threads[THREAD_NR]; // check which threads have read the data
} sbuffer_node_t;

// a structure to keep track of the buffer
struct sbuffer {
    sbuffer_node_t* head;       // a pointer to the first node in the buffer
    sbuffer_node_t* tail;       // a pointer to the last node in the buffer 
    pthread_rwlock_t* rwlock;   
};

// helper methods
int sbuffer_read(sbuffer_node_t* buffer_node, sensor_data_t* data,READ_TH_ENUM thread);

#ifdef DEBUG
void sbuffer_print_tree(sbuffer_node_t *node);
#endif

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

int sbuffer_remove(sbuffer_t* buffer, sensor_data_t* data, READ_TH_ENUM thread){
    if(buffer == NULL) return SBUFFER_FAILURE;
    if(buffer->head == NULL) return SBUFFER_NO_DATA;

    pthread_rwlock_rdlock(buffer->rwlock);
    sbuffer_read(buffer->head, data, thread);
    pthread_rwlock_unlock(buffer->rwlock);

    
    // if all reader threads have not read it do not go further
    for(int i = 0; i < THREAD_NR; i++)
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

#ifdef DEBUG
    printf(YELLOW_CLR "REMOVED FROM BUFFER\n" OFF_CLR);
#endif

    return SBUFFER_SUCCESS;
}

int sbuffer_read(sbuffer_node_t* buffer_node, sensor_data_t* data,READ_TH_ENUM thread){
    // recursive function to read the buffer based on the thread type
    if(buffer_node == NULL) return SBUFFER_NO_DATA;
    if(buffer_node->reader_threads[thread] == READ) 
        return sbuffer_read(buffer_node->next, data, thread);
    
    *data = buffer_node->data;
    buffer_node->reader_threads[thread] = READ;
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

#ifdef DEBUG
    printf(YELLOW_CLR "INSERTED IN BUFFER\n" OFF_CLR);
    printf(YELLOW_CLR "CURRENT BUFFER:\n" OFF_CLR);
    sbuffer_print_tree(buffer->head);
#endif
    return SBUFFER_SUCCESS;
}

#ifdef DEBUG
void sbuffer_print_tree(sbuffer_node_t* node){
    if(node == NULL) return;

    time_t timestamp = node->data.ts;  // Unix timestamp
    char time_str[9];               // buffer for the human-readable time string
    struct tm *timeinfo = localtime(&timestamp);
    strftime(time_str, sizeof(time_str), "%T", timeinfo);

    printf(YELLOW_CLR);
    printf("+-------------------------+\n");
    printf("    node: %p\t\n", node);

    printf("|+-----------------------+|\n");
    printf("||  id:        %u        ||\n", node->data.id);
    printf("||  value:     %.2f     ||\n", node->data.value);
    printf("||  timestamp: %s  ||\n", time_str);
    printf("||  reader threads: ");
    for(int i = 0; i < THREAD_NR; i++){
        printf("%d ", node->reader_threads[i]);
    }
    printf(" ||\n");
    printf("|+-----------------------+|\n");
    printf("    next:      %p\t\n", node->next);
    printf("+-------------------------+\n");
    if(node->next != NULL){
        printf("|                         |\n");
        printf("v                         v\n");
    }
    printf(OFF_CLR);

    sbuffer_print_tree(node->next);
}
#endif
