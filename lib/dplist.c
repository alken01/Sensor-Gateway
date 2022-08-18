/**
 * \author Alken Rrokaj
 */

#define _GNU_SOURCE 

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "dplist.h"


/*
 * definition of error codes
 */
#define DPLIST_NO_ERROR 0
#define DPLIST_MEMORY_ERROR 1 // error due to mem alloc failure
#define DPLIST_INVALID_ERROR 2 //error due to a list operation applied on a NULL list 

#ifdef DEBUG
#define DEBUG_PRINTF(...) 									                                        \
        do {											                                            \
            fprintf(stderr,"\nIn %s - function %s at line %d: ", __FILE__, __func__, __LINE__);	    \
            fprintf(stderr,__VA_ARGS__);								                            \
            fflush(stderr);                                                                         \
                } while(0)
#else
#define DEBUG_PRINTF(...) (void)0
#endif


#define DPLIST_ERR_HANDLER(condition, err_code)                         \
    do {                                                                \
            if ((condition)) DEBUG_PRINTF(#condition " failed\n");      \
            assert(!(condition));                                       \
        } while(0)


/*
 * The real definition of struct list / struct node
 */
struct dplist_node {
    dplist_node_t *prev, *next;
    void *element;
};

struct dplist {
    dplist_node_t *head;

    void *(*element_copy)(void *src_element);
    void (*element_free)(void **element);
    int (*element_compare)(void *x, void *y);
};


dplist_t *dpl_create(// callback functions
        void *(*element_copy)(void *src_element),
        void (*element_free)(void **element),
        int (*element_compare)(void *x, void *y)
) {
    dplist_t *list;
    list = malloc(sizeof(struct dplist));
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_MEMORY_ERROR);
    list->head = NULL;
    list->element_copy = element_copy;
    list->element_free = element_free;
    list->element_compare = element_compare;
    return list;
}

// 'free_element' is true: remove the list node containing the element and use the callback to free the memory of the removed element;
// 'free_element' is false: remove the list node containing the element without freeing the memory of the removed element;
void dpl_free(dplist_t **list, bool free_element) {
    int size = dpl_size(*list);
    if(*list == NULL) return;

    if((*list)->head != NULL){
        for(int i = 0; i < size;i++) dpl_remove_at_index(*list,size,free_element);
    }
    //The list itself also needs to be deleted. (free all memory)
    free((*list));
    *list = NULL;
}

dplist_t *dpl_insert_at_index(dplist_t *list, void *element, int index, bool insert_copy) {

    dplist_node_t *ref_at_index, *list_node;
    if (list == NULL) return NULL;

    list_node = malloc(sizeof(dplist_node_t));
    if(insert_copy) list_node->element = list->element_copy(element);
    else list_node->element = element;
    // pointer drawing breakpoint
    if (list->head == NULL) { // covers case 1
        list_node->prev = NULL;
        list_node->next = NULL;
        list->head = list_node;
        // pointer drawing breakpoint
    } else if (index <= 0) { // covers case 2
        list_node->prev = NULL;
        list_node->next = list->head;
        list->head->prev = list_node;
        list->head = list_node;
        // pointer drawing breakpoint
    } else {
        ref_at_index = dpl_get_reference_at_index(list, index);
        assert(ref_at_index != NULL);
        // pointer drawing breakpoint
        if (index < dpl_size(list)) { // covers case 4
            list_node->prev = ref_at_index->prev;
            list_node->next = ref_at_index;
            ref_at_index->prev->next = list_node;
            ref_at_index->prev = list_node;
            // pointer drawing breakpoint
        } else { // covers case 3
            assert(ref_at_index->next == NULL);
            list_node->next = NULL;
            list_node->prev = ref_at_index;
            ref_at_index->next = list_node;
            // pointer drawing breakpoint
        }
    }
    return list;

}

dplist_t *dpl_remove_at_index(dplist_t *list, int index, bool free_element) {
    //If 'index' is 0 or negative, the first list node is removed.
    if(index<0) return dpl_remove_at_index(list, 0, free_element);

    //If the list is empty, return the unmodified list.
    int size = dpl_size(list); 
    if(size == 0 || size == -1) return list;

    //If 'index' is bigger than the number of elements in the list, the last list node is removed.
    if(index>=size) {
        return dpl_remove_at_index(list, size-1, free_element);
    }

    dplist_node_t *current = list->head;
    for(int i = 0; i != index; i++, current = current->next); //current is now pointing at the node to be removed

    if(index==0){ 
        if(current->next!=NULL) { 
            list->head = current->next;
            (list->head)->prev = NULL;
        }
        else {
            list->head = NULL;
        }
        current->next = NULL;
    }
    else if(index==size-1){
        (current->prev)->next = NULL; 
        current->prev = NULL;    
    }
    else{
        (current->prev)->next = current->next;
        (current->next)->prev = current->prev;
        current->next = NULL;
        current->prev = NULL;    
    }
    
    //The list node itself should always be freed.
    if(free_element) list->element_free(&current->element);
    free(current);
    return list;
}

int dpl_size(dplist_t *list) {
    if(list == NULL) return -1;
    if (list->head == NULL) return 0;

    int i;
    dplist_node_t *current = list->head;
    for(i=0; current != NULL; i++, current = current->next);
    return i;

}

void *dpl_get_element_at_index(dplist_t *list, int index) {
    //If 'list' is NULL, 0 is returned.
    //If the list is empty, 0 is returned.
    int size = dpl_size(list); 
    if(size == -1 || size == 0) return 0;

    //If 'index' is 0 or negative, the element of the first list node is returned.
    if(index<0) return dpl_get_element_at_index(list,0);

    //If 'index' is bigger than the number of elements in the list, the element of the last list node is returned.
    if(index>=size) return dpl_get_element_at_index(list, size-1); 

    //return is not returning a copy of the element with index 'index', i.e. 'element_copy()' is not used.
    dplist_node_t *current = list->head;
    for(int i=0; i!=index; i++, current = current->next);
    return current->element;
}

int dpl_get_index_of_element(dplist_t *list, void *element) {
    //If 'list' is NULL, -1 is returned.
    if(list == NULL) return -1;

    //find the element in the list
    dplist_node_t *current = list->head;
    for(int i=0; current != NULL; i++, current = current->next) 
        if(list->element_compare(current->element, element)==0) return i; 
    
    //If 'element' is not found in the list, -1 is returned.
    return -1;
}

dplist_node_t *dpl_get_reference_at_index(dplist_t *list, int index) {
    int count;
    dplist_node_t *dummy;
    if (list->head == NULL) return NULL;
    for (dummy = list->head, count = 0; dummy->next != NULL; dummy = dummy->next, count++) {
        if (count >= index) return dummy;
    }
    return dummy;

}

void *dpl_get_element_at_reference(dplist_t *list, dplist_node_t *reference) {
    // If the list is empty, NULL is returned.
    if(list == NULL) return NULL;

    // If 'list' is is NULL, NULL is returned.
    if(list ->head == NULL) return NULL;
    
    // If 'reference' is NULL, NULL is returned.
    if(reference == NULL) return NULL;

    dplist_node_t *current = list->head;
    while(current != NULL) {
        if(current == reference) return current->element;
        current = current->next;
    }
    // If 'reference' is not an existing reference in the list, NULL is returned.
    return NULL;

}


//*** HERE STARTS THE EXTRA SET OF OPERATORS ***//


dplist_node_t *dpl_get_first_reference(dplist_t *list){
    return dpl_get_reference_at_index(list, 0);
}


dplist_node_t *dpl_get_last_reference(dplist_t *list){
    return dpl_get_reference_at_index(list, dpl_size(list));
}


dplist_node_t *dpl_get_next_reference(dplist_t *list, dplist_node_t *reference){
    return dpl_get_reference_at_index(list, dpl_get_index_of_reference(list, reference)+1);
}

dplist_node_t *dpl_get_previous_reference(dplist_t *list, dplist_node_t *reference){
    return dpl_get_reference_at_index(list, dpl_get_index_of_reference(list, reference)-1);
}


dplist_node_t *dpl_get_reference_of_element(dplist_t *list, void *element){
    return dpl_get_reference_at_index(list, dpl_get_index_of_element(list, element));
}


int dpl_get_index_of_reference(dplist_t *list, dplist_node_t *reference){
    return dpl_get_index_of_element(list, dpl_get_element_at_reference(list, reference));
}


dplist_t *dpl_insert_at_reference(dplist_t *list, void *element, dplist_node_t *reference, bool insert_copy){
    return dpl_insert_at_index(list, element, dpl_get_index_of_reference(list, reference), insert_copy);
}

// TODO
/** Inserts a new list node containing 'element' in the sorted list and returns a pointer to the new list.
 * - The list must be sorted or empty before calling this function.
 * - The sorting is done in ascending order according to a comparison function.
 * - If two members compare as equal, their order in the sorted array is undefined.
 * - If 'list' is is NULL, NULL is returned.
 * \param list a pointer to the list
 * \param element a pointer to an element
 * \param insert_copy if true use element_copy() to make a copy of 'element' and use the copy in the new list node, otherwise the given element pointer is added to the list
 * \return a pointer to the list or NULL
 */
// dplist_t *dpl_insert_sorted(dplist_t *list, void *element, bool insert_copy);


dplist_t *dpl_remove_at_reference(dplist_t *list, dplist_node_t *reference, bool free_element){
    return dpl_remove_at_index(list,  dpl_get_index_of_reference(list, reference), free_element);
}


dplist_t *dpl_remove_element(dplist_t *list, void *element, bool free_element){
    return dpl_remove_at_index(list,  dpl_get_index_of_element(list, element), free_element);
}


