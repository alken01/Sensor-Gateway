/**
 * \author Alken Rrokaj
 */
#ifndef _CONNMGR_H_
#define _CONNMGR_H_

#include "config.h"
#include "sbuffer.h"

#ifndef TIMEOUT
#define TIMEOUT 5
#endif


/**
 * Initialise the connmgr
 * \param config_thread takes a thread
 */
void connmgr_init(config_thread_t* config_thread);

/**
 * This method holds the core functionality of the connmgr. 
 * It starts listening on the given port and when when a sensor node connects it writes the data to a sensor_data_recv file.
 * This file must have the same format as the sensor_data file in assignment 6 and 7.
 * \param port_number port number to listen too
 * \param buffer to write data too
 */
void connmgr_listen(int port_number, sbuffer_t** buffer);

/**
 * This method should be called to clean up the connmgr, and to free all used memory. 
 * After this no new connections will be accepted
 */
void connmgr_free();

#endif