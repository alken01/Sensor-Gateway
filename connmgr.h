/**
 * \author Alken Rrokaj
 */

#ifndef _CONNMGR_H_
#define _CONNMGR_H_


#ifndef TIMEOUT
#define TIMEOUT 5
#endif

// Implement the connection manager in a single process/thread application as described in the assignment. 
// The test script expects that in your connmgr.h file at least the 2 following methods are defined :

// This method holds the core functionality of your connmgr. 
// It starts listening on the given port and when when a sensor node connects it writes the data to a sensor_data_recv file.
// This file must have the same format as the sensor_data file in assignment 6 and 7.
void connmgr_listen(int port_number);

// This method should be called to clean up the connmgr, and to free all used memory. 
// After this no new connections will be accepted
void connmgr_free();

// ***Also this connection manager should be using your dplist to store all the info on the active sensor nodes.***

// Checking if connmgr.c, connmgr.h and config.h are available in the ZIP file (in the root of the file, no sub-folder). Any main.c will be ignored, for this a default one will be used. Also your dplist.c, dplist.h, tcpsock.c and tcpsock.h need to be available in the ZIP file, these should be located in a separate folder called 'lib' inside the ZIP file and will be compiled as shared libraries. This file structure is important for the test to run.
// Running cppcheck on the source code (parameters: --enable=all --suppress=missingIncludeSystem)
// Compiling your connmgr functions and checking if it produces no errors or no warnings (parameters: -Wall -std=c11 -Werror -DTIMEOUT=5)
// Running the connection manager and testing if it only produces one process with a single thread
// Checking the content of the sensor_data_recv file. The data sent by the sensor nodes should be available in sensor_data_recv.

#endif