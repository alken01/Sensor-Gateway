/**
 * \author Luc Vandeurzen
 */

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include "config.h"
#include "lib/tcpsock.h"

 // conditional compilation option to control the number of measurements this sensor node wil generate
#if (LOOPS > 1)
#define UPDATE(i) (i--)
#else
#define LOOPS 1
#define UPDATE(i) (void)0 //create infinite loop
#endif

// conditional compilation option to log all sensor data to a text file
#ifdef LOG_SENSOR_DATA

#define LOG_FILE	"sensor_log"

#define LOG_OPEN()                                      \
	FILE *fp_log;                                       \
	do                                                  \
	{                                                   \
		fp_log = fopen(LOG_FILE, "w");                  \
		if ((fp_log) == NULL)                           \
		{                                               \
			printf("%s\n", "couldn't create log file"); \
			exit(EXIT_FAILURE);                         \
		}                                               \
	} while (0)

#define LOG_PRINTF(sensor_id, temperature, timestamp)                                               \
	do                                                                                              \
	{                                                                                               \
		fprintf(fp_log, "%" PRIu16 " %g %ld\n", (sensor_id), (temperature), (long int)(timestamp)); \
		fflush(fp_log);                                                                             \
	} while (0)

#define LOG_CLOSE() fclose(fp_log);

#else
#define LOG_OPEN(...) (void)0
#define LOG_PRINTF(...) (void)0
#define LOG_CLOSE(...) (void)0
#endif

#define INITIAL_TEMPERATURE 20
#define TEMP_DEV 5    // max deviation from previous temperature in 0.1 celsius


void print_help(void);
int checkIP(char server_ip[]);

/**
 * For starting the sensor node 4 command line arguments are needed. These should be given in the order below
 * and can then be used through the argv[] variable
 *
 * argv[1] = sensor ID
 * argv[2] = sleep time
 * argv[3] = server IP
 * argv[4] = server port
 */

int main(int argc, char* argv[]){
	sensor_data_t data;
	int server_port;
	char server_ip[] = "000.000.000.000";
	tcpsock_t* client;
	int i, bytes, sleep_time;

	LOG_OPEN();

	if(argc != 5){
		print_help();
		exit(EXIT_SUCCESS);
	} else{
		data.id = atoi(argv[1]);
		sleep_time = atoi(argv[2]);
		strncpy(server_ip, argv[3], strlen(server_ip));
		server_port = atoi(argv[4]);
	}

	//verifying IP
	if(checkIP(server_ip) == -1) printf("ERROR: INVALID IP: %s\n", server_ip), exit(EXIT_FAILURE);

	srand48(time(NULL));

	// open TCP connection to the server; server is listening to SERVER_IP and PORT
	if(tcp_active_open(&client, server_port, server_ip) != TCP_NO_ERROR) printf("CANNOT OPEN TCP CONNECTION\n"), exit(EXIT_FAILURE);

	data.value = INITIAL_TEMPERATURE;
	i = LOOPS;
	while(i){
		data.value = data.value + TEMP_DEV * ((drand48() - 0.5) / 10);
		time(&data.ts);
		// send data to server in this order (!!): <sensor_id><temperature><timestamp>
		// remark: don't send as a struct!
		bytes = sizeof(data.id);
		if(tcp_send(client, (void*) &data.id, &bytes) != TCP_NO_ERROR) exit(EXIT_FAILURE);
		bytes = sizeof(data.value);
		if(tcp_send(client, (void*) &data.value, &bytes) != TCP_NO_ERROR) exit(EXIT_FAILURE);
		bytes = sizeof(data.ts);
		if(tcp_send(client, (void*) &data.ts, &bytes) != TCP_NO_ERROR) exit(EXIT_FAILURE);
		LOG_PRINTF(data.id, data.value, data.ts);
		sleep(sleep_time);
		UPDATE(i);
	}


	if(tcp_close(&client) != TCP_NO_ERROR) exit(EXIT_FAILURE);

	LOG_CLOSE();

	exit(EXIT_SUCCESS);
}

/**
 * Helper method to print a message on how to use this application
 */
void print_help(void){
	printf("Use this program with 4 command line options: \n");
	printf("\t%-15s : a unique sensor node ID\n", "\'ID\'");
	printf("\t%-15s : node sleep time (in sec) between two measurements\n", "\'sleep time\'");
	printf("\t%-15s : TCP server IP address\n", "\'server IP\'");
	printf("\t%-15s : TCP server port number\n", "\'server port\'");
}

// helper method to check if IP is valid, if it is not return -1
int checkIP(char server_ip[]){
	if(strlen(server_ip) < 7 || strlen(server_ip) > 15) return -1;

	char tail[16];
	tail[0] = 0;

	unsigned int d[4];
	int c = sscanf(server_ip, "%3u.%3u.%3u.%3u%s", &d[0], &d[1], &d[2], &d[3], tail);

	if(c != 4 || tail[0]) return -1;

	for(int i = 0; i < 4; i++)
		if(d[i] > 255) return -1;
	return 0;
}
