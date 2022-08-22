/**
 * \author Alken Rrokaj
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include "config.h"
#include <sqlite3.h>
#include "sensor_db.h"

 // Stringify the DB_NAME
 // Source: https://stackoverflow.com/a/3419392
#define QUOTE(str) #str
#define EXPAND_AND_QUOTE(str) QUOTE(str)
#define DB_NAME_STRING EXPAND_AND_QUOTE(DB_NAME)
#define TABLE_NAME_STRING EXPAND_AND_QUOTE(TABLE_NAME)

void log_event(char* log_event);
int sql_query(DBCONN* conn, callback_t f, char* sql);

// global variables
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

void sensor_db_init(config_thread_t* config_thread){
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


DBCONN* init_connection(char clear_up_flag){
    DBCONN* db;
    if(sqlite3_open(DB_NAME_STRING, &db) != SQLITE_OK){ //if database can't open
        fprintf(stderr, "CANNOT OPEN DATABASE: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        log_event("UNABLE TO CONNECT TO SQL SERVER.");
#ifdef DEBUG
        printf(BLUE_CLR);
        printf("DB: CANNOT OPEN DATABASE.\n");
        printf("DB: UNABLE TO CONNECT TO SQL SERVER.\n");
        printf(OFF_CLR);
#endif
        return NULL;
    }

    if(clear_up_flag){
        char* sql = sqlite3_mprintf("DROP TABLE IF EXISTS %s", TABLE_NAME_STRING);
        if(sql_query(db, 0, sql) == -1)
            return NULL;
    }

    char* sql = sqlite3_mprintf("CREATE TABLE IF NOT EXISTS `%s` ("
        "`id` INTEGER PRIMARY KEY AUTOINCREMENT,"
        "`sensor_id` INTEGER NULL,"
        "`sensor_value` DECIMAL(4,2) NULL,"
        "`timestamp` TIMESTAMP NULL)", TABLE_NAME_STRING);

    if(sql_query(db, 0, sql) == -1)
        return NULL;

    log_event("ESTABLISHED SQL SERVER CONNECTION.");
    sql = sqlite3_mprintf("NEW TABLE %s CREATED.", DB_NAME_STRING);
    log_event(sql);

#ifdef DEBUG
    printf(BLUE_CLR);
    printf("DB: ESTABLISHED SQL SERVER CONNECTION.\n");
    printf("%s\n", sql);
    printf(OFF_CLR);
#endif
    sqlite3_free(sql);
    return db;
}


void disconnect(DBCONN* conn){
    sqlite3_close(conn);
#ifdef DEBUG
    printf(BLUE_CLR);
    printf("DB: DISCONNECTED FROM DATABASE\n");
    printf(OFF_CLR);
#endif
}

int sensor_db_listen(DBCONN* conn, sbuffer_t** buffer){
    // pthread_rwlock_rdlock(connmgr_active_lock);

    while(*connmgr_working != 0){
        pthread_mutex_lock(db_lock);
        while(data_sensor_db <= 0){
            pthread_cond_wait(db_cond, db_lock);
#ifdef DEBUG
            printf(BLUE_CLR);
            printf("DB: WAITING FOR DATA.\n");
            printf(OFF_CLR);
#endif
        }
        pthread_mutex_unlock(db_lock);

        // copy the data
        sensor_data_t data;
        if(sbuffer_remove(*buffer, &data, DB_THREAD) != SBUFFER_SUCCESS)
            break;

        // insert the sensor in the database
        if(insert_sensor(conn, data.id, data.value, data.ts) != 0)
            return -1;

        pthread_mutex_lock(db_lock);
        data_sensor_db--;
        pthread_mutex_unlock(db_lock);

    }
    return 0;
}

int insert_sensor(DBCONN* conn, sensor_id_t id, sensor_value_t value, sensor_ts_t ts){
    char* sql = sqlite3_mprintf("INSERT INTO `%s` (`sensor_id`, `sensor_value`, `timestamp`)"
        "VALUES ('%d', '%f', '%ld');", TABLE_NAME_STRING, id, value, ts);
    return sql_query(conn, 0, sql);
}

int insert_sensor_from_file(DBCONN* conn, FILE* sensor_data){
    while(!feof(sensor_data)){
        sensor_id_t buffer_id[1];           //2 bytes
        sensor_value_t buffer_val[1];       //8 bytes
        sensor_ts_t buffer_ts[1];           //8 bytes

        if(fread(buffer_id, sizeof(buffer_id), 1, sensor_data) == 0) return 0;
        fread(buffer_val, sizeof(buffer_val), 1, sensor_data);
        fread(buffer_ts, sizeof(buffer_ts), 1, sensor_data);
        if(insert_sensor(conn, buffer_id[0], buffer_val[0], buffer_ts[0]) != 0) return -1;
    }
    return 0;
}

int find_sensor_all(DBCONN* conn, callback_t f){
    char* sql = sqlite3_mprintf("SELECT * FROM %s", TABLE_NAME_STRING);
    return sql_query(conn, f, sql);
}


int find_sensor_by_value(DBCONN* conn, sensor_value_t value, callback_t f){
    char* sql = sqlite3_mprintf("SELECT * FROM `%s` WHERE sensor_value = %f;", TABLE_NAME_STRING, value);
    return sql_query(conn, f, sql);
}


int find_sensor_exceed_value(DBCONN* conn, sensor_value_t value, callback_t f){
    char* sql = sqlite3_mprintf("SELECT * FROM `%s` WHERE sensor_value > %f;", TABLE_NAME_STRING, value);
    return sql_query(conn, f, sql);
}


int find_sensor_by_timestamp(DBCONN* conn, sensor_ts_t ts, callback_t f){
    char* sql = sqlite3_mprintf("SELECT * FROM `%s` WHERE timestamp = %ld;", TABLE_NAME_STRING, ts);
    return sql_query(conn, f, sql);
}

int find_sensor_after_timestamp(DBCONN* conn, sensor_ts_t ts, callback_t f){
    char* sql = sqlite3_mprintf("SELECT * FROM `%s` WHERE timestamp > %ld;", TABLE_NAME_STRING, ts);
    return sql_query(conn, f, sql);
}


// helper methods 
void log_event(char* log_event){
    FILE* fp_log = fopen("gateway.log", "a");
    fprintf(fp_log, "\nSEQ_NR: 1   TIME: %ld\n%s\n", time(NULL), log_event);
    fclose(fp_log);
}

int sql_query(DBCONN* conn, callback_t f, char* sql){
    char* err_msg = 0;
    if(sqlite3_exec(conn, sql, f, 0, &err_msg) != SQLITE_OK){
        fprintf(stderr, "Failed: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_free(sql);
        sqlite3_close(conn);
        log_event("CONNECTION TO SQL SERVER LOST.");
#ifdef DEBUG
        printf(BLUE_CLR);
        printf("DB: CONNECTION TO SQL SERVER LOST.\n");
        printf(OFF_CLR);
#endif
        return -1;
    }
    sqlite3_free(sql);
#ifdef DEBUG
    printf(BLUE_CLR);
    printf("DB: EXECUTED \n%s\n\n", sql);
    printf(OFF_CLR);
#endif
    return 0;
}
