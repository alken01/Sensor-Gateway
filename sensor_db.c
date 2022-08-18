/**
 * \author Alken Rrokaj
 */

#include <stdio.h>
#include <stdlib.h>
#include "config.h"
#include <sqlite3.h>
#include "sensor_db.h"


// Source: 3.4 Stringizing 
// https://gcc.gnu.org/onlinedocs/gcc-11.3.0/cpp/Stringizing.html
#define xstr(s) str(s)
#define str(s) #s

DBCONN* init_connection(char clear_up_flag){
    char* err_msg = 0;

    DBCONN* db;
    if(sqlite3_open(str(DB_NAME.db), &db) != SQLITE_OK){ //if database can't open
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        log_event("Unable to connect to SQL server.");
        return NULL;
    }

    char* sql;
    if(clear_up_flag){
        sql = "DROP TABLE IF EXISTS TABLE_NAME";
        if(sqlite3_exec(db, sql, 0, 0, &err_msg) != SQLITE_OK){
            fprintf(stderr, "Cannot execute drop: %s\n", err_msg);
            sqlite3_free(err_msg);
            sqlite3_free(sql);
            sqlite3_close(db);
            log_event("Unable to connect to SQL server.");
            return NULL;
        }
    }

    sql = "CREATE TABLE `TABLE_NAME` ("
        "`id` INTEGER PRIMARY KEY AUTOINCREMENT,"
        "`sensor_id` INTEGER NULL,"
        "`sensor_value` DECIMAL(4,2) NULL,"
        "`timestamp` TIMESTAMP NULL)";


    if(sqlite3_exec(db, sql, 0, 0, &err_msg) != SQLITE_OK){
        fprintf(stderr, "Cannot execute create: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_free(sql);
        sqlite3_close(db);
        log_event("Unable to connect to SQL server.");
        return NULL;
    }

    log_event("Connection to SQL server established.");
    log_event("New table %s created.",str(DB_NAME.db));
    sqlite3_free(sql);
    return db;
}


void disconnect(DBCONN* conn){
    sqlite3_close(conn);
}


int insert_sensor(DBCONN* conn, sensor_id_t id, sensor_value_t value, sensor_ts_t ts){
    char* sql = sqlite3_mprintf("INSERT INTO `TABLE_NAME` (`sensor_id`, `sensor_value`, `timestamp`)"
        "VALUES ('%d', '%f', '%ld');", id, value, ts);
    char* err_msg;
    if(sqlite3_exec(conn, sql, 0, 0, &err_msg) != SQLITE_OK){
        fprintf(stderr, "Cannot insert sensor: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(conn);
        sqlite3_free(sql);
        log_event("Connection to SQL server lost.");
        return -1;
    }
    sqlite3_free(sql);
    return 0;
}


int insert_sensor_from_file(DBCONN* conn, FILE* sensor_data){
    while(!feof(sensor_data)){
        sensor_id_t buffer_id[1];           //2 bytes
        sensor_value_t buffer_val[1];       //8 bytes
        sensor_ts_t buffer_ts[1];           //8 bytes

        if(fread(buffer_id, sizeof(buffer_id), 1, sensor_data) == 0) continue; //avoids duplicate in end
        fread(buffer_val, sizeof(buffer_val), 1, sensor_data);
        fread(buffer_ts, sizeof(buffer_ts), 1, sensor_data);
        insert_sensor(conn, buffer_id[0], buffer_val[0], buffer_ts[0]);
    }

    return 0;
}


int find_sensor_all(DBCONN* conn, callback_t f){
    char* sql = "SELECT * FROM TABLE_NAME";
    char* err_msg;
    if(sqlite3_exec(conn, sql, f, 0, &err_msg) != SQLITE_OK){
        fprintf(stderr, "Failed to find all: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(conn);
        sqlite3_free(sql);
        log_event("Connection to SQL server lost.");
        return -1;
    }
    sqlite3_free(sql);
    return 0;
}


int find_sensor_by_value(DBCONN* conn, sensor_value_t value, callback_t f){
    char* sql = sqlite3_mprintf("SELECT * FROM `TABLE_NAME` WHERE sensor_value = %d;", value);
    printf("%s\n", sql);
    char* err_msg;
    if(sqlite3_exec(conn, sql, f, 0, &err_msg) != SQLITE_OK){
        fprintf(stderr, "Failed to find all: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_free(sql);
        sqlite3_close(conn);
        log_event("Connection to SQL server lost.");
        return -1;
    }
    sqlite3_free(sql);
    return 0;
}


int find_sensor_exceed_value(DBCONN* conn, sensor_value_t value, callback_t f){
    char* sql = sqlite3_mprintf("SELECT * FROM `TABLE_NAME` WHERE sensor_value > %d;", value);
    printf("%s\n", sql);
    char* err_msg;
    if(sqlite3_exec(conn, sql, f, 0, &err_msg) != SQLITE_OK){
        fprintf(stderr, "Failed to find all: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_free(sql);
        sqlite3_close(conn);
        log_event("Connection to SQL server lost.");
        return -1;
    }
    sqlite3_free(sql);
    return 0;
}


int find_sensor_by_timestamp(DBCONN* conn, sensor_ts_t ts, callback_t f){
    char* sql = sqlite3_mprintf("SELECT * FROM `TABLE_NAME` WHERE timestamp = %d;", ts);
    printf("%s\n", sql);
    char* err_msg;
    if(sqlite3_exec(conn, sql, f, 0, &err_msg) != SQLITE_OK){
        fprintf(stderr, "Failed to find all: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(conn);
        sqlite3_free(sql);
        log_event("Connection to SQL server lost.");
        return -1;
    }
    sqlite3_free(sql);
    return 0;
}


int find_sensor_after_timestamp(DBCONN* conn, sensor_ts_t ts, callback_t f){
    char* sql = sqlite3_mprintf("SELECT * FROM `TABLE_NAME` WHERE timestamp > %d;", ts);
    printf("%s\n", sql);
    char* err_msg;
    if(sqlite3_exec(conn, sql, f, 0, &err_msg) != SQLITE_OK){
        fprintf(stderr, "Failed to find all: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_free(sql);
        sqlite3_close(conn);
        log_event("Connection to SQL server lost.");
        return -1;
    }
    sqlite3_free(sql);
    return 0;
}

void log_event(char* log_event){
	FILE* fp_log = fopen("gateway.log", "a");
	fprintf(fp_log, "\nSEQ_NR: 1   TIME: %ld\n%s %d\n", time(NULL), log_event);
	fclose(fp_log);
}
