#ifndef _HONE_WEB10G_CPP_
#define _HONE_WEB10G_CPP_

#include <iostream>
#include <sstream>
#include <map>
#include <vector>
#include <string>
#include <queue>
#include <algorithm>

//#define _RAND_SOCKFD_
//#define _MATCH_ALL_

extern "C" {

#include <estats/estats.h>
#include <estats/sockinfo.h>
#include <pthread.h>

#define NUM_WORKERS 50

class WorkTask
{
public:
    int cid;
    std::string sockfd;
    estats_connection* conn;

    WorkTask()
    {
        cid = -1;
        sockfd.clear();
        conn = NULL;
    }

    WorkTask(int cid, std::string sockfd, estats_connection* conn)
    {
        this->cid = cid;
        this->sockfd = sockfd;
        this->conn = conn;
    }
};

class WorkTaskQueue
{
public:
    pthread_mutex_t lock;
    std::queue<WorkTask> workq;
    
    WorkTaskQueue()
    {
        pthread_mutex_init(&lock, NULL);
    }

    void enqueue(int cid, std::string sockfd, estats_connection* conn)
    {
        WorkTask item = WorkTask(cid, sockfd, conn);
        pthread_mutex_lock(&lock);
        workq.push(item);
        pthread_mutex_unlock(&lock);
    }

    WorkTask dequeue()
    {
        WorkTask item;
        pthread_mutex_lock(&lock);
        if (!workq.empty())
        {
            item = workq.front();
            workq.pop();
        }
        pthread_mutex_unlock(&lock);
        return item;
    }
};

std::map<std::string, std::string> measure(std::map<int, std::string>, std::map<std::string, std::string>, std::vector<int>);

void* worker_run(void* arg);

#define Chk(x) \
    do { \
        err = (x); \
        if (err != NULL) { \
            goto Cleanup; \
        } \
    } while (0)

#define ChkIgn(x) \
    do { \
        err = (x); \
        if (err != NULL) { \
            estats_error_free(&err); \
            goto Cleanup; \
        } \
    } while (0)

#define SWAP(x, y) \
    do { \
        typeof(x) tmp; \
        tmp = x; \
        x = y; \
        y = tmp; \
    } while (0)

#define PRINT_AND_FREE(err) \
    do { \
        estats_error_print(stderr, err); \
        estats_error_free(&err); \
    } while (0)
}

#endif 
