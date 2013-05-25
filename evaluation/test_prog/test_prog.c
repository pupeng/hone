/* Copyright (c) 2011-2013 Peng Sun. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the COPYRIGHT file.
 *
 *  Peng Sun
 *  Test purpose for some functions
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <string.h>
#include <errno.h>

struct client_param
{
    char* server_ip;
    int server_port;
};

void* one_client_run(void* arg) {
    struct client_param * param;
    int sockfd, n;
    struct sockaddr_in serv_addr;
    unsigned short port;
    char* msg; 
    char * server;

    param = (struct client_param *) arg;
    server = param->server_ip;
    port = param->server_port;
    msg = "hone test\r\n";
    
    bzero((char*)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr(server);
    serv_addr.sin_port = htons(port);

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("socket error\n");
        exit(1);
    }
    printf("pass socket\n");
    if ((connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr))) < 0) {
        printf("connect error\n");
        exit(1);
    }
    printf("pass connect\n");
    
    int i;
    for (i = 0; i < 30000; i++)
    {
        n = send(sockfd, msg, strlen(msg), 0);
        usleep(10000);
    }
    close(sockfd);
}  

int main(int argc, char* argv[])
{
    struct client_param param;
    pthread_t* clients;
    int number;

    if (argc!=4) {
        printf("provide server IP port and number of clients\n");
        printf("usage: ./test_prog serverIP port number\n");
        exit(0);
    }

    param.server_ip = (char *)argv[1];
    param.server_port = atoi(argv[2]);
    number = atoi(argv[3]);

    clients = (pthread_t *)malloc(number*sizeof(pthread_t));
    int i;
    for(i=0;i<number;i++) {
        printf("Number so far: %d\n", i);
        pthread_create(&clients[i], NULL, one_client_run, (void *)(&param));
        usleep(10000);
    }

    for(i=0;i<number;i++) {
        pthread_join(clients[i], NULL);
    }

    printf("serverIP: %s, number: %d\n", param.server_ip, number);
    return 0;
}

