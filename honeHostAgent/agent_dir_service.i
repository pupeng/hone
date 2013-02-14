%module agent_dir_service
%{
#include "agent_dir_service.h"
%}

extern void agent_dir_service_cleanup(void);
extern void agent_dir_service_run(void);
extern char* agent_dir_service_recv(void);
