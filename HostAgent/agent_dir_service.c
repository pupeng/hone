/*
 * agent_dir_service.c
 * Peng Sun
 * C implementation of kernel communication with kernel module
 */

#include "agent_dir_service.h"

static int nlfd = -1;

void agent_dir_service_cleanup(void) {
	if (nlfd >= 0)
		close(nlfd);
}

void agent_dir_service_run(void) {
	struct sockaddr_nl nl_src_addr, nl_dst_addr;
	struct msghdr nl_msg;
	struct nlmsghdr *nlh = NULL;
	struct iovec iov;

	nlfd = socket(AF_NETLINK, SOCK_RAW, NETLINK_HONE);

	memset(&nl_src_addr, 0, sizeof(nl_src_addr));
	nl_src_addr.nl_family = AF_NETLINK;
	nl_src_addr.nl_pid = getpid();
	bind(nlfd, (struct sockaddr*) &nl_src_addr, sizeof(nl_src_addr));

	memset(&nl_dst_addr, 0, sizeof(nl_dst_addr));
	nl_dst_addr.nl_family = AF_NETLINK;

	nlh = (struct nlmsghdr *) malloc(NL_SIZE);
	memset(nlh, 0, NL_SIZE);
	nlh->nlmsg_len = NL_SIZE;
	nlh->nlmsg_pid = getpid();
	nlh->nlmsg_flags = 1;

	iov.iov_base = (void *) nlh;
	iov.iov_len = nlh->nlmsg_len;

	memset(&nl_msg, 0, sizeof(nl_msg));
	nl_msg.msg_name = (void *) &nl_dst_addr;
	nl_msg.msg_namelen = sizeof(struct sockaddr_nl);
	nl_msg.msg_iov = &iov;
	nl_msg.msg_iovlen = 1;
	sendmsg(nlfd, &nl_msg, 0);
}


char* agent_dir_service_recv(void) {
    ssize_t nbytes;
    struct sockaddr_nl nl_dst_addr;
    struct nlmsghdr *nlh = NULL;
    struct msghdr nl_msg;
	struct iovec iov;
    char *message;

    message = (char *)malloc(MESSAGE_SIZE);
    memset(message, 0, MESSAGE_SIZE);

    memset(&nl_dst_addr, 0, sizeof(nl_dst_addr));
    nl_dst_addr.nl_family = AF_NETLINK;

	nlh = (struct nlmsghdr *) malloc(NL_SIZE);
	memset(nlh, 0, NL_SIZE);
	nlh->nlmsg_len = NL_SIZE;
	nlh->nlmsg_pid = getpid();
	nlh->nlmsg_flags = 1;

	iov.iov_base = (void *) nlh;
	iov.iov_len = nlh->nlmsg_len;

	memset(&nl_msg, 0, sizeof(nl_msg));
	nl_msg.msg_name = (void *) &nl_dst_addr;
	nl_msg.msg_namelen = sizeof(struct sockaddr_nl);
	nl_msg.msg_iov = &iov;
	nl_msg.msg_iovlen = 1;

    memset(nlh, 0, NL_SIZE);
    nbytes = recvmsg(nlfd, &nl_msg, 0);

    if (nlh->nlmsg_type == NLTYPE_SEND) {
        struct nlsend *msg = (struct nlsend *)nlh;
        sprintf(message, "send#%s#%u#%d.%d.%d.%d#%u#%d.%d.%d.%d#%u#%u\n", 
                msg->app, (unsigned int) (msg->sk), IPQUAD(msg->saddr), 
                msg->sport, IPQUAD(msg->daddr), msg->dport, (unsigned int) (msg->size));
    }
    else if (nlh->nlmsg_type == NLTYPE_CONNECT) {
        struct nlconnect *msg = (struct nlconnect *)nlh;
        sprintf(message, "connect#%s#%u#%d.%d.%d.%d#%u#%d.%d.%d.%d#%u\n", 
               msg->app, (unsigned int)(msg->sk), IPQUAD(msg->saddr),
               msg->sport, IPQUAD(msg->daddr), msg->dport);
    }
    else if (nlh->nlmsg_type == NLTYPE_CLOSE) {
        struct nlclose *msg = (struct nlclose *) nlh;
		sprintf(message, "close#%s#%u\n", msg->app, (unsigned int) (msg->sk));
    }
    return message;
}



