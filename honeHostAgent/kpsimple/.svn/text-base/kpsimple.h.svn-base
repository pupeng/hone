/*
 * kpsimple.h
 *
 *  Created on: Dec 10, 2011
 *      Author: zhihongx
 */

#ifndef KPSIMPLE_H_
#define KPSIMPLE_H_

#include <linux/sched.h>
#include <linux/socket.h>

#define NLTYPE_CONNECT 0
#define NLTYPE_SEND 1
#define NLTYPE_CLOSE 2
#define NLCONNECT_LEN sizeof(struct nlconnect)
#define NLSEND_LEN sizeof(struct nlsend)
#define NLCLOSE_LEN sizeof(struct nlclose)

struct nlconnect {
	__u32 nlmsg_len;
	__u16 nlmsg_type;
	__u16 nlmsg_flags;
	int sk;
	int saddr, daddr;
	unsigned short sport, dport;
	char app[16];
};

struct nlsend {
	__u32 nlmsg_len;
	__u16 nlmsg_type;
	__u16 nlmsg_flags;
	int sk;
	size_t size;
    int saddr, daddr;
    unsigned short sport, dport;
    char app[16];
};

struct nlclose {
	__u32 nlmsg_len;
	__u16 nlmsg_type;
	__u16 nlmsg_flags;
	int sk;
    char app[16];
};

#endif /* KPSIMPLE_H_ */
