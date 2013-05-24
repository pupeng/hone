/*
 * kpsimple.c
 * HONE kernel probe
 *
 *  Created on: Dec 10, 2011
 *  Modified on: Apr 17, 2013
 *      Author: zhihongx, pengsun
 */

#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/kprobes.h>
#include <linux/kallsyms.h>
#include <linux/netlink.h>
#include <linux/skbuff.h>
#include <linux/string.h>
#include <linux/in.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/byteorder/generic.h>
#include <asm-generic/current.h>
#include <net/sock.h>
#include <net/net_namespace.h>
#include <net/inet_sock.h>
#include "kpsimple.h"

#define MY_NIPQUAD(addr) \
    ((unsigned char *)&addr)[0], \
    ((unsigned char *)&addr)[1], \
    ((unsigned char *)&addr)[2], \
    ((unsigned char *)&addr)[3]

static struct sock *nlsk = NULL; /* netlink socket */
static __u32 pid = 0; /* pid of user-space dir service */

struct my_in_addr {
	unsigned long s_addr; // load with inet_pton()
};

struct my_sockaddr_in {
	short sin_family; // e.g. AF_INET, AF_INET6
	unsigned short sin_port; // e.g. htons(3490)
	struct my_in_addr sin_addr; // see struct in_addr, below
	char sin_zero[8]; // zero this if you want to
};

void nl_recv(struct sk_buff *skb) {
	struct nlmsghdr *nlh;

	if (skb == NULL) {
		printk("skb is NULL \n");
		return;
	}

	nlh = (struct nlmsghdr *) skb->data;
	pid = nlh->nlmsg_pid;
	printk("Kernel Module: Received pid from %u\n", pid);
}

int nl_init(void) {
	nlsk = netlink_kernel_create(&init_net, NETLINK_HONE, 0, nl_recv, NULL,
			THIS_MODULE);
	if (!nlsk) {
		printk("Kernel Module: Cannot create netlink socket\n");
		return -1;
	}
	return 0;
}

int my_connect(struct sock *sk) {
	struct nlconnect *nlh;
	struct sk_buff *skb;
	struct inet_sock *socket;
	int saddr, daddr;
	unsigned short sport, dport;

	if (pid == 0)
		jprobe_return();

	socket = (struct inet_sock*) sk;
	saddr = socket->inet_saddr;
	sport = ntohs(socket->inet_sport);
	daddr = socket->inet_daddr;
	dport = ntohs(socket->inet_dport);

	printk(
			"connect to socket %u from %s src = %d.%d.%d.%d:%u dst = %d.%d.%d.%d:%u\n",
			(unsigned int) sk, current->comm, MY_NIPQUAD(saddr),sport,
			MY_NIPQUAD(daddr), dport) ;

	skb = alloc_skb(NLCONNECT_LEN, GFP_KERNEL);

	if (!skb)
		jprobe_return();

	nlh = (struct nlconnect*) skb_put(skb, NLCONNECT_LEN);
	nlh->nlmsg_type = NLTYPE_CONNECT;
	nlh->nlmsg_len = NLCONNECT_LEN;
	nlh->nlmsg_flags = 1;
	nlh->saddr = saddr;
	nlh->daddr = daddr;
	nlh->sport = sport;
	nlh->dport = dport;
	nlh->sk = (int) sk;
	strcpy(nlh->app, current->comm);
	NETLINK_CB(skb).pid = 0;
	netlink_unicast(nlsk, skb, pid, MSG_DONTWAIT);

	jprobe_return();

	/* shouldn't reach here */
	return 0;
}

ssize_t my_send(struct kiocb *iocb, struct sock *sk, struct msghdr *msg,
		size_t size) {
	struct nlsend *nlh;
	struct sk_buff *skb;
	struct inet_sock *socket;
	int saddr, daddr;
	unsigned short sport, dport;

	if (pid == 0)
		jprobe_return();

	skb = alloc_skb(NLSEND_LEN, GFP_KERNEL);

	if (!skb)
		jprobe_return();

	printk("sent %u bytes from socket %u (%s)\n", size, (unsigned int) sk,
			current->comm);

	socket = (struct inet_sock*) sk;
	saddr = socket->inet_saddr;
	sport = ntohs(socket->inet_sport);
	daddr = socket->inet_daddr;
	dport = ntohs(socket->inet_dport);

	nlh = (struct nlsend*) skb_put(skb, NLSEND_LEN);
	nlh->nlmsg_type = NLTYPE_SEND;
	nlh->nlmsg_len = NLSEND_LEN;
	nlh->nlmsg_flags = 1;
	nlh->sk = (int) sk;
	nlh->size = size;
    nlh->saddr = saddr;
    nlh->sport = sport;
    nlh->daddr = daddr;
    nlh->dport = dport;
	strcpy(nlh->app, current->comm);
	NETLINK_CB(skb).pid = 0;
	netlink_unicast(nlsk, skb, pid, MSG_DONTWAIT);

	jprobe_return();

	/* shouldn't reach here */
	return 0;
}

int my_close(struct sock *sk, long timeout) {
	struct nlclose *nlh;
	struct sk_buff *skb;

	if (pid == 0)
		jprobe_return();

	printk("socket %u (%s) closed\n", (unsigned int) sk, current->comm);

	skb = alloc_skb(NLCLOSE_LEN, GFP_KERNEL);

	if (!skb)
		jprobe_return();

	nlh = (struct nlclose*) skb_put(skb, NLCLOSE_LEN);
	nlh->nlmsg_type = NLTYPE_CLOSE;
	nlh->nlmsg_len = NLCLOSE_LEN;
	nlh->nlmsg_flags = 1;
	nlh->sk = (int) sk;
	strcpy(nlh->app, current->comm);
	NETLINK_CB(skb).pid = 0;
	netlink_unicast(nlsk, skb, pid, MSG_DONTWAIT);

	jprobe_return();

	/* shouldn't reach here */
	return 0;
}

static struct jprobe
		connect_jprobe = { .entry = (kprobe_opcode_t *) my_connect };

static struct jprobe send_jprobe = { .entry = (kprobe_opcode_t *) my_send };

static struct jprobe close_jprobe = { .entry = (kprobe_opcode_t *) my_close };

int init_jprobe(struct jprobe* probe, const char* fn) {
	int ret;
	probe->kp.addr = (kprobe_opcode_t *) kallsyms_lookup_name(fn);
	if (!probe->kp.addr) {
		printk("Couldn't find %s to plant jprobe\n", fn);
		return -1;
	}

	ret = register_jprobe(probe);
	if (ret < 0) {
		printk("register_jprobe failed, returned %d\n", ret);
		return -1;
	}

	printk("Planted jprobe at %s (%p), handler addr %p\n", fn, probe->kp.addr,
			probe->entry);

	return 0;
}

int init_module(void) {
	if (nl_init() < 0) {
		printk("nl_init failed\n");
		return -1;
	}
	if (init_jprobe(&connect_jprobe, "tcp_connect") < 0) {
		printk("init connect jprobe failed\n");
		return -1;
	}
	if (init_jprobe(&send_jprobe, "tcp_sendmsg") < 0) {
		printk("init send jprobe failed\n");
		return -1;
	}

	if (init_jprobe(&close_jprobe, "tcp_close") < 0) {
		printk("init close jprobe failed\n");
		return -1;
	}

	return 0;
}

void cleanup_module(void) {
	unregister_jprobe(&connect_jprobe);
	unregister_jprobe(&send_jprobe);
	unregister_jprobe(&close_jprobe);
	if (nlsk) {
		sock_release(nlsk->sk_socket);
	}
}

MODULE_LICENSE("GPL");
