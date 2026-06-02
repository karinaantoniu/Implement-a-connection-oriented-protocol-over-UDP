
#pragma once

#include <cstdint>
#include "utils.h"
#include <arpa/inet.h>
#include "protocol.h"
#include <queue>
#include <vector>
#include <map>


/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 512
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))

#define MAX_CONNECTIONS 32

struct Packet {
    char payload[MAX_SEGMENT_SIZE];
    int size;
    int seq_nr;
    uint64_t sent_time;
};

struct ComparePacket {
    bool operator()(const Packet &p1, const Packet &p2) const {
        return p1.seq_nr > p2.seq_nr;
    }
};

/* Protocol control block. Used track different parameters about a connection. 
 * Will need to be extenden to solve the homework with other parameters such as
 * last_ack or status depending on how you implement your protocol. */
struct connection {
    /* common window for both the sender and receiver. */
    /* list window: A window representation */
    int sockfd; /* socket used for this connection */
    int conn_id; /* connection identifier */
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock; /* Used for syncronization with the handler thread and read/send calls.*/
 
    /* TODO. Parameters used only by the sender */
    int max_window_seq = 300; /* Used to store the max number of packets that can be inflight, since we can
                           have many more packets in our window */
    int base = 0; /* base for the window for retransmission*/
    int next_seq = 0; /* the id for the next sequence */
    std::queue<Packet> senderBuffer; /* buffer for the packets waiting to be sent */
    std::map<int, Packet> waitingForACKBuffer; 
    int maxSpace = 5000; // maximum 100 packets in the buffer

    /* TODO. Parameters used only by the client */
    int next_seq_client = 0;
    int max_size = 5000;
    int baseClient = 0;
    std::queue<Packet> receiverBuffer; /* buffer for packets*/
    // it's a priority queue sorted by the seq_num
    std::priority_queue<Packet, std::vector<Packet>, ComparePacket> outOfOrderBuffer; /* buffer for the packets received but in the wrong order*/
};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);
/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);
/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);
/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);
/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);
/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);
