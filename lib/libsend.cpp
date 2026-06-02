#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <cstring>
#include <chrono>
#include <unistd.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

const uint64_t timeout = 100;

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    while(1) {
        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* We will write code here as to not have sync problems with sender_handler */
        
        // if the buffer is full, i can't add the data
        if (cons[conn_id]->senderBuffer.size() <= (size_t) cons[conn_id]->maxSpace) {
            
            // maybe the data is bigger than MAX_DATA_SIZE, so i devide it in multiple packets
            // of size MAX_DATA_SIZE
            int offset = 0;
            while (offset < len) {
                struct Packet p;
                if (len - offset < MAX_DATA_SIZE) {
                    memcpy(p.payload, buffer + offset, len - offset);
                    p.size = len - offset;
                    size += len - offset;
                    cons[conn_id]->senderBuffer.push(p);
                    offset = len;
                } else {
                    memcpy(p.payload, buffer + offset, MAX_DATA_SIZE);
                    p.size = MAX_DATA_SIZE;
                    size += MAX_DATA_SIZE;
                    cons[conn_id]->senderBuffer.push(p);
                    offset += MAX_DATA_SIZE;
                }
            }

            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            return size;

        }
    
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        usleep(1000);
    }

    return size;
}

uint64_t get_current_time_in_ms() {

    auto now = std::chrono::system_clock::now();

    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()
    ).count();

    uint64_t timp_ms = ms;
    
    return timp_ms;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {

        if (cons.size() == 0) {
            // usleep(1000);
            continue;
        }

        int conn_id = -1;
        res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);

        if (res == -14) {
            // if i got timeout then i search the packet i need to resend
            // code -14 means timeout
            for (auto &[id, conn] : cons) {
                pthread_mutex_lock(&conn->con_lock);
                uint64_t current_time = get_current_time_in_ms();

                for (auto&[seq, p] : conn->waitingForACKBuffer) {
                    if (current_time - p.sent_time > timeout) {
                        p.sent_time = current_time;
                        sendto(conn->sockfd, p.payload, p.size, 0, (struct sockaddr *)&conn->servaddr, sizeof(conn->servaddr));
                    }
                }
                pthread_mutex_unlock(&conn->con_lock);
            }
        }
        else if (res > 0 && conn_id >= 0 && cons.find(conn_id) != cons.end()) {
            // for every ACK received we slide forward the window
            struct poli_tcp_ctrl_hdr *hdr = (struct poli_tcp_ctrl_hdr *)buf;
            uint16_t ack = ntohs(hdr->ack_num);
            // if (ack > cons[conn_id]->base)
            //     cons[conn_id]->base = ack;
            
            // then send the packet
            pthread_mutex_lock(&cons[conn_id]->con_lock);

            // erase the confirmed packet from the map
            cons[conn_id]->waitingForACKBuffer.erase(ack);
        
            // update the base (the window slides)
            if (cons[conn_id]->waitingForACKBuffer.empty()) {
                cons[conn_id]->base = cons[conn_id]->next_seq;
            } else {
                cons[conn_id]->base = cons[conn_id]->waitingForACKBuffer.begin()->first;
            }

            pthread_mutex_unlock(&cons[conn_id]->con_lock);
        }

        for (auto &[id, conn] : cons) {
            // sends new packets
            pthread_mutex_lock(&conn->con_lock);

            /* Handle segment received from the receiver. We use this between locks
            as to not have synchronization issues with the send_data calls which are
            on the main thread */
            // send as many packets within max_size_window from queue
            while (conn->next_seq < conn->base + conn->max_window_seq && !conn->senderBuffer.empty()) {
                // create the new packet i want to send
                char buffer[MAX_SEGMENT_SIZE];

                // and in the same time a packet to put it in the waiting for ACK buffer
                struct Packet p;
                
                // create the header
                struct poli_tcp_data_hdr data_hdr;
                data_hdr.protocol_id = 42;
                data_hdr.conn_id = id;
                data_hdr.type = 4;
                data_hdr.seq_num = htons(conn->next_seq);
                int current_seq = conn->next_seq;
                conn->next_seq++;
                data_hdr.len = htons(conn->senderBuffer.front().size);


                // add the payload to the packet
                memcpy(buffer, &data_hdr, sizeof(struct poli_tcp_data_hdr));

                // add the data to the payload;
                memcpy(buffer + sizeof(struct poli_tcp_data_hdr), conn->senderBuffer.front().payload, conn->senderBuffer.front().size);

                int total_size =  sizeof(poli_tcp_data_hdr) + conn->senderBuffer.front().size;
                sendto(conn->sockfd, buffer, total_size, 0, (struct sockaddr *)&conn->servaddr, sizeof(conn->servaddr));

                conn->senderBuffer.pop();

                memcpy(p.payload, buffer, total_size);
                p.size = total_size;
                p.seq_nr = current_seq;
                p.sent_time = get_current_time_in_ms();
                conn->waitingForACKBuffer[current_seq] = p;
            }

            pthread_mutex_unlock(&conn->con_lock);
        }
    }
}


int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    printf("Setup connection\n");

    // struct connection *conn = (struct connection *)malloc(sizeof(struct connection));
    struct connection *conn = new connection();
    int conn_id = fdmax;
    conn->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    
    memset(&conn->servaddr, 0, sizeof(conn->servaddr));
    conn->servaddr.sin_family = AF_INET;
    conn->servaddr.sin_port = port;
    conn->servaddr.sin_addr.s_addr = ip;

    // Set the values for the connection variables
    // SYN = 1; SYN_ACK = 2; ACK = 3;

    // the syn segment
    struct poli_tcp_ctrl_hdr setupSegment;
    setupSegment.protocol_id = 42;
    setupSegment.conn_id = conn_id;
    setupSegment.type = 1;
    setupSegment.ack_num = htons(0);

    // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    if (setsockopt(conn->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } 

    // the syn_ack segment
    int syn_received = 0;
    char buffer[MAX_SEGMENT_SIZE];
    struct sockaddr_in reply;
    socklen_t reply_len = sizeof(reply);

    while(!syn_received) {
        sendto(conn->sockfd, &setupSegment, sizeof(setupSegment), 0, (struct sockaddr *)&conn->servaddr, sizeof(conn->servaddr));

        int rc = recvfrom(conn->sockfd, buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr *)&reply, &reply_len);

        if (rc > 0) {
            struct poli_tcp_ctrl_hdr *recv_hdr = (struct poli_tcp_ctrl_hdr *)buffer;
            if (recv_hdr->type == 2) {
                syn_received = 1; // succes
                
                // we need the port of the server, which is included in the payload as the task states
                // then i update the connection structure
                uint16_t server_port;
                memcpy(&server_port, buffer + sizeof(struct poli_tcp_ctrl_hdr), sizeof(uint16_t));
                conn->servaddr.sin_port = server_port;
            }
        }
    }

    tv.tv_sec = 0;
    tv.tv_usec = 0;
    setsockopt(conn->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // send the 3 segment (ACK)
    struct poli_tcp_ctrl_hdr ack_hdr;
    ack_hdr.protocol_id = POLI_PROTOCOL_ID;
    ack_hdr.conn_id = conn_id;
    ack_hdr.type = 3;
    ack_hdr.ack_num = htons(2);

    sendto(conn->sockfd, &ack_hdr, sizeof(ack_hdr), 0, (struct sockaddr *)&conn->servaddr, sizeof(conn->servaddr));

    pthread_mutex_init(&conn->con_lock, NULL);
    cons.insert({conn_id, conn});

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = conn->sockfd;    
    data_fds[fdmax].events = POLLIN;  

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */  
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 0;    
    spec.it_value.tv_nsec = 5 * 1000000;    
    spec.it_interval.tv_sec = 0;    
    spec.it_interval.tv_nsec = 5 * 1000000;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;

    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}



