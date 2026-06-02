#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
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

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    while(1) {
        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* We will write code here as to not have sync problems with recv_handler */
        if (!cons[conn_id]->receiverBuffer.empty()) {
            struct Packet p = cons[conn_id]->receiverBuffer.front();
            if (p.size <= len) {
                cons[conn_id]->receiverBuffer.pop();
                cons[conn_id]->next_seq_client++;
                
                memcpy(buffer, p.payload, p.size);
                size = p.size;

                pthread_mutex_unlock(&cons[conn_id]->con_lock);
                return size;
            }
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

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting receiver handler\n");

    while (1) {

        int conn_id = -1;
        res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);

        if (res <= 0 || res == -14)
            continue;

        if (cons.find(conn_id) == cons.end())
            continue;

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */

        struct poli_tcp_data_hdr *data_hdr = (struct poli_tcp_data_hdr *)segment;
        if (data_hdr->type != 4) {
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            continue; 
        }
        uint16_t seq_num = ntohs(data_hdr->seq_num);

        // also i need to send back an ack message for the packet received
        struct poli_tcp_ctrl_hdr ackMessage;
        ackMessage.protocol_id = 42;
        ackMessage.conn_id = conn_id;
        ackMessage.type = 3;
        ackMessage.ack_num = htons(seq_num);

        sendto(cons[conn_id]->sockfd, &ackMessage, sizeof(struct poli_tcp_ctrl_hdr), 0, (struct sockaddr *)&cons[conn_id]->servaddr, sizeof(cons[conn_id]->servaddr));


        if (seq_num == cons[conn_id]->baseClient) {
            // if the packet is the expected one, then place it in the receiverBuffer for the app
            struct Packet p;
            p.seq_nr = seq_num;
            p.size = res - sizeof(struct poli_tcp_data_hdr);
            memcpy(p.payload, segment + sizeof(struct poli_tcp_data_hdr), p.size);
            cons[conn_id]->receiverBuffer.push(p);
            cons[conn_id]->baseClient++;

            while (!cons[conn_id]->outOfOrderBuffer.empty()) {
                if (cons[conn_id]->outOfOrderBuffer.top().seq_nr == cons[conn_id]->baseClient) {
                    // it's the expected pachet, put it in the app buffer
                    cons[conn_id]->receiverBuffer.push(cons[conn_id]->outOfOrderBuffer.top());
                    cons[conn_id]->outOfOrderBuffer.pop();
                    cons[conn_id]->baseClient++;
                } else if (cons[conn_id]->outOfOrderBuffer.top().seq_nr < cons[conn_id]->baseClient) {
                    // a duplicate caused by a retransmission
                    cons[conn_id]->outOfOrderBuffer.pop();
                } else {
                    // packet is for the future
                    break;
                }
            }
        } else if (seq_num > cons[conn_id]->baseClient && seq_num < cons[conn_id]->baseClient + cons[conn_id]->max_size) {
            struct Packet p;
            memcpy(p.payload, segment + sizeof(struct poli_tcp_data_hdr), res - sizeof(struct poli_tcp_data_hdr));
            p.seq_nr = seq_num;
            p.size = res - sizeof(struct poli_tcp_data_hdr);
            cons[conn_id]->outOfOrderBuffer.push(p);
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }  
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    printf("Wait for connect\n");

    // struct connection *conn = (struct connection *)malloc(sizeof(struct connection));
    struct connection *conn = new connection();
    int conn_id = fdmax;

    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */

    int listen_sockfd = -1;

    // bind only one time
    //if (listen_sockfd == -1) {
        listen_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        
        int opt = 1;
        setsockopt(listen_sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in listen_addr;
        memset(&listen_addr, 0, sizeof(listen_addr));
        listen_addr.sin_family = AF_INET;
        listen_addr.sin_port = port;
        listen_addr.sin_addr.s_addr = ip;

        int ret = bind(listen_sockfd, (struct sockaddr *)&listen_addr, sizeof(listen_addr));
        if (ret < 0) {
            perror("Bind failed\n");
            exit(1);
        }
    //}

    char buffer[MAX_SEGMENT_SIZE];
    struct sockaddr_in recvSYN;
    socklen_t lenSYN = sizeof(recvSYN);

    recvfrom(listen_sockfd, buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr *)&recvSYN, &lenSYN);
    close(listen_sockfd);
    struct poli_tcp_ctrl_hdr *recvSYN_hdr = (struct poli_tcp_ctrl_hdr *) buffer;

    // SYN = 1; SYN_ACK = 2; ACK = 3;
    uint16_t randomNumber;

    if (recvSYN_hdr->type == 1) {
        // generate a new random number of 4 digits for the new port
        randomNumber = htons((rand() % (9999 - 1000 + 1)) + 1000);

        conn->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        if (conn->sockfd < 1) {
            perror("Socket was not created\n");
            exit(1);
        }

        struct sockaddr_in serverAddr;
        memset(&serverAddr, 0, sizeof(serverAddr));
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = randomNumber;
        serverAddr.sin_addr.s_addr = INADDR_ANY;

        // bind the socket
        int ret = bind(conn->sockfd, (struct sockaddr *)&serverAddr, sizeof(serverAddr));
        if (ret < 0) {
            perror("Bind failed\n");
            exit(1);
        }

        // send a new pachet to the client to notify about the port number
        char newPacket[sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t)];
        
        struct poli_tcp_ctrl_hdr newHdr;
        newHdr.protocol_id = 42;
        newHdr.conn_id = conn_id;
        newHdr.type = 2;
        newHdr.ack_num = htons(recvSYN_hdr -> ack_num + 1);

        // add the header to the packet
        memcpy(newPacket, &newHdr, sizeof(struct poli_tcp_ctrl_hdr));
    
        // add the newPort in the payload so the client knows the server's port
        memcpy(newPacket + sizeof(struct poli_tcp_ctrl_hdr), &randomNumber, sizeof(uint16_t));

        /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0) */
        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 100000;
        if (setsockopt(conn->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
            perror("Error");
        }
        
        // wait for the confirmation of ACK packet
        char lastBuffer[MAX_SEGMENT_SIZE];
        struct sockaddr_in lastPacket;
        socklen_t lastLen = sizeof(lastPacket);
        int ack_or_data_received = 0;
        int res = 0;

        while(!ack_or_data_received) {
            // send the confirmation of SYN packet
            sendto(conn->sockfd, newPacket, sizeof(newPacket), 0, (struct sockaddr *)&recvSYN, lenSYN);

            // recv is a blocking call so we wait until we receive a packet
            res = recvfrom(conn->sockfd, lastBuffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr *)&lastPacket, &lastLen);
            
            if (res > 0) {
                ack_or_data_received = 1;
            }
        }

        // deactivate the timeout so the normal data traffic isn't affected
        tv.tv_usec = 0;
        setsockopt(conn->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        
        conn->servaddr = lastPacket;

        struct poli_tcp_ctrl_hdr *check_hdr = (struct poli_tcp_ctrl_hdr *)lastBuffer;
        if (check_hdr->type == 4) {
            struct poli_tcp_data_hdr *data_hdr = (struct poli_tcp_data_hdr *)lastBuffer;
            struct Packet p;
            p.seq_nr = ntohs(data_hdr->seq_num);
            
            p.size = res - sizeof(struct poli_tcp_data_hdr); 
            memcpy(p.payload, lastBuffer + sizeof(struct poli_tcp_data_hdr), p.size);
            
            conn->outOfOrderBuffer.push(p); 
        }
    }

    pthread_mutex_init(&conn->con_lock, NULL);
    cons.insert({conn_id, conn});

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = conn->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
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

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}


