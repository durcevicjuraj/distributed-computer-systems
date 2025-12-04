#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define ELECTION 1
#define OK 2
#define COORDINATOR 3

int main(int argc, char** argv) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    int leader = size - 1;  // Step 1: highest rank is initial coordinator
    int msg_count = 0;
    
    printf("Process %d: Initial leader = %d\n", rank, leader);
    
    // Step 2: Simulate failure - the leader will not participate
    int i_am_failed = (rank == leader) ? 1 : 0;
    if (i_am_failed) {
        printf("Process %d: I am the coordinator but I have failed!\n", rank);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Step 3: Start election (process 0 initiates as per PDF)
    if (rank == 0) {
        printf("Process %d: Starting election\n", rank);
        for (int j = rank + 1; j < size; j++) {
            MPI_Send(NULL, 0, MPI_INT, j, ELECTION, MPI_COMM_WORLD);
            msg_count++;
        }
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Step 4: Handle election messages (multiple rounds as suggested in hints)
    int received_ok = 0;
    int i_started_election = (rank == 0) ? 1 : 0;
    
    for (int round = 0; round < size * 2; round++) {
        MPI_Status status;
        int flag;
        
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
        
        if (flag) {
            if (status.MPI_TAG == ELECTION) {
                // Received ELECTION message
                MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, ELECTION, 
                         MPI_COMM_WORLD, &status);
                printf("Process %d: Received ELECTION from %d\n", rank, status.MPI_SOURCE);
                
                // Only respond if not failed
                if (!i_am_failed) {
                    // Send OK
                    MPI_Send(NULL, 0, MPI_INT, status.MPI_SOURCE, OK, MPI_COMM_WORLD);
                    msg_count++;
                    printf("Process %d: Sent OK to %d\n", rank, status.MPI_SOURCE);
                    
                    // Forward election to higher ranks
                    for (int j = rank + 1; j < size; j++) {
                        MPI_Send(NULL, 0, MPI_INT, j, ELECTION, MPI_COMM_WORLD);
                        msg_count++;
                    }
                    i_started_election = 1;
                }
            }
            else if (status.MPI_TAG == OK) {
                // Received OK message
                MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, OK, 
                         MPI_COMM_WORLD, &status);
                printf("Process %d: Received OK from %d\n", rank, status.MPI_SOURCE);
                received_ok = 1;
            }
        }
        
        usleep(50000);  // Small delay
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Step 5: Declare coordinator
    // The highest ID that started election and got no OK becomes leader
    if (i_started_election && !received_ok && !i_am_failed) {
        printf("\n*** Process %d: I am the new coordinator! ***\n\n", rank);
        leader = rank;
        
        for (int j = 0; j < size; j++) {
            if (j != rank) {
                MPI_Send(&rank, 1, MPI_INT, j, COORDINATOR, MPI_COMM_WORLD);
                msg_count++;
            }
        }
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    sleep(1);
    
    // Step 6: Accept the new leader
    MPI_Status status;
    int flag;
    MPI_Iprobe(MPI_ANY_SOURCE, COORDINATOR, MPI_COMM_WORLD, &flag, &status);
    
    if (flag) {
        int new_leader;
        MPI_Recv(&new_leader, 1, MPI_INT, status.MPI_SOURCE, COORDINATOR, 
                 MPI_COMM_WORLD, &status);
        leader = new_leader;
        printf("Process %d acknowledges leader %d\n", rank, leader);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Count total messages
    int total_messages;
    MPI_Reduce(&msg_count, &total_messages, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    
    if (rank == 0) {
        printf("\nTotal messages sent: %d\n", total_messages);
    }
    
    printf("Process %d: Final leader = %d, sent %d messages\n", rank, leader, msg_count);
    
    MPI_Finalize();
    return 0;
}