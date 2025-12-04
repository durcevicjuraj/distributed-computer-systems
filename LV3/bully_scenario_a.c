#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define ELECTION 1
#define OK 2
#define COORDINATOR 3

int main(int argc, char** argv) {
    int rank, size;
    double start_time, end_time;
    
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    int leader = size - 1;  // Initial leader is highest rank
    int msg_count = 0;
    
    // SCENARIO A: Leader (highest rank) fails
    int failed_process = size - 1;  // The leader fails
    int i_am_failed = (rank == failed_process) ? 1 : 0;
    
    printf("Process %d: Initial leader = %d\n", rank, leader);
    if (i_am_failed) {
        printf("Process %d: [SCENARIO A] I am the leader but I have FAILED!\n", rank);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // START TIMING
    if (rank == 0) {
        start_time = MPI_Wtime();
        printf("\n=== SCENARIO A: Leader Failure ===\n");
        printf("Failed process: %d (original leader)\n", failed_process);
        printf("Expected new leader: %d\n\n", size - 2);
    }
    
    // Step 3: Process 0 starts election after detecting failure
    if (rank == 0) {
        printf("Process %d: Detecting leader failure, starting election\n", rank);
        for (int j = rank + 1; j < size; j++) {
            MPI_Send(NULL, 0, MPI_INT, j, ELECTION, MPI_COMM_WORLD);
            msg_count++;
        }
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Step 4: Handle election messages
    int received_ok = 0;
    int i_started_election = (rank == 0) ? 1 : 0;
    
    for (int round = 0; round < size * 2; round++) {
        MPI_Status status;
        int flag;
        
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
        
        if (flag) {
            if (status.MPI_TAG == ELECTION) {
                MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, ELECTION, 
                         MPI_COMM_WORLD, &status);
                
                // Failed process doesn't respond
                if (!i_am_failed) {
                    printf("Process %d: Received ELECTION, sending OK and forwarding\n", rank);
                    MPI_Send(NULL, 0, MPI_INT, status.MPI_SOURCE, OK, MPI_COMM_WORLD);
                    msg_count++;
                    
                    for (int j = rank + 1; j < size; j++) {
                        MPI_Send(NULL, 0, MPI_INT, j, ELECTION, MPI_COMM_WORLD);
                        msg_count++;
                    }
                    i_started_election = 1;
                } else {
                    printf("Process %d: Received ELECTION but I am FAILED (no response)\n", rank);
                }
            }
            else if (status.MPI_TAG == OK) {
                MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, OK, 
                         MPI_COMM_WORLD, &status);
                received_ok = 1;
            }
        }
        
        usleep(50000);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Step 5: Declare coordinator
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
        printf("Process %d: Acknowledges leader %d\n", rank, leader);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // END TIMING
    if (rank == 0) {
        end_time = MPI_Wtime();
    }
    
    // Count total messages
    int total_messages;
    MPI_Reduce(&msg_count, &total_messages, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    
    if (rank == 0) {
        printf("\n=== SCENARIO A RESULTS ===\n");
        printf("Failed process: %d\n", failed_process);
        printf("New leader elected: %d\n", leader);
        printf("Total messages: %d\n", total_messages);
        printf("Election time: %.6f seconds\n", end_time - start_time);
        
        // Verify correct leader
        if (leader == size - 2) {
            printf("✓ CORRECT: Second-highest process became leader\n");
        } else {
            printf("✗ ERROR: Unexpected leader!\n");
        }
    }
    
    printf("Process %d: Final leader = %d, sent %d messages\n", 
           rank, leader, msg_count);
    
    MPI_Finalize();
    return 0;
}