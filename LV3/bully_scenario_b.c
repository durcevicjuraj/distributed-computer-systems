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
    
    // SCENARIO B: Middle-rank process fails DURING election
    int failed_process = size / 2;  // Middle process
    int i_am_failed = 0;  // Initially all alive
    int fail_after_first_message = 0;
    
    printf("Process %d: Initial leader = %d\n", rank, leader);
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // START TIMING
    if (rank == 0) {
        start_time = MPI_Wtime();
        printf("\n=== SCENARIO B: Middle Process Fails During Election ===\n");
        printf("Process %d will fail during election\n", failed_process);
        printf("Expected new leader: %d\n\n", size - 1);
    }
    
    // Step 3: Process 0 starts election
    if (rank == 0) {
        printf("Process %d: Starting election\n", rank);
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
                
                // SCENARIO B: Middle process fails after receiving first ELECTION
                if (rank == failed_process && !fail_after_first_message) {
                    printf("Process %d: Received ELECTION, but now FAILING mid-election!\n", rank);
                    fail_after_first_message = 1;
                    i_am_failed = 1;
                    // Don't send OK or forward - just fail
                    continue;
                }
                
                // Normal processing if not failed
                if (!i_am_failed) {
                    printf("Process %d: Received ELECTION from %d, sending OK and forwarding\n", 
                           rank, status.MPI_SOURCE);
                    MPI_Send(NULL, 0, MPI_INT, status.MPI_SOURCE, OK, MPI_COMM_WORLD);
                    msg_count++;
                    
                    for (int j = rank + 1; j < size; j++) {
                        MPI_Send(NULL, 0, MPI_INT, j, ELECTION, MPI_COMM_WORLD);
                        msg_count++;
                    }
                    i_started_election = 1;
                }
            }
            else if (status.MPI_TAG == OK) {
                MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, OK, 
                         MPI_COMM_WORLD, &status);
                printf("Process %d: Received OK from %d\n", rank, status.MPI_SOURCE);
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
    
    // Step 6: Accept the new leader (failed process won't receive)
    if (!i_am_failed) {
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
    } else {
        printf("Process %d: I am FAILED, not participating in acknowledgment\n", rank);
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
        printf("\n=== SCENARIO B RESULTS ===\n");
        printf("Failed process (mid-election): %d\n", failed_process);
        printf("New leader elected: %d\n", leader);
        printf("Total messages: %d\n", total_messages);
        printf("Election time: %.6f seconds\n", end_time - start_time);
        
        // Verify correct leader
        if (leader == size - 1) {
            printf("✓ CORRECT: Highest process became leader despite mid-election failure\n");
        } else {
            printf("✗ WARNING: Leader is %d (expected %d)\n", leader, size - 1);
        }
    }
    
    if (!i_am_failed) {
        printf("Process %d: Final leader = %d, sent %d messages\n", 
               rank, leader, msg_count);
    } else {
        printf("Process %d: FAILED during election, sent %d messages before failing\n",
               rank, msg_count);
    }
    
    MPI_Finalize();
    return 0;
}