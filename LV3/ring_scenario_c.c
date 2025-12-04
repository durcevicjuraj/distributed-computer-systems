#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define ELECTION 1
#define COORDINATOR 2
#define MAX_PROCESSES 20

int main(int argc, char** argv) {
    int rank, size;
    double start_time, end_time;
    
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    int successor = (rank + 1) % size;
    int leader = size - 1;
    int msg_count = 0;
    
    // SCENARIO C: Initiator (Process 0) fails after starting election
    int failed_process = 0;
    int i_am_failed = 0;  // Initially alive
    
    printf("Process %d: successor is %d\n", rank, successor);
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    if (rank == 1) {  // Process 1 reports since Process 0 will fail
        start_time = MPI_Wtime();
        printf("\n=== SCENARIO C: Initiator Fails After Starting Election ===\n");
        printf("Process 0 will start election then immediately fail\n");
        printf("Expected: Deadlock - initiator can't complete election\n\n");
    }
    
    int election_list[MAX_PROCESSES];
    int list_size = 0;
    int initiator = 0;
    
    // Process 0 initiates election then FAILS
    if (rank == initiator) {
        printf("Process %d: Starting ELECTION\n", rank);
        election_list[0] = rank;
        list_size = 1;
        
        MPI_Send(election_list, list_size, MPI_INT, successor, ELECTION, 
                 MPI_COMM_WORLD);
        msg_count++;
        
        // SCENARIO C: Process 0 fails immediately after starting
        printf("Process %d: Sent ELECTION, now FAILING!\n", rank);
        i_am_failed = 1;
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    int i_am_initiator = (rank == initiator) ? 1 : 0;
    
    // Election phase
    int timeout_counter = 0;
    int max_timeouts = 50;  // 5 second timeout
    
    if (!i_am_failed) {
        while (timeout_counter < max_timeouts) {
            MPI_Status status;
            int flag;
            
            MPI_Iprobe(MPI_ANY_SOURCE, ELECTION, MPI_COMM_WORLD, &flag, &status);
            
            if (flag) {
                MPI_Recv(election_list, MAX_PROCESSES, MPI_INT, MPI_ANY_SOURCE, 
                         ELECTION, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_INT, &list_size);
                
                printf("Process %d: Received ELECTION with %d IDs\n", rank, list_size);
                
                // Check if we're trying to send back to failed initiator
                if (successor == failed_process) {
                    printf("Process %d: My successor is FAILED Process %d - CANNOT FORWARD!\n", 
                           rank, failed_process);
                    printf("Process %d: Election INCOMPLETE - ring is broken\n", rank);
                    break;
                }
                
                // Add my ID and forward
                election_list[list_size] = rank;
                list_size++;
                
                printf("Process %d: Adding my ID, forwarding to %d\n", 
                       rank, successor);
                
                MPI_Send(election_list, list_size, MPI_INT, successor, ELECTION, 
                         MPI_COMM_WORLD);
                msg_count++;
                break;
            }
            
            usleep(100000);
            timeout_counter++;
        }
        
        if (timeout_counter >= max_timeouts) {
            printf("Process %d: TIMEOUT - No ELECTION message (DEADLOCK!)\n", rank);
        }
    } else {
        printf("Process %d: I am FAILED, not participating in election\n", rank);
    }
    
    // Coordinator phase - can't happen because initiator can't send COORDINATOR
    MPI_Barrier(MPI_COMM_WORLD);
    
    if (!i_am_failed) {
        timeout_counter = 0;
        while (timeout_counter < max_timeouts) {
            MPI_Status status;
            int flag;
            
            MPI_Iprobe(MPI_ANY_SOURCE, COORDINATOR, MPI_COMM_WORLD, &flag, &status);
            
            if (flag) {
                int new_leader;
                MPI_Recv(&new_leader, 1, MPI_INT, MPI_ANY_SOURCE, COORDINATOR, 
                         MPI_COMM_WORLD, &status);
                leader = new_leader;
                
                printf("Process %d: Acknowledges leader %d\n", rank, leader);
                
                if (!i_am_initiator) {
                    MPI_Send(&leader, 1, MPI_INT, successor, COORDINATOR, 
                             MPI_COMM_WORLD);
                    msg_count++;
                }
                break;
            }
            usleep(100000);
            timeout_counter++;
        }
        
        if (timeout_counter >= max_timeouts) {
            printf("Process %d: TIMEOUT - No COORDINATOR message (DEADLOCK!)\n", rank);
            printf("Process %d: Initiator failed, cannot announce leader\n", rank);
        }
    } else {
        printf("Process %d: I am FAILED, not receiving coordinator\n", rank);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    if (rank == 1) {
        end_time = MPI_Wtime();
    }
    
    int total_messages;
    MPI_Reduce(&msg_count, &total_messages, 1, MPI_INT, MPI_SUM, 1, 
               MPI_COMM_WORLD);
    
    if (rank == 1) {
        printf("\n=== SCENARIO C RESULTS ===\n");
        printf("Failed process (initiator): %d\n", failed_process);
        printf("Leader status: %s\n", 
               (leader == size - 1) ? "Could be determined" : "UNKNOWN (deadlock)");
        printf("Total messages sent: %d\n", total_messages);
        printf("Election time: %.6f seconds\n", end_time - start_time);
        printf("\n");
        printf("✗ DEADLOCK: Ring algorithm failed\n");
        printf("   Reason: Initiator must receive message back to determine leader\n");
        printf("   Without initiator, no COORDINATOR announcement can be made\n");
    }
    
    if (!i_am_failed) {
        printf("Process %d: Final state - leader = %d, sent %d messages\n", 
               rank, leader, msg_count);
    } else {
        printf("Process %d: FAILED after starting election, sent %d messages\n",
               rank, msg_count);
    }
    
    MPI_Finalize();
    return 0;
}