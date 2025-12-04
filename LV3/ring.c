#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define ELECTION 1
#define COORDINATOR 2
#define MAX_PROCESSES 20

int main(int argc, char** argv) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    // Step 1: Create the logical ring
    int successor = (rank + 1) % size;
    int leader = size - 1;  // Initial leader
    int msg_count = 0;
    
    printf("Process %d: successor is %d\n", rank, successor);
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Step 2: Process 0 initiates election
    int election_list[MAX_PROCESSES];
    int list_size = 0;
    int initiator = 0;
    
    if (rank == initiator) {
        printf("\nProcess %d: Starting ELECTION\n", rank);
        election_list[0] = rank;
        list_size = 1;
        
        printf("Process %d: Sending ELECTION [%d] to process %d\n", 
               rank, rank, successor);
        MPI_Send(election_list, list_size, MPI_INT, successor, ELECTION, 
                 MPI_COMM_WORLD);
        msg_count++;
    }
    
    // Step 3: Circulate the election message
    MPI_Barrier(MPI_COMM_WORLD);
    
    int i_am_initiator = (rank == initiator) ? 1 : 0;
    int i_forwarded = 0;  // Track if I've forwarded the message
    
    // Wait for ELECTION message
    while (1) {
        MPI_Status status;
        int flag;
        
        MPI_Iprobe(MPI_ANY_SOURCE, ELECTION, MPI_COMM_WORLD, &flag, &status);
        
        if (flag) {
            // Receive election message
            MPI_Recv(election_list, MAX_PROCESSES, MPI_INT, MPI_ANY_SOURCE, 
                     ELECTION, MPI_COMM_WORLD, &status);
            
            MPI_Get_count(&status, MPI_INT, &list_size);
            
            printf("Process %d: Received ELECTION with %d IDs: [", rank, list_size);
            for (int i = 0; i < list_size; i++) {
                printf("%d", election_list[i]);
                if (i < list_size - 1) printf(", ");
            }
            printf("]\n");
            
            // Check if message has returned to initiator
            if (i_am_initiator && election_list[0] == rank) {
                // Election complete - find the highest ID
                int max_id = election_list[0];
                for (int i = 1; i < list_size; i++) {
                    if (election_list[i] > max_id) {
                        max_id = election_list[i];
                    }
                }
                
                leader = max_id;
                printf("\n*** Process %d: Election complete! Leader is %d ***\n\n", 
                       rank, leader);
                
                // Step 4: Send COORDINATOR message
                printf("Process %d: Sending COORDINATOR(%d) to process %d\n", 
                       rank, leader, successor);
                MPI_Send(&leader, 1, MPI_INT, successor, COORDINATOR, 
                         MPI_COMM_WORLD);
                msg_count++;
                
                break;  // Exit election loop
            } else {
                // Add my ID to the list and forward
                election_list[list_size] = rank;
                list_size++;
                
                printf("Process %d: Adding my ID, forwarding ELECTION [", rank);
                for (int i = 0; i < list_size; i++) {
                    printf("%d", election_list[i]);
                    if (i < list_size - 1) printf(", ");
                }
                printf("] to process %d\n", successor);
                
                MPI_Send(election_list, list_size, MPI_INT, successor, ELECTION, 
                         MPI_COMM_WORLD);
                msg_count++;
                
                break;  // Exit after forwarding once
            }
        }
        
        usleep(100000);
    }
    
    // Step 5: Wait for and circulate COORDINATOR message
    MPI_Barrier(MPI_COMM_WORLD);
    
    while (1) {
        MPI_Status status;
        int flag;
        
        MPI_Iprobe(MPI_ANY_SOURCE, COORDINATOR, MPI_COMM_WORLD, &flag, &status);
        
        if (flag) {
            int new_leader;
            MPI_Recv(&new_leader, 1, MPI_INT, MPI_ANY_SOURCE, COORDINATOR, 
                     MPI_COMM_WORLD, &status);
            
            printf("Process %d: Received COORDINATOR(%d)\n", rank, new_leader);
            leader = new_leader;
            
            // Forward to successor (unless we're the initiator)
            if (!i_am_initiator) {
                printf("Process %d: Forwarding COORDINATOR(%d) to process %d\n", 
                       rank, leader, successor);
                MPI_Send(&leader, 1, MPI_INT, successor, COORDINATOR, 
                         MPI_COMM_WORLD);
                msg_count++;
            } else {
                printf("Process %d: COORDINATOR message returned to me, stopping\n", 
                       rank);
            }
            
            break;  // Exit coordinator loop
        }
        
        usleep(100000);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Count total messages
    int total_messages;
    MPI_Reduce(&msg_count, &total_messages, 1, MPI_INT, MPI_SUM, 0, 
               MPI_COMM_WORLD);
    
    if (rank == 0) {
        printf("\n=== RING ALGORITHM COMPLETE ===\n");
        printf("Total messages: %d\n", total_messages);
        printf("Expected (2N): %d\n", 2 * size);
    }
    
    printf("Process %d: Final leader = %d, sent %d messages\n", 
           rank, leader, msg_count);
    
    MPI_Finalize();
    return 0;
}