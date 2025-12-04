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
    
    // SCENARIO A: Leader (highest rank) fails
    int failed_process = size - 1;
    int i_am_failed = (rank == failed_process) ? 1 : 0;
    
    printf("Process %d: successor is %d\n", rank, successor);
    
    if (i_am_failed) {
        printf("Process %d: [SCENARIO A] I am the leader but I have FAILED!\n", rank);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    if (rank == 0) {
        start_time = MPI_Wtime();
        printf("\n=== SCENARIO A: Leader Failure ===\n");
        printf("Failed process: %d (original leader)\n", failed_process);
        printf("Expected new leader: %d\n\n", size - 2);
    }
    
    int election_list[MAX_PROCESSES];
    int list_size = 0;
    int initiator = 0;
    
    // Process 0 initiates election
    if (rank == initiator) {
        printf("Process %d: Starting ELECTION\n", rank);
        election_list[0] = rank;
        list_size = 1;
        
        MPI_Send(election_list, list_size, MPI_INT, successor, ELECTION, 
                 MPI_COMM_WORLD);
        msg_count++;
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    int i_am_initiator = (rank == initiator) ? 1 : 0;
    
    // Election phase
    if (!i_am_failed) {
        while (1) {
            MPI_Status status;
            int flag;
            
            MPI_Iprobe(MPI_ANY_SOURCE, ELECTION, MPI_COMM_WORLD, &flag, &status);
            
            if (flag) {
                MPI_Recv(election_list, MAX_PROCESSES, MPI_INT, MPI_ANY_SOURCE, 
                         ELECTION, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_INT, &list_size);
                
                printf("Process %d: Received ELECTION with %d IDs\n", rank, list_size);
                
                if (i_am_initiator && election_list[0] == rank) {
                    // Find highest ID
                    int max_id = election_list[0];
                    for (int i = 1; i < list_size; i++) {
                        if (election_list[i] > max_id) {
                            max_id = election_list[i];
                        }
                    }
                    leader = max_id;
                    
                    printf("*** Process %d: Election complete! Leader is %d ***\n", 
                           rank, leader);
                    
                    MPI_Send(&leader, 1, MPI_INT, successor, COORDINATOR, 
                             MPI_COMM_WORLD);
                    msg_count++;
                    break;
                } else {
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
            }
            usleep(100000);
        }
    } else {
        printf("Process %d: I am FAILED, not participating in election\n", rank);
    }
    
    // Coordinator phase
    MPI_Barrier(MPI_COMM_WORLD);
    
    if (!i_am_failed) {
        while (1) {
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
        }
    } else {
        printf("Process %d: I am FAILED, not receiving coordinator\n", rank);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    if (rank == 0) {
        end_time = MPI_Wtime();
    }
    
    int total_messages;
    MPI_Reduce(&msg_count, &total_messages, 1, MPI_INT, MPI_SUM, 0, 
               MPI_COMM_WORLD);
    
    if (rank == 0) {
        printf("\n=== SCENARIO A RESULTS ===\n");
        printf("Failed process: %d\n", failed_process);
        printf("New leader elected: %d\n", leader);
        printf("Total messages: %d\n", total_messages);
        printf("Election time: %.6f seconds\n", end_time - start_time);
        
        if (leader == size - 2) {
            printf("✓ CORRECT: Second-highest process became leader\n");
        }
    }
    
    if (!i_am_failed) {
        printf("Process %d: Final leader = %d, sent %d messages\n", 
               rank, leader, msg_count);
    }
    
    MPI_Finalize();
    return 0;
}