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
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // START TIMING
    start_time = MPI_Wtime();
    
    int election_list[MAX_PROCESSES];
    int list_size = 0;
    int initiator = 0;
    
    if (rank == initiator) {
        election_list[0] = rank;
        list_size = 1;
        MPI_Send(election_list, list_size, MPI_INT, successor, ELECTION, 
                 MPI_COMM_WORLD);
        msg_count++;
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    int i_am_initiator = (rank == initiator) ? 1 : 0;
    
    // Election phase
    while (1) {
        MPI_Status status;
        int flag;
        
        MPI_Iprobe(MPI_ANY_SOURCE, ELECTION, MPI_COMM_WORLD, &flag, &status);
        
        if (flag) {
            MPI_Recv(election_list, MAX_PROCESSES, MPI_INT, MPI_ANY_SOURCE, 
                     ELECTION, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_INT, &list_size);
            
            if (i_am_initiator && election_list[0] == rank) {
                // Find highest ID
                int max_id = election_list[0];
                for (int i = 1; i < list_size; i++) {
                    if (election_list[i] > max_id) {
                        max_id = election_list[i];
                    }
                }
                leader = max_id;
                
                MPI_Send(&leader, 1, MPI_INT, successor, COORDINATOR, 
                         MPI_COMM_WORLD);
                msg_count++;
                break;
            } else {
                election_list[list_size] = rank;
                list_size++;
                
                MPI_Send(election_list, list_size, MPI_INT, successor, ELECTION, 
                         MPI_COMM_WORLD);
                msg_count++;
                break;
            }
        }
        usleep(100000);
    }
    
    // Coordinator phase
    MPI_Barrier(MPI_COMM_WORLD);
    
    while (1) {
        MPI_Status status;
        int flag;
        
        MPI_Iprobe(MPI_ANY_SOURCE, COORDINATOR, MPI_COMM_WORLD, &flag, &status);
        
        if (flag) {
            int new_leader;
            MPI_Recv(&new_leader, 1, MPI_INT, MPI_ANY_SOURCE, COORDINATOR, 
                     MPI_COMM_WORLD, &status);
            leader = new_leader;
            
            if (!i_am_initiator) {
                MPI_Send(&leader, 1, MPI_INT, successor, COORDINATOR, 
                         MPI_COMM_WORLD);
                msg_count++;
            }
            break;
        }
        usleep(100000);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // END TIMING
    end_time = MPI_Wtime();
    
    // Collect total messages
    int total_messages;
    MPI_Reduce(&msg_count, &total_messages, 1, MPI_INT, MPI_SUM, 0, 
               MPI_COMM_WORLD);
    
    // Collect timing data (average across all processes)
    double avg_time;
    double elapsed = end_time - start_time;
    MPI_Reduce(&elapsed, &avg_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    
    if (rank == 0) {
        avg_time /= size;
        printf("%d,%d,%.6f\n", size, total_messages, avg_time);
    }
    
    MPI_Finalize();
    return 0;
}