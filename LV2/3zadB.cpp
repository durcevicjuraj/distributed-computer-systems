#include <mpi.h>
#include <stdio.h>

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        // TIME SERVER: wait for requests and respond with current time
        printf("[Server] Time server ready\n");
        
        for (int i = 0; i < 3; i++) {
            int request;
            MPI_Status status;
            MPI_Recv(&request, 1, MPI_INT, 1, 0, MPI_COMM_WORLD, &status);
            
            double server_time = MPI_Wtime();
            MPI_Send(&server_time, 1, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD);
            printf("[Server] Sent time: %.6f\n", server_time);
        }
    } 
    else if (rank == 1) {
        // CLIENT: request time and adjust local clock
        double local_clock = MPI_Wtime() - 5.0;  // Simulate 5 second offset
        
        for (int i = 0; i < 3; i++) {
            printf("\n[Client] Request %d:\n", i+1);
            printf("[Client] Local time before: %.6f\n", local_clock);
            
            // Send request and measure round-trip time
            double t0 = MPI_Wtime();
            int request = 1;
            MPI_Send(&request, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            
            double server_time;
            MPI_Recv(&server_time, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            double t1 = MPI_Wtime();
            
            // Cristian's Algorithm: adjust for network delay
            double round_trip = t1 - t0;
            double estimated_server_time = server_time + (round_trip / 2.0);
            
            // Calculate offset error
            double offset = estimated_server_time - local_clock;
            
            // Adjust local clock
            local_clock = estimated_server_time;
            
            printf("[Client] Server time: %.6f\n", server_time);
            printf("[Client] Round-trip delay: %.6f seconds\n", round_trip);
            printf("[Client] Estimated current server time: %.6f\n", estimated_server_time);
            printf("[Client] Offset error: %.6f seconds\n", offset);
            printf("[Client] Local time after adjustment: %.6f\n", local_clock);
        }
    }

    MPI_Finalize();
    return 0;
}