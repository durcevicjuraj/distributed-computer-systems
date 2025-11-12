#include <mpi.h>
#include <stdio.h>
#include <algorithm>

const int NUM_PROCESSES = 2;
int V[NUM_PROCESSES] = {0, 0};  // Vector clock: [rank0_events, rank1_events]

void on_send(int rank) {
    V[rank]++;  // Increment own counter
}

void on_receive(int rank, int* V_msg) {
    // Take max of each element, then increment own counter
    for (int i = 0; i < NUM_PROCESSES; i++) {
        V[i] = std::max(V[i], V_msg[i]);
    }
    V[rank]++;
}

void print_vector(const char* label, int seq, int* vec) {
    printf("%s seq=%d V=[%d,%d]\n", label, seq, vec[0], vec[1]);
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        for (int seq = 1; seq <= 3; seq++) {
            on_send(rank);
            
            // Send: [seq, V[0], V[1]]
            int payload[3] = { seq, V[0], V[1] };
            MPI_Send(payload, 3, MPI_INT, 1, 0, MPI_COMM_WORLD);
            print_vector("[A] Sent", seq, V);
        }
        int end[3] = { -1, 0, 0 };
        MPI_Send(end, 3, MPI_INT, 1, 0, MPI_COMM_WORLD);
    } 
    else if (rank == 1) {
        while (1) {
            int payload[3];
            MPI_Status status;
            MPI_Recv(payload, 3, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
            if (payload[0] == -1) break;
            
            int seq = payload[0];
            int V_msg[NUM_PROCESSES] = { payload[1], payload[2] };
            
            printf("[B] Received seq=%d V_in=[%d,%d] ", seq, V_msg[0], V_msg[1]);
            on_receive(rank, V_msg);
            printf("V_after=[%d,%d]\n", V[0], V[1]);
        }
    }

    MPI_Finalize();
    return 0;
}