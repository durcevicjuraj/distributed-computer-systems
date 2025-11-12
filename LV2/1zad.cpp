#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
// Global Lamport clock
int L = 0;
// TODO: Implement Lamport update rules

int on_send()
{
    // increment clock before sending
    // return the Lamport value to attach to message
    return ++L;
}

int on_receive(int L_msg)
{
    // update Lamport clock with max rule
    // return the new Lamport value
    L = std::max(L, L_msg) + 1;
    return L;
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rank == 0)
    {
        // Sender: send 3 messages
        for (int seq = 1; seq <= 3; seq++)
        {
            int L_out = on_send(); // TODO: implement
            int payload[2] = {seq, L_out};
            MPI_Send(payload, 2, MPI_INT, 1, 0, MPI_COMM_WORLD);
            printf("[A] Sent seq=%d L=%d\n", seq, L_out);
        }
        int end[2] = {-1, -1};
        MPI_Send(end, 2, MPI_INT, 1, 0, MPI_COMM_WORLD);
    }
    else if (rank == 1)
    {
        // Receiver: log Lamport updates
        while (1)
        {
            int payload[2];
            MPI_Status status;
            MPI_Recv(payload, 2, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
            if (payload[0] == -1)
                break; // end signal
            int seq = payload[0];
            int L_in = payload[1];
            int L_after = on_receive(L_in); // TODO: implement
            printf("[B] Received seq=%d L_in=%d L_after=%d\n", seq, L_in, L_after);
        }
    }
    MPI_Finalize();
    return 0;
}