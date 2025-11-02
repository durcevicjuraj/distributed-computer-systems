#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

int main(int argc, char** argv) { 

    MPI_Init(&argc, &argv); 

    FILE *fp;
    int rank, size, name_len;
    char processor_name[MPI_MAX_PROCESSOR_NAME]; 

    MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
    MPI_Comm_size(MPI_COMM_WORLD, &size); 
    MPI_Get_processor_name(processor_name, &name_len);
    int broj_zapisa = (rank > 0) ? 1 : 0;

    int color = rank / 2;  
    MPI_Comm new_comm;
    MPI_Comm_split(MPI_COMM_WORLD, color, rank, &new_comm);
    
    int new_rank, new_size;
    MPI_Comm_rank(new_comm, &new_rank);
    MPI_Comm_size(new_comm, &new_size);

    for (int i = 0; i < size; i++)
    {  
        if(i == rank){
            fp = fopen("3zad.txt", "a");
            fprintf(fp, "P[%d] od [%d] se izvršava na procesoru [%s]. Prethodni proces napravio je [%d] zapisa. Rang unutar novog komunikatora [%d].\n", rank, size, processor_name, broj_zapisa, new_rank);
            fclose(fp);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }

    MPI_Finalize();

    return 0;
}