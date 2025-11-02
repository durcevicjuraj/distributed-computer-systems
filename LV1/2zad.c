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

    for (int i = 0; i < size; i++)
    {  
        if(i == rank){
            fp = fopen("2zad.txt", "a");
            fprintf(fp, "P[%d] od [%d] se izvršava na procesoru [%s]. Prethodni proces napravio je [%d] zapisa.\n", rank, size, processor_name, broj_zapisa);
            fclose(fp);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }

    MPI_Finalize();

    return 0;
}