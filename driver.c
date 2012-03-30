#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#include "header.h"

void validate(int* output, int num_elements) {
  int i = 0;
  assert(output != NULL);
  for(i = 0; i < num_elements - 1; i++) {
    if (output[i] > output[i+1]) {
      printf("************* NOT sorted at i=%d *************\n",i);
      return;
    }
  }
  printf("============= SORTED ===========\n"); 
}

int main(int argc, char **argv)
{
    FILE* fin = NULL;
    int* input = NULL;
    int* output = NULL;
    int num_elements, num_threads, i = 0;

    if(argc != 2)
      {printf("Usage: ./pqsort <num of threads>\n");}
    
    num_threads = atoi(argv[1]);

    //read input_size and input
    if((fin = fopen("input.txt", "r")) == NULL)
      {printf("Error opening input file\n"); exit(0);}

    fscanf(fin, "%d", &num_elements);
    if( !(input = (int *)calloc(num_elements, sizeof(int))) )
      {printf("Memory error\n"); exit(0);}

    for(i = 0; i < num_elements || feof(fin); i++)
        fscanf(fin, "%d", &input[i]);
    
    if(i < num_elements)
      {printf("Invalid input\n"); exit(0);}

    struct timeval tv;
    gettimeofday(&tv, NULL);
    double t1=tv.tv_sec+(tv.tv_usec/1000000.0);
    output = pqsort(input, num_elements, num_threads);
    gettimeofday(&tv, NULL);
    double t2=tv.tv_sec+(tv.tv_usec/1000000.0);
    printf("Parallel execution time with %d threads on %d elements is %.6lfs\n",num_threads,num_elements,t2-t1);
    /*printf("\nOutput: \n");
    for(i=0;i<num_elements;i++){
        printf("%d ",output[i]);
    }
    printf("\n");*/

    validate(output, num_elements);
    return 0;
}
