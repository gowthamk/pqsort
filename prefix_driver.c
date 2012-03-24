#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "prefix.h"

void validate(int* output, int *input, int num_elements) {
  assert(output != NULL);
  seq_prefix_sum(input,num_elements);
  for(int i = 0; i < num_elements - 1; i++) {
    if (output[i] != input[i]) {
      printf("PREFIX SUM WRONG AT %d\n",i);
      return;
    }
  }
  printf("============= PREFIX SUM CORRECT ===========\n"); 
}

int main(int argc, char **argv)
{
    FILE* fin = NULL;
    int* input = NULL;
    int* output = NULL;
    int num_elements, num_threads, i = 0;

    if(argc != 2)
      {printf("Usage: ./prefix <num of threads>\n");}
    
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

    int *cpy = (int *) malloc(num_elements*sizeof(int));
    for(i=0;i<num_elements;i++){
        cpy[i] = input[i];
    }

    printf("Num threads : %d Num elems: %d\n",num_threads,num_elements);
    output = prefix_sum(input, num_elements, num_threads);

    validate(output, cpy, num_elements);
    return 0;
}
