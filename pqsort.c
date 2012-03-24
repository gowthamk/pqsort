#include <stdio.h>
#include <pthread.h>
int *msg_buf;
pthread_mutex_t *lock_array;
pthread_cond_t *cond_array;
pthread_t *threads;
int thread_count;
int compare (const void * a, const void * b)
{
  return ( *(int*)a - *(int*)b );
}

int get_pivot(int *input)
{
    int pivot = input[0];
    return pivot;
}

int *pqsort(int* input, int num_elements, int num_threads)
{
    threads = (pthread_t *)malloc(num_threads*sizeof(pthread_t));
    thread_count = num_threads;
    pthread_attr_t attr;
    msg_buf = (int *)calloc(num_threads*num_threads*sizeof(int));
    lock_array = (pthread_mutex_t *)calloc(num_threads*num_threads*sizeof(pthread_mutex_t));
    cond_array = (pthread_cond_t *)calloc(num_threads*num_threads*sizeof(pthread_cond_t));
    for(int i=0;i<num_threads*num_threads;i++){
        pthread_mutex_init(lock_array+i,NULL);
        pthread_cond_init(cond_array+i,NULL);
    }
    pthread_attr_init(&attr);
    for(i=0;i<num_threads;i++){
        pthread_create(&threads[i], &attr, pthread_pqsort_main, NULL);
    }
    return input; //return appropriately
}

void add_to_all(int incr,int *arr,int size){
    for(int i=0;i<size;i++){
        arr[i]+=incr;
    }
    return;
}

void seq_prefix_sum(int *arr,int num_elems){
    for(int i=0;i<num_elems;i++){
        arr[i]+=arr[i-1];
    }
    return;
}
/**
 * Entry points for parallel threads. Calulates posistion
 * of each element around a given pivot.
*/
int pthread_pqsort_main(int pivot,int *arr, int start, int end)
{
    int pos_vector[end-start+1];
    for(int i=start;i<=end;i++){
        if(arr[i]<pivot){
            pos_vector[i-start] = 1;
        }
        else {
            pos_vector[i-start] = 0;
        }
    }
    pthread_prefix_sum(pos_vector,end-start+1);
}
void pthread_prefix_sum(int arr,int num_elems)
{
    int myid;
    seq_prefix_sum(pos_vector,num_elems);
    pthread_t tid = pthread_self();
    for(int i=0;i<thread_count;i++)
        if(threads[i] == tid){
            myid = i;break;
        }
    if(myid%step == 0){
        for(int j=1;j<step;j++){
            pthread_mutex_lock(&lock_array[myid][j]);
            
        }
    }
    return;
}
