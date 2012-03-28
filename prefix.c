#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include<pthread.h>
#define NUM_THREADS 4
#define XOR ^
int *mail_box;
int num_threads;
int pivot_replacement_index;
pthread_mutex_t *lock_array;
pthread_cond_t *cond_array;
pthread_t *threads;
typedef struct {
    int thread_no;
    int *inp;
    int offset;
    int size;
    int pivot;
} prefix_args_t;
void _pqsort(int *,int,int);
void *pthread_pqsort(void *);
void seq_prefix_sum(int *,int);
void add_to_all(int *arr, int incr, int size);
/*
    todo: define this as macro
*/
int array_index(int row,int col)
{
    return row*num_threads+col;
}
int compare (const void * a, const void * b)
{
  return ( *(int*)a - *(int*)b );
}
int get_pivot(int *arr,int size)
{
    if(size<10)
        return arr[size/2];
    int num_samples;
    num_samples = (size>=1000)?21:(size>=100?11:3);
    int offset = (size/2)>num_samples?(size/2):0;
    int samples[num_samples];
    memcpy(samples,arr+offset,num_samples*sizeof(int));
    qsort(samples,num_samples,sizeof(int),compare);
    return samples[num_samples/2];
}
int *pqsort(int *inp,int size,int _num_threads)
{
    num_threads = _num_threads;
    _pqsort(inp,0,size-1);
    return inp;
}
void _pqsort(int *inp,int first, int last)
{
    if(last<=first){
        return;
    }
    int *arr = inp+first; /*(int *)malloc(size * sizeof(int));
    memcpy(arr,inp,size*sizeof(int));
    printf("\n");*/
    int size = last-first+1;
    /* Firstly, if size<num_threads, we are here for a sequential sort*/
    if(size <= num_threads){
        qsort(arr,size,sizeof(int),compare);
        return;
    }
    int pivot = get_pivot(arr,size);
    printf("Pivot is %d\n",pivot);
    pthread_t tids[num_threads*num_threads];
    pthread_attr_t attr;
    /* Initializing pthread attribs */
    pthread_attr_init(&attr);
    /* Making threads joinable */
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    mail_box = (int *) malloc(num_threads*num_threads*sizeof(int));
    lock_array = (pthread_mutex_t *) malloc(num_threads*num_threads*sizeof(pthread_mutex_t));
    cond_array = (pthread_cond_t *) malloc(num_threads*num_threads*sizeof(pthread_cond_t));
    prefix_args_t *thread_args = (prefix_args_t *) malloc(num_threads * sizeof(prefix_args_t));
    int fair_share = size/num_threads;
    int max_share = fair_share+(size%num_threads);
    for(int i=0;i<num_threads;i++){
        /* Initialize Arguments for N threads */
        thread_args[i].thread_no = i;
        thread_args[i].inp = arr;
        thread_args[i].offset = i*fair_share;
        thread_args[i].size = (i==num_threads-1)?max_share:fair_share;
        thread_args[i].pivot = pivot;
        for(int j=0;j<num_threads;j++){
            /* Initializing shared memory for msg exchange */
            mail_box[array_index(i,j)] = -1;
            /* Initializing locks for shared memory */
            pthread_mutex_init(&lock_array[array_index(i,j)],NULL);
            /* Initializing conditions for threads to act on */
            pthread_cond_init(&cond_array[array_index(i,j)],NULL);
        }
    }
    /* Launch N threads */
    for(int i=0;i<num_threads;i++){
        pthread_create(&tids[i],
                       &attr,
                       pthread_pqsort,
                       (void *)&thread_args[i]);
    }
    /* Wait for N threads to join */
    int pivot_index;
    for(int i=0;i<num_threads;i++){
        pthread_join(tids[i],(void *) &pivot_index);
    }
    if(arr[pivot_index]!=pivot){
        arr[pivot_replacement_index] = arr[pivot_index];
        arr[pivot_index] = pivot;
    }
    printf("Pivot index is %d\n",(int) pivot_index);
    free(mail_box);
    free(lock_array);
    free(cond_array);
    free(thread_args);
    _pqsort(arr,0,(int) pivot_index-1);
    _pqsort(arr,(int) pivot_index+1,size-1);
    return;

}
void *pthread_pqsort(void *tdata)
{
    prefix_args_t *myargs = (prefix_args_t *) tdata;
    int my_id = myargs->thread_no;
    int *inp = myargs->inp;
    int offset = myargs->offset;
    int *arr = inp + offset;
    int size = myargs->size;
    int pivot = myargs->pivot;
    int aux[size],bin[size];
    memcpy(aux,arr,size*sizeof(int));
    for(int i=0;i<size;i++){
        if(aux[i] <= pivot){
            bin[i] = 1;
        }
        else
            bin[i] = 0;
    }
    /* START PREFIX SUM on bin*/
    //printf("Thread no %d : size is %d\n",my_id,size);
    /* d holds the dimensions of imaginary hypercube */
    int d = ceil(log(num_threads));
    /* Get address of my inbox */
    int *my_mail = mail_box+(num_threads*my_id);
    /* Calculate sequential prefix_sum */
    seq_prefix_sum(bin,size);
    /* Last element in prefix_sum is sum of all elements */
    int sum = bin[size-1];
    /* msg is the running sum */
    int msg = sum;
    int partner = -1, mask = 0, number = 0, incr = 0;
    for(int i=0;i<=d-1;i++){
        mask = (mask==0)?1:mask<<1;
        partner = my_id XOR mask;
        /* Send msg to partner */
        pthread_mutex_lock(&lock_array[array_index(partner,my_id)]);
        //printf("Thread(%d): sending %d to %d\n",my_id,msg,partner);
        mail_box[array_index(partner,my_id)] = msg;
        pthread_cond_signal(&cond_array[array_index(partner,my_id)]);
        pthread_mutex_unlock(&lock_array[array_index(partner,my_id)]);
        /* Recv number from partner */
        pthread_mutex_lock(&lock_array[array_index(my_id,partner)]);
        while(my_mail[partner] == -1){
            //printf("Thread(%d): waiting for value from %d\n",my_id,partner);
            pthread_cond_wait(&cond_array[array_index(my_id,partner)],
                              &lock_array[array_index(my_id,partner)]);
        }
        number = my_mail[partner];
        pthread_mutex_unlock(&lock_array[array_index(my_id,partner)]);
        msg+=number;
        /* If partner deals with lesser indexes in array, add its
           results to your elements
        */
        if(partner < my_id)
            incr+=number;
    }
    add_to_all(bin,incr,size);
    /* END PREFIX SUM on bin*/
    int cur_index,new_index,returnable = 0;
    for(int i=0;i<size;i++){
        if(aux[i] <  pivot){
            new_index = bin[i]-1;
        }
        else if(aux[i] == pivot){
            new_index = msg-1;
            pivot_replacement_index = bin[i]-1;
        }
        else {
            cur_index = offset+i;
            /* Fix: msg could be wrong if num_threads not power of 2 */
            new_index = msg+(cur_index - bin[i]);
        }
        inp[new_index] = aux[i];
    }
    return (void *) (msg - 1);
}
void seq_prefix_sum(int *arr,int num_elems){
    for(int i=1;i<num_elems;i++){
        arr[i]+=arr[i-1];
    }
    return;
}
void add_to_all(int *arr, int incr, int size){
    for(int i=0;i<size;i++){
        arr[i]+=incr;
    }
    return;
}
