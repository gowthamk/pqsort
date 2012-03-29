#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include<pthread.h>
#define NUM_THREADS 4
#define XOR ^
#define array_index(row,col) (row*total_num_threads+col)
int *mail_box;
int total_num_threads;
pthread_mutex_t *lock_array;
pthread_cond_t *cond_array;
pthread_t *threads;
typedef struct {
    int thread_no;
    int num_threads;
    int *inp;
    int *aux;
    int *bin;
    int offset;
    int size;
    int pivot;
    int *pivot_replacement_ptr;
} prefix_args_t;
typedef struct {
    int  *inp;
    int first;
    int last;
    int *aux;
    int *bin;
    int num_threads;
    int thread_offset;
} pqsort_args_t;
void *_pqsort(void *);
void *pthread_pqsort(void *);
void seq_prefix_sum(int *,int);
void add_to_all(int *arr, int incr, int size);
int compare (const void * a, const void * b)
{
  return ( *(int*)a - *(int*)b );
}
int get_pivot(int *arr,int size)
{
    if(size<10)
        return arr[size/2];
    int num_samples;
    num_samples = (size>=1000)?111:(size>=100?21:6);
    int offset = (size/2)>num_samples?(size/2):0;
    int samples[num_samples];
    memcpy(samples,arr+offset,num_samples*sizeof(int));
    qsort(samples,num_samples,sizeof(int),compare);
    return samples[num_samples/2];
}
int *pqsort(int *inp,int size,int num_threads)
{
    total_num_threads = num_threads;
    /* Initialize all shared datastructures that
       stay alive througout the execution
     */
    mail_box = (int *) malloc(num_threads*num_threads*sizeof(int));
    lock_array = (pthread_mutex_t *) malloc(num_threads*num_threads*sizeof(pthread_mutex_t));
    cond_array = (pthread_cond_t *) malloc(num_threads*num_threads*sizeof(pthread_cond_t));
    /* auxilary and binary array for pivot rearrangement and prefix sum */
    int *aux = (int *) malloc(size*sizeof(int));
    int *bin = (int *) malloc(size*sizeof(int));
    for(int i=0;i<num_threads;i++){
        for(int j=0;j<num_threads;j++){
            /* Initializing shared memory for msg exchange */
            mail_box[array_index(i,j)] = -1;
            /* Initializing locks for shared memory */
            pthread_mutex_init(&lock_array[array_index(i,j)],NULL);
            /* Initializing conditions for threads to act on */
            pthread_cond_init(&cond_array[array_index(i,j)],NULL);
        }
    }
    /* Launch first instance of quicksort */
    pqsort_args_t args;
    args.inp = inp;
    args.first = 0;
    args.last = size-1;
    args.aux = aux;
    args.bin = bin;
    args.num_threads = num_threads;
    args.thread_offset = 0;
    _pqsort((void *)&args);
    return inp;
}
void *_pqsort(void *pqdata)
{
    pqsort_args_t *my_args = (pqsort_args_t *)pqdata;
    int *inp = my_args->inp;
    int first = my_args->first;
    int last = my_args->last;
    int *aux = my_args->aux;
    int *bin = my_args->bin;
    int num_threads = my_args->num_threads;
    int thread_offset = my_args->thread_offset;
    if(last<=first){
        return NULL;
    }
    int *arr = inp+first; 
    int size = last-first+1;
    aux+=first;
    bin+=first;
    /*printf("[");
    for(int i=0;i<size;i++)
    {
        printf("%d ",arr[i]);
    }
    printf("]\n");*/
    /* Firstly, if num_threads == 1, we are here for a sequential sort*/
    if(num_threads == 1 || size<num_threads){
        qsort(arr,size,sizeof(int),compare);
        return NULL;
    }
    int pivot = get_pivot(arr,size);
    int pivot_replacement_index;
    //printf("Pivot is %d\n",pivot);
    pthread_t tids[num_threads];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    int fair_share = size/num_threads;
    int max_share = fair_share+(size%num_threads);
    prefix_args_t *thread_args = (prefix_args_t *) malloc(num_threads * sizeof(prefix_args_t));
    for(int i=0;i<num_threads;i++){
        /* Initialize Arguments for N threads */
        thread_args[i].thread_no = i + thread_offset;
        thread_args[i].num_threads = num_threads;
        thread_args[i].inp = arr;
        thread_args[i].aux = aux;
        thread_args[i].bin = bin;
        thread_args[i].offset = i*fair_share;
        thread_args[i].size = (i==num_threads-1)?max_share:fair_share;
        thread_args[i].pivot = pivot;
        thread_args[i].pivot_replacement_ptr = &pivot_replacement_index;
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
    //printf("Pivot index is %d\n",(int) pivot_index);
    /* Launch 2 new threads to deal with 2 recursive calls */
    pqsort_args_t args[2];
    args[0].inp = arr;
    args[0].first = 0;
    args[0].last = pivot_index-1;
    args[0].aux = aux;
    args[0].bin = bin;
    args[0].num_threads = num_threads/2;
    args[0].thread_offset = thread_offset;
    pthread_create(&tids[0],
                   &attr,
                   _pqsort,
                   (void *)&args[0]);
    args[1].inp = arr;
    args[1].first = pivot_index+1;
    args[1].last = size-1;
    args[1].aux = aux;
    args[1].bin = bin;
    args[1].num_threads = num_threads - (num_threads/2);
    args[1].thread_offset = thread_offset+(num_threads/2);
    pthread_create(&tids[1],
                   &attr,
                   _pqsort,
                   (void *)&args[1]);
    for(int i=0;i<2;i++){
        pthread_join(tids[i],NULL);
    }
    return NULL;
}
void *pthread_pqsort(void *tdata)
{
    prefix_args_t *myargs = (prefix_args_t *) tdata;
    int my_id = myargs->thread_no;
    //printf("My Id is %d\n",my_id);
    int num_threads = myargs->num_threads;
    int *inp = myargs->inp;
    int offset = myargs->offset;
    int *arr = inp + offset;
    int *aux = (myargs->aux)+offset;
    int *bin = (myargs->bin)+offset;
    int size = myargs->size;
    int pivot = myargs->pivot;
    int *pivot_replacement_ptr = myargs->pivot_replacement_ptr;
    //int aux[size],bin[size];
    //int *aux = (int *)malloc(size*sizeof(int));
    //int *bin = (int *)malloc(size*sizeof(int));
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
    int d = ceil(log(num_threads)/log(2));
    //printf("num_threads is %d and d is %d\n",num_threads,d);
    /* Get address of my inbox */
    int *my_mail = mail_box+(total_num_threads*my_id);
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
        //printf("Thread(%d): Recieved %d from %d\n",my_id,number,partner);
        msg+=number;
        /* If partner deals with lesser indexes in array, add its
           results to your elements
        */
        if(partner < my_id)
            incr+=number;
    }
    add_to_all(bin,incr,size);
    /* Reset mailbox for reuse at next level */
    for(int i=0;i<total_num_threads;i++){
        my_mail[i] = -1;
    }
    /* END PREFIX SUM on bin*/
    int cur_index,new_index,returnable = 0;
    for(int i=0;i<size;i++){
        if(aux[i] <  pivot){
            new_index = bin[i]-1;
        }
        else if(aux[i] == pivot){
            new_index = bin[i]-1;
            *pivot_replacement_ptr = bin[i]-1;
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
