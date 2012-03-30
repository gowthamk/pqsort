#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include<pthread.h>
#include<assert.h>
#define NUM_THREADS 4
#define XOR ^
#define array_index(row,col) (row*total_num_threads+col)
int *mail_box1;
int *mail_box2;
int total_num_threads;
int total_num_elements;
pthread_mutex_t *lock_array;
pthread_cond_t *cond_array;
pthread_t *threads;
pthread_attr_t attr;
typedef struct {
    int thread_no;
    int num_threads;
    int *inp;
    int *aux;
    int offset;
    int size;
    int pivot;
    int *pivot_replacement_ptr;
} prefix_args_t;
prefix_args_t *thread_args;
typedef struct {
    int  *inp;
    int first;
    int last;
    int *aux;
    int num_threads;
    int thread_offset;
} pqsort_args_t;
void *_pqsort(void *);
void *pthread_pqsort(void *);
void  seq_prefix_sum(int *,int);
int   seq_pivot_rearrange(int *,int,int);
void  add_to_all(int *arr, int incr, int size);
int   compare (const void * a, const void * b)
{
  return ( *(int*)a - *(int*)b );
}
int get_pivot(int *arr,int size)
{
    if(size<10)
        return arr[size/2];
    int num_samples;
    num_samples = (size>=1000)?111:(size>=100?21:6);
    int offset = (size/2)>num_samples?((size/2)-(num_samples/2)):0;
    //int samples[num_samples];
    //memcpy(samples,arr+offset,num_samples*sizeof(int));
    qsort(arr+offset,num_samples,sizeof(int),compare);
    return arr[offset+num_samples/2];
}
int *pqsort(int *inp,int size,int num_threads)
{
    total_num_threads = num_threads;
    total_num_elements = size;
    /* Initialize all shared datastructures that
       stay alive througout the execution
     */
    mail_box1 = (int *) malloc(num_threads*num_threads*sizeof(int));
    mail_box2 = (int *) malloc(num_threads*num_threads*sizeof(int));
    lock_array = (pthread_mutex_t *) malloc(num_threads*num_threads*sizeof(pthread_mutex_t));
    cond_array = (pthread_cond_t *) malloc(num_threads*num_threads*sizeof(pthread_cond_t));
    thread_args = (prefix_args_t *) malloc(num_threads * sizeof(prefix_args_t));
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    /* auxilary array for pivot rearrangement and prefix sum */
    int *aux = (int *) malloc(size*sizeof(int));
    for(int i=0;i<num_threads;i++){
        for(int j=0;j<num_threads;j++){
            /* Initializing shared memory for msg exchange */
            mail_box1[array_index(i,j)] = -1;
            mail_box2[array_index(i,j)] = -1;
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
    args.num_threads = num_threads;
    args.thread_offset = 0;
    _pqsort((void *)&args);
    if((int) ceil(log(num_threads)/log(2))%2 == 0)
        return inp;
    else
        return aux;
}
void *_pqsort(void *pqdata)
{
    pqsort_args_t *my_args = (pqsort_args_t *)pqdata;
    int *inp = my_args->inp;
    int first = my_args->first;
    int last = my_args->last;
    int *aux = my_args->aux;
    int num_threads = my_args->num_threads;
    int thread_offset = my_args->thread_offset;
    if(last<first){
        return NULL;
    }
    int *arr = inp+first; 
    int size = last-first+1;
    aux+=first;
    /* In every recursive call, aux is a shadow for arr */
    /* Firstly, if num_threads == 1, we are here for a sequential sort*/
    if(num_threads == 1 || size<num_threads){
        qsort(arr,size,sizeof(int),compare);
        if(num_threads!=1 && ((int) ceil(log(total_num_threads/num_threads)/log(2))%2 == 0))
            memcpy(aux,arr,size*sizeof(int));
        return NULL;
    }
    int pivot = get_pivot(arr,size);
    int pivot_replacement_index;
   // printf("Pivot is %d\n",pivot);
    pthread_t tids[num_threads];

    int fair_share = size/num_threads;
    int max_share = fair_share+(size%num_threads);
    int global_tid;
    for(int i=0;i<num_threads;i++){
        /* Initialize Arguments for N threads */
        global_tid = i + thread_offset;
        thread_args[global_tid].thread_no = global_tid;
        thread_args[global_tid].num_threads = num_threads;
        thread_args[global_tid].inp = arr;
        thread_args[global_tid].aux = aux;
        thread_args[global_tid].offset = i*fair_share;
        thread_args[global_tid].size = (i==num_threads-1)?max_share:fair_share;
        thread_args[global_tid].pivot = pivot;
        thread_args[global_tid].pivot_replacement_ptr = &pivot_replacement_index;
    }
    /* Launch N threads */
    for(int i=0;i<num_threads;i++){
        global_tid = i + thread_offset;
        pthread_create(&tids[i],
                       &attr,
                       pthread_pqsort,
                       (void *)&thread_args[global_tid]);
    }
    /* Wait for N threads to join */
    int pivot_index;
    for(int i=0;i<num_threads;i++){
        pthread_join(tids[i],(void *) &pivot_index);
    }
    /* Global positioning of pivot */
    /* aux is the new arr */
    if(aux[pivot_index]!=pivot){
        assert(pivot_replacement_index>=0);
        aux[pivot_replacement_index] = aux[pivot_index];
        aux[pivot_index] = pivot;
    }
    /* arr is the aux of next level */
    arr[pivot_index] = pivot;
    pqsort_args_t args[2];
    args[0].inp = aux;
    args[0].first = 0;
    args[0].last = pivot_index-1;
    args[0].aux = arr;
    args[0].num_threads = num_threads/2;
    args[0].thread_offset = thread_offset;
    pthread_create(&tids[0],
                   &attr,
                   _pqsort,
                   (void *)&args[0]);
    args[1].inp = aux;
    args[1].first = pivot_index+1;
    args[1].last = size-1;
    args[1].aux = arr;
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
    int *aux = myargs->aux;
    /* Here, inp and aux correspond to each other
       arr is just my part of input array.*/
    int size = myargs->size;
    int pivot = myargs->pivot;
    int *pivot_replacement_ptr = myargs->pivot_replacement_ptr;
    int num_le = seq_pivot_rearrange(arr,size,pivot);
    int num_g = size - num_le;
    int pivot_index = -1;
    if(num_le >0 && arr[num_le-1] == pivot){
        pivot_index = num_le-1;
    }
    /* START PREFIX SUM */
    int d = ceil(log(num_threads)/log(2));
    //printf("num_threads is %d and d is %d\n",num_threads,d);
    int *my_mail1 = mail_box1+(total_num_threads*my_id);
    int *my_mail2 = mail_box2+(total_num_threads*my_id);
    int msg1 = num_le;
    int msg2 = num_g;
    int partner = -1, mask = 0, number1 = 0, number2 =0, incr1 = 0, incr2 = 0;
    for(int i=0;i<=d-1;i++){
        mask = (mask==0)?1:mask<<1;
        partner = my_id XOR mask;
        /* Send msg to partner */
        pthread_mutex_lock(&lock_array[array_index(partner,my_id)]);
        //printf("Thread(%d): sending %d and %d to %d\n",my_id,msg1,msg2,partner);
        mail_box1[array_index(partner,my_id)] = msg1;
        mail_box2[array_index(partner,my_id)] = msg2;
        pthread_cond_signal(&cond_array[array_index(partner,my_id)]);
        pthread_mutex_unlock(&lock_array[array_index(partner,my_id)]);
        /* Recv number from partner */
        pthread_mutex_lock(&lock_array[array_index(my_id,partner)]);
        while(my_mail2[partner] == -1){
            //printf("Thread(%d): waiting for value from %d\n",my_id,partner);
            pthread_cond_wait(&cond_array[array_index(my_id,partner)],
                              &lock_array[array_index(my_id,partner)]);
        }
        number1 = my_mail1[partner];
        number2 = my_mail2[partner];
        pthread_mutex_unlock(&lock_array[array_index(my_id,partner)]);
        //printf("Thread(%d): Recieved %d and %d from %d\n",my_id,number1,number2,partner);
        msg1+=number1;
        msg2+=number2;
        /* If partner deals with lesser indexes in array, add its
           results to your elements
        */
        if(partner < my_id){
            incr1+=number1;
            incr2+=number2;
        }
    }
    /* Reset mailbox for reuse at next level */
    /* TODO reset only ones that are actually used*/
    for(int i=0;i<total_num_threads;i++){
        my_mail1[i] = -1;
        my_mail2[i] = -1;
    }
    /* END PREFIX SUM */
    int my_lower_offset = incr1;
    int my_higher_offset = msg1+incr2;
    //printf("Thread(%d): lower offset %d Higher offset %d msg1=%d msg2 = %d\n",my_id,my_lower_offset,my_higher_offset,msg1,msg2);
    if(num_le > 0)
        memcpy(aux+my_lower_offset,arr,num_le*sizeof(int));
    if(num_g > 0)
        memcpy(aux+my_higher_offset,(arr+num_le),num_g*sizeof(int));
    /* Assuming imaginary barrier here, aux will have all number less than
       or equal to pivot from 0 to msg-1 and all numbers greater than
       pivot from msg to total_num_elems-1.
     */
     if(pivot_index >= 0){
        *pivot_replacement_ptr = incr1+pivot_index;
     }

    return (void *) (msg1 - 1);
}
int seq_pivot_rearrange(int *arr,int size,int pivot)
{
    int pivot_index = -1,i=0,j=size-1,tmp;
    while(i<j){
        while(arr[i]<pivot && i<size)
            i++;
        if(i<size && arr[i]==pivot){
            pivot_index = i;
            i++;
            continue;
        }
        while(arr[j]>pivot && j>0)
            j--;
        if(i<j){
            tmp = arr[i];
            arr[i] = arr[j];
            arr[j] = tmp;
            i++;j--;
        }
    }
    if(i==j){
        if(arr[i]<pivot)
            i++;
        else if(arr[i]==pivot){
            pivot_index = -1;
            i++;
        }
    }
    /*POSTCONDITION: arr[i-1] holds last number <= pivot*/
    if(pivot_index!=-1){
        arr[pivot_index] = arr[i-1];
        arr[i-1] = pivot;
        //printf("Pivot %d will be location %d\n",pivot,i-1);
    }
    /* There are i elements less than or equal to pivot */
    return i;
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
