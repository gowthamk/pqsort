#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include<pthread.h>
#define NUM_THREADS 4
#define XOR ^
int *mail_box;
int num_threads;
pthread_mutex_t *lock_array;
pthread_cond_t *cond_array;
pthread_t *threads;
typedef struct {
    int *arr;
    int size;
    int thread_no;
} prefix_args_t;
void *pthread_prefix_sum(void *);
void seq_prefix_sum(int *,int);
void add_to_all(int *arr, int incr, int size);
/*
    todo: define this as macro
*/
int array_index(int row,int col)
{
    return row*num_threads+col;
}
int *prefix_sum(int *inp,int size, int _num_threads)
{
    int *arr = (int *)malloc(size * sizeof(int));
    memcpy(arr,inp,size*sizeof(int));
    printf("\n");
    num_threads = _num_threads;
    if(num_threads > size){
       num_threads = size;
    }
    pthread_t tids[num_threads*num_threads];
    pthread_attr_t attr;
    /* Initializing pthread attribs */
    pthread_attr_init(&attr);
    /* Making threads joinable */
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    /* Initializing shared memory for msg exchange */
    mail_box = (int *) malloc(num_threads*num_threads*sizeof(int));
    for(int i=0;i<num_threads*num_threads;i++){
        mail_box[i] = -1;
    }
    /* Initializing locks for shared memory */
    lock_array = (pthread_mutex_t *) malloc(num_threads*num_threads*sizeof(pthread_mutex_t));
    for(int i=0;i<num_threads*num_threads;i++){
        pthread_mutex_init(lock_array+i,NULL);
    }
    /* Initializing conditions for threads to act on */
    cond_array = (pthread_cond_t *) malloc(num_threads*num_threads*sizeof(pthread_cond_t));
    for(int i=0;i<num_threads*num_threads;i++){
        pthread_cond_init(cond_array+i,NULL);
    }
    /* Arguments for N threads */
    prefix_args_t *thread_args = (prefix_args_t *) malloc(num_threads * sizeof(prefix_args_t));
    int fair_share = size/num_threads;
    int max_share = fair_share+(size%num_threads);
    for(int i=0;i<num_threads;i++){
        thread_args[i].thread_no = i;
        thread_args[i].arr = arr+(i*fair_share);
        thread_args[i].size = (i==num_threads-1)?max_share:fair_share;
    }
    /* Launch N threads */
    for(int i=0;i<num_threads;i++){
        pthread_create(&tids[i],
                       &attr,
                       pthread_prefix_sum,
                       (void *)&thread_args[i]);
    }
    /* Wait for N threads to join */
    for(int i=0;i<num_threads;i++){
        pthread_join(tids[i],NULL);
    }
    return arr;

}
void *pthread_prefix_sum(void *tdata)
{
    prefix_args_t *myargs = (prefix_args_t *) tdata;
    int my_id = myargs->thread_no;
    int *arr = myargs->arr;
    int size = myargs->size;
    //printf("Thread no %d : size is %d\n",my_id,size);
    /* d holds the dimensions of imaginary hypercube */
    int d = ceil(log(num_threads));
    /* Get address of my inbox */
    int *my_mail = mail_box+(num_threads*my_id);
    /* Calculate sequential prefix_sum */
    seq_prefix_sum(arr,size);
    /* Last element in prefix_sum is sum of all elements */
    int sum = arr[size-1];
    /* msg is the running sum */
    int msg = sum;
    int partner = -1, mask = 0, number = 0, incr = 0;
    for(int i=0;i<=d-1;i++){
        mask = (mask==0)?1:mask<<1;
        partner = my_id XOR mask;
        /* Send msg to partner */
        pthread_mutex_lock(&lock_array[array_index(partner,my_id)]);
        printf("Thread(%d): sending %d to %d\n",my_id,msg,partner);
        mail_box[array_index(partner,my_id)] = msg;
        pthread_cond_signal(&cond_array[array_index(partner,my_id)]);
        pthread_mutex_unlock(&lock_array[array_index(partner,my_id)]);
        /* Recv number from partner */
        pthread_mutex_lock(&lock_array[array_index(my_id,partner)]);
        while(my_mail[partner] == -1){
            printf("Thread(%d): waiting for value from %d\n",my_id,partner);
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
    add_to_all(arr,incr,size);
    return NULL;
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
