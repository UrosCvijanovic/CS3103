#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>  //Header file for sleep(). man 3 sleep for details.
#include <pthread.h>

FILE *data_fp;
static int block_size;

void * printFunc(void * tid){
    long * myID = (long *) tid;
    printf("Hello World, this is thread %ld\n", myID);
}

int main() {
    pthread_t tid0;
    pthread_t tid1;
    pthread_t tid2;
    pthread_t tid3;
    pthread_t tid4;
    pthread_t tid5;
    pthread_t * pthreads[] = {&tid0, &tid1, &tid2, &tid3, &tid4, &tid5};
    data_fp = fopen(“fileName.txt”, “r”);
    int filesize = ftell(fp);
    int n_threads = 6;
    block_size = filesize/n_threads;
    for(int i = 0; i < n_threads; i++){
        start_offset = filesize*((float i)/n_threads) ;
        pthread_create(pthreads[i], NULL, search_start, &start_offset);
    }


    
    return 0;
}
