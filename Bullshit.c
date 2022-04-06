#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>  //Header file for sleep(). man 3 sleep for details.
#include <pthread.h>


void * search_start(FILE **fp){
    
}

int main() {
    FILE *data_fp;
    FILE *ptr_writefile;
    pthread_t tid0;
    pthread_t tid1;
    pthread_t tid2;
    pthread_t tid3;
    pthread_t tid4;
    pthread_t tid5;
    pthread_t tid6;
    pthread_t tid7;
    pthread_t tid8;
    pthread_t * pthreads[] = {&tid0, &tid1, &tid2, &tid3, &tid4, &tid5,
    &tid6, &tid7, &tid8};
    char line [128]; /* or some other suitable maximum line size */
	char fileoutputname[15];
    int filecounter=1, linecounter=1;

    data_fp = fopen("test.txt", "r");

    if (!data_fp){
        return 1;
    }
	
	sprintf(fileoutputname, "file_part%d", filecounter);
	ptr_writefile = fopen(fileoutputname, "w");
    

    while (fgets(line, sizeof line, data_fp)!=NULL) {
		if (linecounter == 40) {
            pthread_create(pthreads[filecounter], NULL, search_start(FILE **), &ptr_writefile);
			fclose(ptr_writefile);
			linecounter = 1;
			filecounter++;
			sprintf(fileoutputname, "file_part%d", filecounter);
			ptr_writefile = fopen(fileoutputname, "w");
			if (!data_fp){
                return 1;
            }	
		}
		fprintf(ptr_writefile,"%s\n", line);
		linecounter++;
	}

	fclose(data_fp);

    return 0;
}
   
