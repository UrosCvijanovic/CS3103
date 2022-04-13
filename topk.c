#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <errno.h>

int total_threads = 0;
char targetDir[20] = {0};
char startDate[11] = {0};
int entryCount = 0;


struct struct_readChunk_args {
    FILE * file;
    long start;
    long end;
    int n;
};

typedef struct struct_readChunk_args readCunck_args;

long get_file_length(const char* file_name){
    struct stat sb;
    if(stat(file_name, &sb) == -1){exit(-1);}
    return sb.st_size;
}

void *readChunck(void *args){
    readCunck_args *actual_args = (readCunck_args*) args;
    FILE *file = actual_args->file;
    long start = actual_args->start;
    long end = actual_args->end;
    int n = actual_args->n;
    long read = 0;

    fseek(file, start, SEEK_SET);
    char buffer[22];

    read = fread(buffer,22,1,file);
    while(start+read < end){
        int r = fread(buffer,22,1,file);
        if(r==0) break;
        read += r;
        printf("%s", buffer);
    }
}

void readFile(char *f){
    FILE *file = fopen(f, "r");
    readCunck_args args[total_threads];

    long file_len = get_file_length(f);
    long size_for_each_thread = file_len/total_threads;

    pthread_t readers[total_threads];

    for(size_t i = 0; i < total_threads; i++){
        args[i].file = fopen(f, "r");
        args[i].start = i * size_for_each_thread;
        args[i].end = args[i].start + size_for_each_thread; 
        args[i].n = i;
    }

    for(size_t i = 0; i < total_threads; i++){
        pthread_create(&readers[i], NULL, readChunck, &args[i]);
    }

      for(size_t i = 0; i < total_threads; i++){
        pthread_join(readers[i], NULL);
        fclose(args[i].file);
    }
}

void readDir(){
    DIR * d;
    struct dirent *dir;
    d = opendir(targetDir);
    if(d){
        while((dir = readdir(d))!=NULL){
            if(dir->d_type == DT_REG){
                char path[300];
                snprintf(path, 300, "./%s/%s", targetDir, dir->d_name);
                readFile(path);
            }
        }
        closedir(d);
    }
}

int main(int argc, char **argv){
    total_threads = get_nprocs() * 2;
    total_threads = 4;

    strcpy(targetDir, argv[1]);
    strcpy(startDate, argv[1]);
    entryCount = atoi(argv[3]);

    targetDir[strcspn(targetDir, "/")] = 0;
    readDir();

    return 0;
}