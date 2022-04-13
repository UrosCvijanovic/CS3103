#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <dirent.h>
#include <pthread.h>
#include <math.h>
#include <time.h>

int total_threads = 0;
char targetDir[20] = {0};
long startDate = 0;
int enrtyCount = 0;

struct struct_readChunk_args {
	FILE *file;
	long start;							// seek start
	long end;								// seek end
};

#define buf_size 22				// for reading entries from files
#define count_max 466402	// max date 1679046032 divided by 3600

typedef struct struct_readChunk_args readChunk_args;

int counter[count_max] = {0};	// holds frequency of each index
int idx_arr[count_max];	// holds index as value, used in sorting

// ----------------------------------------------------------------------------

void parseEntry(char *str) {
	char buffer[buf_size];
	char delim[] = "\n ,";
	strcpy(buffer, str);

	char *ptr = strtok(buffer, delim);	// parsing entry

	long timestamp = atoi(ptr);

	if(timestamp < startDate) return;	// ignore timestamps less than startDate

	int t = floor(atoi(ptr) / 3600); // timestamp clipped to days

	counter[t]++;
}

void *readChunk(void *args) {
	readChunk_args *actual_args = (readChunk_args*) args;
	
	FILE *file = actual_args->file;
	long start = actual_args->start;
	long end = actual_args->end;
	long read = 0;

	fseek(file, start, SEEK_SET);

	char c[1];
	int offset = 1;

	// place seek just after the new line
	while(start != 0 && c[0] != '\n') {
		fseek(file, start + offset, SEEK_SET);
		fread(c, 1, 1, file);
		offset++;
	}

	if(offset > 1) fseek(file, start + offset + 1, SEEK_SET);

	char buffer[buf_size];
	
	// read each line and call parsing function on each
	while (start + read < end) {
		char *r = fgets(buffer, buf_size, file);
		if(r == NULL) break;
		read += strlen(buffer);

		if(buffer[0] != '\n')
			parseEntry(buffer);

		memset(buffer, 0, buf_size);
	}
}

long get_file_length(const char* file_name){
	struct stat sb;
	if (stat(file_name,&sb)==-1){exit(-1);}
	return sb.st_size;
}

// open file, create threads to read file in chunks
void readFile(char *f) {
	FILE *file = fopen(f, "r");

	readChunk_args args[total_threads];

	long file_len = get_file_length(f); 
	long size_for_each_thread = file_len / total_threads;
	long start = 0;

	pthread_t readers[total_threads];

	for (size_t i = 0; i < total_threads; i++)
	{
		args[i].file = fopen(f, "r");
		args[i].start = i * size_for_each_thread;
		args[i].end = args[i].start + size_for_each_thread;
	}

	for (size_t i = 0; i < total_threads; i++)
	{
		pthread_create(&readers[i], NULL, readChunk, &args[i]);
	}

	for (size_t i = 0; i < total_threads; i++)
	{
		pthread_join(readers[i], NULL);
		fclose(args[i].file);
	}
	
}

// open directory & call function on each file
void readDir() {
	DIR *d;
  struct dirent *dir;
  d = opendir(targetDir);
  if (d) {
    while ((dir = readdir(d)) != NULL) {
			if(dir->d_type == DT_REG) {
				char path[300];
				snprintf(path, 300, "./%s/%s", targetDir, dir->d_name);
      	readFile(path);
			}
    }
    closedir(d);
  }
}

void swap(int *a, int *b) {
  int tmp = *a;
  *a = *b;
  *b = tmp;
}

// chose selection sort because, for example: if we just need
// top 5 values then why bother sorting the whole data ?
// So thats why selection sort is a better option here 
void selectionSort(int arr_A[], int arr_B[], int size) {
	int end = enrtyCount < (size - 1) ? enrtyCount : (size - 1);

  for (int step = 0; step < end; step++) {
    int max_idx = step;
    for (int i = step + 1; i < size; i++) {
      if (arr_A[i] > arr_A[max_idx])
        max_idx = i;
			else if(arr_A[i] == arr_A[max_idx])
				max_idx = i > max_idx ? i : max_idx;
    }
    swap(&arr_A[max_idx], &arr_A[step]);
    swap(&arr_B[max_idx], &arr_B[step]);
  }
}

void processResults() {

	// initialize idx_arr

	for (size_t i = 0; i < count_max; i++)
		idx_arr[i] = i;
	
	// sort parallel arrays counter and idx_arr
	selectionSort(counter, idx_arr, count_max);
	
}

void displayResults() {
	const char date_format[] = "%c";

	printf("Top K frequently accessed hour:\n");

	for (size_t i = 0; i < enrtyCount; i++)
	{
		time_t time_val = idx_arr[i] * 3600;
		struct tm *time = localtime(&time_val);

		char buffer[80];
		strftime(buffer, 80, date_format, time);

		printf("%s\t%d\n", buffer, counter[i]);
	}
}

int main(int argc, char **argv)
{
	// 2 threads per processor
	total_threads = get_nprocs() * 2;

	strcpy(targetDir, argv[1]);
	startDate = atoi(argv[2]);
	enrtyCount = atoi(argv[3]);

	targetDir[strcspn(targetDir, "/")] = 0;

	readDir();

	processResults();

	displayResults();

  return 0;
}
