#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <dirent.h>
#include <pthread.h>
#include <math.h>
#include <time.h>

#define HASH_TABLE_SIZE 1000

int total_threads = 0;
char targetDir[20] = {0};
long startDate = 0;
int enrtyCount = 0;
static int k;

struct struct_readChunk_args {
	FILE *file;
	long start;							// seek start
	long end;								// seek end
};

#define buf_size 22				// for reading entries from files
#define count_max 466402	// max date 1679046032 divided by 3600

typedef struct struct_readChunk_args readChunk_args;

/*
<Hash Table implementation using seperate chaining>
Reference: https://pages.cs.wisc.edu/~remzi/OSTEP/threads-locks-usage.pdf (Operating Systems: Three Easy Pieces)
*/

// A node containing timestamp and no. of occurance
typedef struct key_value
{
    long timestamp;
    long count;
    struct key_value *next;
} key_value;

// Compare if the value of key-value pair a is smaller than b
bool smallerThan(key_value *a, key_value *b)
{
    if (a->count < b->count)
    {
        return true;
    }
    else if (a->count == b->count && a->timestamp < b->timestamp)
    {
        return true;
    }
    else
    {
        return false;
    }
}

// A node as a header of each linked list, containing a lock
typedef struct list_header
{
    key_value *front;
    pthread_mutex_t lock;
} list_header;

// Hash table for global access
static list_header hasht[HASH_TABLE_SIZE];

// Initialize the hash table
void initHashTable()
{
    for (int i = 0; i < HASH_TABLE_SIZE; i++)
    {
        hasht[i].front = NULL;
        pthread_mutex_init(&hasht[i].lock, NULL);
    }
}

// Function that return the hash index
long getHashIdx(long key)
{
    return (key / 100) % HASH_TABLE_SIZE;
}

// Insert a record to the hash table
void *insert(long key)
{
    long hashKey = getHashIdx(key);
    pthread_mutex_lock(&hasht[hashKey].lock);
    if (hasht[hashKey].front == NULL)
    {
        key_value *newEntry = (struct key_value *)malloc(sizeof(key_value));
        newEntry->timestamp = key;
        newEntry->count = 1;
        hasht[hashKey].front = newEntry;
    }
    else
    {
        bool found = false;
        key_value *pointer = hasht[hashKey].front;
        while (pointer != NULL)
        {
            if (pointer->timestamp == key)
            {
                pointer->count++;
                found = true;
                break;
            }
            else
            {
                pointer = pointer->next;
            }
        }
        if (!found)
        {
            key_value *newEntry = (struct key_value *)malloc(sizeof(key_value));
            newEntry->timestamp = key;
            newEntry->count = 1;
            newEntry->next = hasht[hashKey].front;
            hasht[hashKey].front = newEntry;
        }
    }
    pthread_mutex_unlock(&hasht[hashKey].lock);
}

/*
<Heap & heap sort implementation>
Reference: https://www.geeksforgeeks.org/insertion-and-deletion-in-heaps/ (GeeksforGeeks)
*/

static key_value **heap;
static int k;
static int heap_size;

// Initialize the Heap
void initHeap()
{
    heap = (key_value **)malloc(k * sizeof(struct key_value *));
    heap_size = 0;
}

// Function for basic Heap insertion operation
void heapInsert(key_value *node)
{
    key_value *temp;
    heap[heap_size] = node;
    int i = heap_size;
    while (smallerThan(heap[i], heap[(i - 1) / 2]) && i != 0)
    {
        temp = heap[i];
        heap[i] = heap[(i - 1) / 2];
        heap[(i - 1) / 2] = temp;
        i = (i - 1) / 2;
    }
    heap_size++;
}

// heapify function for Heap
// Reference: https://www.geeksforgeeks.org/min-heap-in-java/ (GeeksforGeeks)
void heapify(int i)
{
    key_value *temp;
    if (i < (heap_size - 1) / 2)
    {
        if (smallerThan(heap[2 * i + 1], heap[i]) || smallerThan(heap[2 * i + 2], heap[i]))
        {
            if (smallerThan(heap[2 * i + 1], heap[2 * i + 2]))
            {
                temp = heap[i];
                heap[i] = heap[2 * i + 1];
                heap[2 * i + 1] = temp;
                heapify(2 * i + 1);
            }
            else
            {
                temp = heap[i];
                heap[i] = heap[2 * i + 2];
                heap[2 * i + 2] = temp;
                heapify(2 * i + 2);
            }
        }
    }
}

void heapSortK()
{
    key_value *root_val;
    int init_size = heap_size;
    for (int i = 0; i < init_size; i++)
    {
        root_val = heap[0];
        heap[0] = heap[init_size - i - 1];
        heap_size--;
        heapify(0);
        heap[init_size - i - 1] = root_val;
    }
    heap_size = init_size;
}

// Put Record Into the Heap
// Reference: https://www.geeksforgeeks.org/k-largestor-smallest-elements-in-an-array/ (GeeksforGeeks)
void readRecordNPut()
{
    key_value *pointer = NULL;
    for (int i = 0; i < HASH_TABLE_SIZE; i++)
    {
        if (hasht[i].front != NULL)
        {
            pointer = hasht[i].front;
            do
            {

                if (heap_size < k)
                {
                    heapInsert(pointer);
                }
                else
                {
                    if (smallerThan(heap[0], pointer))
                    {
                        heap[0] = pointer;
                        heapify(0);
                    }
                }

                pointer = pointer->next;
            } while (pointer != NULL);
        }
    }
}

// Function that round the timestamp to the nearest hour
long convertRecord(char *timestamp)
{
    long decimal = strtol(timestamp, NULL, 10);
    return decimal - decimal % 3600;
}

// ----------------------------------------------------------------------------

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
			insert(convertRecord(buffer));

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

void processResults() {

    k = enrtyCount;
	initHeap();
    readRecordNPut();
    heapSortK();
	
}

void displayResults() {
	const char date_format[] = "%c";

	printf("Top K frequently accessed hour:\n");

	for (size_t i = 0; i < enrtyCount; i++)
	{
		time_t time_val = heap[i]->timestamp;
		struct tm *time = localtime(&time_val);

		char buffer[80];
		strftime(buffer, 80, date_format, time);

		printf("%s\t%d\n", buffer, heap[i]->count);
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

    initHashTable();

	readDir();

	processResults();

	displayResults();

  return 0;
}
