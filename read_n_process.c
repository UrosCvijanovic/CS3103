#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>

#define NO_OF_THREAD 4
#define HASH_TABLE_SIZE 100

//*Hash Table implementation using seperate chaining*
//Note: It hasn't been tested yet.

// A node containing timestamp and no. of occurance
typedef struct key_value
{
    long timestamp;
    long count;
    struct key_value *next;
} key_value;

// A node as a header of each linked list, containing a lock
typedef struct list_header
{
    key_value *front;
    pthread_mutex_t lock;
} list_header;

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

// Insert a record to the hash table
void *insert(long key)
{
    pthread_mutex_lock(&hasht[key % HASH_TABLE_SIZE].lock);
    if (hasht[key % HASH_TABLE_SIZE].front == NULL)
    {
        key_value *newEntry = (struct key_value *)malloc(sizeof(key_value));
        newEntry->timestamp = key;
        newEntry->count = 1;
        hasht[key % HASH_TABLE_SIZE].front = newEntry;
    }
    else
    {
        bool found = false;
        key_value *pointer = hasht[key % HASH_TABLE_SIZE].front;
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
            newEntry->next = hasht[key % HASH_TABLE_SIZE].front;
            hasht[key % HASH_TABLE_SIZE].front = newEntry;
        }
    }
    pthread_mutex_unlock(&hasht[key % HASH_TABLE_SIZE].lock);
}

// Function that round the timestamp to the nearest hour
long convertRecord(char *timestamp)
{
    long decimal = strtol(timestamp, NULL, 10);
    return decimal - decimal % 3600;
}

//return the file size, adopted from the demo code
long getLength(char *filePath)
{
    struct stat sb;
    stat(filePath, &sb);
    return sb.st_size;
}

// The argument list for individual thread
typedef struct tArg
{
    FILE *input;
    int start;
    int end;
} tArg;

// Read a segment of the file, then add it to the hash table
//Note: this code doesn't work yet

void *readAndProcess(void *args)
{
    tArg *arg = (tArg *)args;
    char timestamp[4096];
    fseek(arg->input, arg->start, SEEK_SET);
    int read = fread(timestamp, 1, 4096, arg->input);
    printf("%s\n",timestamp);
    /*
    while (read + arg->start < arg->end)
    {
        read += fread(timestamp, strlen(timestamp)+1, 1, arg->input);
        printf(timestamp);
    }
    */
}


//Note: some code are "commented out" for testing purpose.
int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        printf("Please provide at least 3 arguments \n");
        // return 0;
    }
    initHashTable();
    char *dataFilePath;
    // dataFilePath = argv[1];
    dataFilePath = "test.txt";

    FILE *data = fopen(dataFilePath, "r");
    int topK;

    // sscanf(argv[3], "%d", &topK);
    topK = 5;

    long fileLen = getLength(dataFilePath);
    long sizeForThread = fileLen / NO_OF_THREAD;
    pthread_t segmentReader[NO_OF_THREAD];
    tArg argList[NO_OF_THREAD];

    int fileStartPos = 0;
    for (int i = 0; i < NO_OF_THREAD; i++)
    {
        argList[i].input = fopen(dataFilePath, "r");
        argList[i].start = fileStartPos;
        argList[i].end = fileStartPos + sizeForThread;
        fileStartPos += sizeForThread;
        pthread_create(&segmentReader[i], NULL, readAndProcess, &argList[i]);
    }
    for (int i = 0; i < NO_OF_THREAD; i++)
    {
        pthread_join(segmentReader[i], NULL);
        fclose(argList[i].input);
    }

    fclose(data);
    return 0;
}