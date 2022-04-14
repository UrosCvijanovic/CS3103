#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>

#define NO_OF_THREAD 4
#define HASH_TABLE_SIZE 1000

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
// This code doesn't work properly yet. Possible reason: I took the implemntation
// of Max heap and modify it, but it is not implemented correctly

void heapify(int i)
{
    int min = i;
    key_value *temp;
    if (2 * i + 1 < heap_size && smallerThan(heap[2 * i + 1],heap[min]))
    {
        min = 2 * i + 1;
    }
    else if (2 * i + 2 < heap_size && smallerThan(heap[2 * i + 2],heap[min]))
    {
        min = 2 * i + 2;
    }
    if (min != i)
    {
        temp = heap[i];
        heap[i] = heap[min];
        heap[min] = temp;
        heapify(min);
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

// main function to test hash table & heap sort
int main(int argc, char *argv[])
{
    initHashTable();
    char *filename[50];
    int topk;
    printf("Input file: ");
    scanf("%s", filename); // manually input the file path (textfile.txt)
    FILE *input = fopen(filename, "r");

    printf("Input value for K: ");
    scanf("%d", &topk);
    k = topk;

    printf("Reading the file...\n");

    char *line[50];
    while (fgets(line, 50, input))
    {
        insert(convertRecord(line));
    }
    printf("Building the heap...\n");
    initHeap();
    readRecordNPut();

    printf("Heap sort...\n");
    heapSortK();
    // print heap content
    for (int i = 0; i < heap_size; i++)
    {
        printf("[(%d) %ld : %ld]\n", i, heap[i]->timestamp, heap[i]->count);
    }
}
