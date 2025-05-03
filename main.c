/**
 * This program reads a file containing player names and their MVP awards,
 * counts the occurrences of each player, and sorts them in descending order.
 * It uses multiple threads to speed up the counting process.
 *
 * Usage: ./program_name file.txt num_threads
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

pthread_mutex_t tableMutex;

typedef struct HashItem
{
	char *key;
	int value;
	struct HashItem *next;
} HashItem;

typedef struct HashTable
{
	HashItem **items;
	size_t size;
	size_t count;
} HashTable;

typedef struct SortableItem
{
	char *key;
	int value;
} SortableItem;

typedef struct ThreadArgs
{
	int tid;
	char *fileName;
	size_t start;
	size_t end;
	HashTable *table;
} ThreadArgs;

// Function prototypes
int countVisibleCharacters(const char *str);
int ceilDivision(int numerator, int divisor);
int getLineCount(const char *fileName);
unsigned int hashGenerator(char *key, int size);
HashTable *createHashTable(int size);
HashItem *createHashItem(char *key, int value);
void incrementOrInsertHashItem(HashTable *table, char *key, int value);
void freeHashTable(HashTable *table);
char **extractMVPNamesFromFileRange(const char *fileName, int startLine, int endLine);
int compareHashItems(const void *a, const void *b);
void printSortedHashTable(HashTable *table);
void *countPlayerOccurrences(void *arg);

int main(int argc, char *argv[])
{
	// Check if the user provided the correct number of arguments
	if (argc != 3)
	{
		fprintf(stderr, "Uso: %s archivo.txt num_hebras\n", argv[0]);
		return EXIT_FAILURE;
	}

	char *fileName = argv[1];
	size_t N = atoi(argv[2]);
	// const char *fileName = "mvp_champions_23_24.txt";
	// int N = 5; // 3
	size_t lineCount = getLineCount(fileName);
	int chunkSize = ceilDivision(lineCount, N);

	HashTable *table = createHashTable(lineCount);
	if (table == NULL)
	{
		fprintf(stderr, "Error creating hash table\n");
		return EXIT_FAILURE;
	}

	// Initialize the mutex that will be used to protect the shared hash table
	pthread_mutex_init(&tableMutex, NULL);

	// Allocate memory for the threads by the number provided by the user
	pthread_t *threads = malloc(N * sizeof(pthread_t));
	if (threads == NULL)
	{
		fprintf(stderr, "Error allocating memory for threads\n");
		freeHashTable(table);
		return EXIT_FAILURE;
	}

	// Allocate memory for the thread arguments
	ThreadArgs *arrThreads = malloc(N * sizeof(ThreadArgs));
	if (arrThreads == NULL)
	{
		fprintf(stderr, "Error allocating memory for thread arguments\n");
		free(threads);
		freeHashTable(table);
		return EXIT_FAILURE;
	}

	int start = 0;
	for (size_t i = 0; i < N; i++)
	{
		size_t end = start + chunkSize;
		if (end > lineCount)
		{
			end = lineCount;
		}

		arrThreads[i].tid = i;
		arrThreads[i].fileName = fileName;
		arrThreads[i].start = start;
		arrThreads[i].end = end;
		arrThreads[i].table = table;
		start = end;
	}

	// Create threads to count player occurrences in the file
	// Each thread will process a portion of the file
	for (size_t i = 0; i < N; i++)
	{
		pthread_create(&(threads[i]), NULL, countPlayerOccurrences, (void *)&(arrThreads[i]));
	}

	// Wait for all threads to finish
	for (size_t i = 0; i < N; i++)
	{
		pthread_join(threads[i], NULL);
	}

	printSortedHashTable(table);

	free(threads);
	free(arrThreads);
	freeHashTable(table);

	// Destroy the mutex after all threads have finished
	pthread_mutex_destroy(&tableMutex);

	return EXIT_SUCCESS;
}

/**
 * Calculates the ceiling division of two integers (division rounded up).
 *
 * @param numerator The number to be divided
 * @param divisor The number to divide by
 * @return The result of ceiling division (rounded up to the nearest integer)
 */
int ceilDivision(int numerator, int divisor)
{
	if (numerator % divisor == 0)
	{
		return numerator / divisor;
	}
	else
	{
		return (numerator / divisor) + 1;
	}
}

/**
 * Creates and initializes a has table
 *
 * @param size The capacity of the hash table
 * @return Pointer to the newly created hash table or NULL if memory alloc fails
 */
HashTable *createHashTable(int size)
{
	// Allocate memory for the hash table
	HashTable *table = malloc(sizeof(HashTable));
	if (table == NULL)
	{
		perror("Failed to allocate memory for hash table");
		return NULL;
	}

	// Allocate and initializes the array of items pointers to NULL (calloc)
	table->items = calloc(size, sizeof(HashItem *));
	if (table->items == NULL)
	{
		perror("Failed to allocate memory for hash table items");
		free(table);
		return NULL;
	}

	// Initializes table properties
	table->count = 0;
	table->size = size;

	return table;
}

/**
 * Creates and initializes a hash item
 *
 * @param key The key of the hash item
 * @param value The value of the hash item
 * @return Pointer to the newly created hash item or NULL if memory alloc fails
 */
HashItem *createHashItem(char *key, int value)
{
	// Allocate memory for the hash item
	HashItem *item = malloc(sizeof(HashItem));
	if (item == NULL)
	{
		perror("Failed to allocate memory for hash item");
		return NULL;
	}

	// Allocate memory for the key string
	// The +1 is for the null terminator
	item->key = malloc((strlen(key) + 1) * sizeof(char));
	if (item->key == NULL)
	{
		perror("Failed to allocate memory for hash item key");
		free(item);
		return NULL;
	}

	// Copy the key string into the allocated memory
	strcpy(item->key, key);
	// Initialize the value and next pointer
	item->value = value;
	item->next = NULL;

	return item;
}

unsigned int hashGenerator(char *key, int size)
{
	unsigned int hashValue = 0;
	for (size_t i = 0; key[i] != '\0'; i++)
	{
		hashValue = (hashValue * 31 + key[i]) % size;
	}

	return hashValue;
}

/**
 * Increments the count for an existing key or inserts a new item in the hash table.
 * This function is thread-safe by using mutex locking and unlocking.
 *
 * @param table The hash table to modify
 * @param key The key to search for or insert
 * @param value The initial value for new items (existing items are incremented)
 */
void incrementOrInsertHashItem(HashTable *table, char *key, int value)
{
	// Lock mutex to protect the shared hash table
	// Prevents race conditions when multiple threads update the table
	pthread_mutex_lock(&tableMutex);

	// Calculates item index for this key
	unsigned int index = hashGenerator(key, table->size);

	// Search for the item in the hash table
	HashItem *current = table->items[index];

	// Traverse the linked list at this item to find matching key
	// This handles hash collisions by using chaining method
	while (current != NULL)
	{
		// check if the key already exists in the hash table
		if (strcmp(current->key, key) == 0)
		{
			// Key found, increment his value by 1
			current->value += 1;

			// Unlock the mutex before returning
			pthread_mutex_unlock(&tableMutex);
			return;
		}
		// Moves to the next item if they share the item index
		current = current->next;
	}

	// Key doesn't exist so create a new item with the provided key and value
	HashItem *newItem = createHashItem(key, value);
	// Insert the new item at the begining of the ll (chaining)
	newItem->next = table->items[index];
	table->items[index] = newItem;

	// Increments the total items on the hash table
	table->count++;

	// Unlock the mutex
	pthread_mutex_unlock(&tableMutex);
}

// TODO explain
int countVisibleCharacters(const char *str)
{
	int count = 0;
	int i = 0;

	while (str[i] != '\0')
	{
		if ((str[i] & 0xC0) != 0x80)
		{
			count++;
		}
		i++;
	}

	return count;
}

void printSortedHashTable(HashTable *table)
{

	SortableItem *sortedItems = malloc(table->count * sizeof(SortableItem));
	int itemIndex = 0;

	for (size_t i = 0; i < table->size; i++)
	{
		HashItem *current = table->items[i];
		while (current != NULL)
		{
			sortedItems[itemIndex].key = current->key;
			sortedItems[itemIndex].value = current->value;
			itemIndex++;
			current = current->next;
		}
	}

	qsort(sortedItems, table->count, sizeof(SortableItem), compareHashItems);

	FILE *fptr;
	fptr = fopen("reporte mvp.txt", "w");
	fprintf(fptr, "Jugador MVP%*s|\tPremios\n", 13, "");
	fprintf(fptr, "-----------------------------------\n");

	for (size_t i = 0; i < table->count; i++)
	{
		char buffer[100] = {0};
		strcpy(buffer, sortedItems[i].key);

		// count visible chars not bytes
		int visibleCharacters = countVisibleCharacters(buffer);

		for (size_t j = visibleCharacters; j < 24; j++)
		{
			strcat(buffer, " ");
		}

		fprintf(fptr, "%s|\t%d\n", buffer, sortedItems[i].value);
	}

	fclose(fptr);

	free(sortedItems);
}

/**
 * Frees the memory allocated for the hash table and its items
 *
 * @param table The hash table to free
 */
void freeHashTable(HashTable *table)
{
	if (table == NULL)
	{
		return;
	}

	// Free each item in the hash table
	for (size_t i = 0; i < table->size; i++)
	{
		HashItem *item = table->items[i];

		// Traverse the linked list and free each item
		// This handles hash collisions by using chaining method
		// This is the same method used in incrementOrInsertHashItem
		while (item != NULL)
		{
			// Save reference to current item before moving to next
			HashItem *temp = item;
			item = item->next;

			// Free the item key and the item itself
			free(temp->key);
			free(temp);
		}
	}
	// Free the array of item pointers and the hash table itself
	free(table->items);
	free(table);
}

/**
 * Extract player names from a specific range of lines from a file
 *
 * @param fileName Path to the file to be read
 * @param startLine Index of the first line to read
 * @param endLine Index of the last line to read
 * @return Array of strings containing the player names, or NULL if an error
 */
char **extractMVPNamesFromFileRange(const char *fileName, int startLine, int endLine)
{
	FILE *file = fopen(fileName, "r");
	if (file == NULL)
	{
		perror("Error opening file");
		return NULL;
	}

	char buffer[1024];
	size_t lineCount = endLine - startLine;

	char **lines = malloc(lineCount * sizeof(char *));

	// Skip lines before the start position
	int currentLine = 0;
	while (currentLine < startLine && fgets(buffer, sizeof(buffer), file))
	{
		currentLine++;
	}

	// Read the specified range of lines and extract player names
	size_t i = 0;
	while (i < lineCount && fgets(buffer, sizeof(buffer), file))
	{
		// Picks only the player name without the comma before his name
		lines[i] = strdup(strrchr(buffer, ',') + 1);
		i++;
	}

	// Removes \r and \n at the end of the name
	for (size_t j = 0; j < lineCount; j++)
	{
		if (lines[j])
		{
			lines[j][strcspn(lines[j], "\r\n")] = '\0';
		}
	}

	fclose(file);

	return lines;
}

/**
 * Counts the total lines in a file
 *
 * @param fileName Path to the file to be read
 * @return number of lines in the file, or -1 if an error ocurred
 */
int getLineCount(const char *fileName)
{
	FILE *file = fopen(fileName, "r");
	if (file == NULL)
	{
		perror("Error opening file");
		return -1;
	}

	// Char array where the line will be stored
	char buffer[1024];
	int lineCount = 0;

	// Reads every char until it finds \n and count them
	// Runs till it finds EOF
	while (fgets(buffer, sizeof(buffer), file))
	{
		lineCount++;
	}

	// Resets the file
	rewind(file);

	fclose(file);

	return lineCount;
}

void *countPlayerOccurrences(void *arg)
{
	// Typecast the void pointer arg as a pointer to ThreadArgs
	// Neccessary because pthread_create only accept functions with *void params
	// But we need our own specific structure (ThreadArgs)
	ThreadArgs *threadArgs = (ThreadArgs *)arg;

	// Get assigned line range
	size_t lineCount = threadArgs->end - threadArgs->start;

	// Get assigned portion of the file
	char **fileContent = extractMVPNamesFromFileRange(threadArgs->fileName, threadArgs->start, threadArgs->end);
	if (!fileContent)
	{
		fprintf(stderr, "Thread %d: Failed to read file content\n", threadArgs->tid);
		pthread_exit(NULL);
	}

	// Increment or Insert each player name in the assigned range
	for (size_t j = 0; j < lineCount; j++)
	{
		// Insert MVP on the HashTable or Update his value if it exists
		incrementOrInsertHashItem(threadArgs->table, fileContent[j], 1);

		// Free the line
		free(fileContent[j]);
	}

	// Free the array of lines corresponding to this range
	free(fileContent);

	// Terminates thread and return a void pointer as required by pthread API
	pthread_exit(NULL);
}

/**
 * Compares two hash items for sorting
 *
 * @param a Pointer to the first item
 * @param b Pointer to the second item
 * @return Negative if b's value < a's value, positive if b's value > a's value
 *         (Note: returns b-a, not a-b, to sort in descending order)
 */
int compareHashItems(const void *a, const void *b)
{
	// Cast the generic void pointers to SortableItem pointers
	// This is necessary because qsort() uses void pointers for type-agnostic sorting
	SortableItem *itemA = (SortableItem *)a;
	SortableItem *itemB = (SortableItem *)b;

	// Return the difference between values in descending order (b - a)
	return itemB->value - itemA->value;
}
