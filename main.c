/**
 * LAB 2 Paralelizacion con hebras: pthread y mutex en Premios MVP - UEFA Champions League 2023/24
 * Author: Enrique Rodriguez-Lapuente
 * USACH - Sistemas Operativos - 2025
 *
 * This program reads a file containing the matches of the champions league and
 * their MVP, it counts the occurrences of each MVP, and sorts them in
 * descending order.
 * It uses multiple threads to distribute up the counting process.
 *
 * Usage: ./program_name <file.txt> <num_threads>
 * Example: ./program_name partidos.txt 4
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

// Mutex to protect the shared hash table
// This prevents race conditions when multiple threads try to update player counts simultaneously
pthread_mutex_t tableMutex;

/**
 * Represents a single item in the hash table with key-value pair and chaining support.
 */
typedef struct HashItem
{
	char *key;			   // String key (player name)
	int value;			   // Count value (number of MVP awards)
	struct HashItem *next; // Pointer to next item (for collision chaining)
} HashItem;

/**
 * Represents a hash table with an array of pointers to HashItem structures.
 * The size is the capacity of the table, and count is the number of items in it.
 */
typedef struct HashTable
{
	HashItem **items; // Array of pointers to hash items
	size_t size;	  // Capacity of the hash table
	size_t count;	  // Number of items currently stored
} HashTable;

/**
 * Lightweight structure for sorting items extracted from the hash table.
 */
typedef struct SortableItem
{
	char *key; // Reference to original key in hash table
	int value; // Count value for sorting
} SortableItem;

/**
 * Arguments passed to each worker thread to define its work range.
 */
typedef struct ThreadArgs
{
	int tid;		  // Thread ID for identification
	char *fileName;	  // Input file to process
	size_t start;	  // Starting line in the file
	size_t end;		  // Ending line in the file
	HashTable *table; // Shared hash table reference
} ThreadArgs;

// Function prototypes
int countVisibleCharacters(const char *str);
int ceilDivision(int numerator, int divisor);
int getLineCountFromFile(const char *fileName);
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
		// Print message with instructions if the number of arguments is incorrect
		fprintf(stderr, "Uso: %s archivo.txt num_hebras\n", argv[0]);
		return EXIT_FAILURE;
	}

	// Extract command line arguments filename and number of threads
	char *fileName = argv[1];
	size_t numberOfThreads = atoi(argv[2]);

	// Count total lines in the input file to determine work distribution
	size_t lineCount = getLineCountFromFile(fileName);

	// Calculate how many lines each thread should process (rounded up)
	int chunkSize = ceilDivision(lineCount, numberOfThreads);

	// Create a hash table with a size equal to the number of lines in the file
	HashTable *table = createHashTable(lineCount);
	if (table == NULL)
	{
		fprintf(stderr, "Error creating hash table\n");
		return EXIT_FAILURE;
	}

	// Initialize the mutex that will be used to protect the shared hash table
	// This mutex will be used to ensure that only one thread can access the hash table at a time
	pthread_mutex_init(&tableMutex, NULL);

	// Dynamically allocate memory for an array of threads by the number provided by the user
	pthread_t *threads = malloc(numberOfThreads * sizeof(pthread_t));
	if (threads == NULL)
	{
		// if memory allocation fails, print an error message
		fprintf(stderr, "Error allocating memory for threads\n");
		// Free the hash table before exiting
		freeHashTable(table);
		return EXIT_FAILURE;
	}

	// Allocate memory for the thread parameters
	ThreadArgs *arrThreads = malloc(numberOfThreads * sizeof(ThreadArgs));
	if (arrThreads == NULL)
	{
		// if memory allocation fails, print an error message
		fprintf(stderr, "Error allocating memory for thread arguments\n");
		// Free the hash table and threads before exiting
		free(threads);
		freeHashTable(table);
		return EXIT_FAILURE;
	}

	// Distribute work among threads by assigning line ranges to each thread
	int start = 0;
	for (size_t i = 0; i < numberOfThreads; i++)
	{
		// Calculate end position for current thread's work chunk
		size_t end = start + chunkSize;
		// If the end index exceeds the total number of lines, set it to lineCount
		// This ensures that the last thread processes any remaining lines
		// This is important to avoid out-of-bounds access in the file reading function
		if (end > lineCount)
		{
			end = lineCount;
		}

		// Configure thread parameters: thread ID, file to process, line range, and shared table
		arrThreads[i].tid = i;
		arrThreads[i].fileName = fileName;
		arrThreads[i].start = start;
		arrThreads[i].end = end;
		arrThreads[i].table = table;

		// Update start position for next thread
		start = end;
	}

	// Create threads to count player occurrences in the file
	// Each thread will process a range of lines from the file
	for (size_t i = 0; i < numberOfThreads; i++)
	{
		pthread_create(&(threads[i]), NULL, countPlayerOccurrences, (void *)&(arrThreads[i]));
	}

	// Wait for all threads to complete their processing before continuing
	// This ensures all MVP data has been processed before printing results
	for (size_t i = 0; i < numberOfThreads; i++)
	{
		pthread_join(threads[i], NULL);
	}

	// Write the results to file report mvp.txt in sorted order
	printSortedHashTable(table);

	// Clean up resources
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
 * Creates and initializes a hash item for insertion into the hash table.
 * Each item is designed to be part of a linked list to handle hash collisions
 * through chaining.
 *
 * Collision handling is used because:
 * 1. Different player names may hash to the same index (hash collision)
 * 2. Our hash function has limited output range (table size) but unlimited input possibilities
 * 3. Without collision handling, newer players would overwrite previous ones with the same hash
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
	// The next pointer enables chaining for collision handling
	item->value = value;
	item->next = NULL;

	return item;
}

/**
 * Computes a hash value for a given string key using a polynomial rolling hash.
 * Multiplies the current hash by 31 and adds each character, applying modulo to stay within bounds.
 *
 * @param key  The input string key (player name).
 * @param size The size of the hash table.
 * @return     A hash value in the range [0, size - 1].
 */
unsigned int hashGenerator(char *key, int size)
{
	unsigned int hashValue = 0;
	for (size_t i = 0; key[i] != '\0'; i++)
	{
		// Multiply by 31, add current char, and keep within bounds with modulo
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

	// debug to check which thread is locking the mutex
	// printf("Thread %lu lock thread for key: %s\n", (unsigned long)pthread_self(), key);

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

			// Unlock the mutex before returning to allow other threads to access the table
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

	// Unlock the mutex before returning to allow other threads to access the table
	pthread_mutex_unlock(&tableMutex);
}

/**
 * Counts the number of visible characters in a UTF-8 encoded string
 * This function counts characters, not bytes, so it handles multi-byte UTF-8 characters correctly.
 *
 * @param str The string to count visible characters in
 * @return The number of visible characters in the string
 */
int countVisibleCharacters(const char *str)
{
	int count = 0;
	int i = 0;

	// Iterate through the string and count visible characters
	while (str[i] != '\0')
	{
		// Check if the current byte is the start of a multi-byte character
		if ((str[i] & 0xC0) != 0x80)
		{
			count++;
		}
		i++;
	}

	return count;
}

/**
 * Sorts the hash table items by value in descending order and writes the results to a file.
 * This function is called after all threads have completed adding players to the hash table.
 *
 * @param table Pointer to the hash table containing all player names and their MVP counts
 */
void printSortedHashTable(HashTable *table)
{

	// Allocate memory for an array to hold items for sorting
	// An array is needed because the hash table uses linked lists to handle collisions
	// And a linked list is not suitable for sorting
	SortableItem *sortedItems = malloc(table->count * sizeof(SortableItem));
	int itemIndex = 0;

	// Traverse the entire hash table and copy all items to the sortable array
	for (size_t i = 0; i < table->size; i++)
	{
		// Get the first item at this hash index
		HashItem *current = table->items[i];

		// Traverse the linked list at this hash position (handling hash collisions)
		// if current is NULL, it means there are no items at this index
		while (current != NULL)
		{
			// Shallow copies the key and value to the sortable array (no wasted memory)
			// This is safe because the key is not modified in the hash table
			// The value is only used for sorting
			// And they will be freed later
			sortedItems[itemIndex].key = current->key;
			sortedItems[itemIndex].value = current->value;
			itemIndex++;

			// Move to the next item in the linked list
			current = current->next;
		}
	}

	// Sort the array in descending order based on the MVP count using the qsort function
	// compareHashItems is the comparison function that enables sorting by value in descending order
	qsort(sortedItems, table->count, sizeof(SortableItem), compareHashItems);

	// Open a reporte mvp.txt for writing the sorted results
	FILE *fptr;
	fptr = fopen("reporte mvp.txt", "w");
	// Header for the report
	fprintf(fptr, "Jugador MVP%*s|\tPremios\n", 13, "");
	fprintf(fptr, "-----------------------------------\n");

	// Iterate through each sorted item and write it to the file
	for (size_t i = 0; i < table->count; i++)
	{
		// Create a buffer to store the key (player name) and ensure it is null-terminated
		char buffer[100] = {0};
		// Copy the key (player name) to the buffer
		strcpy(buffer, sortedItems[i].key);

		// Count the visible characters in the player name
		// This is important for UTF-8 strings with non-ASCII characters
		// to ensure proper alignment in the output
		int visibleCharacters = countVisibleCharacters(buffer);

		// Pad the player name with spaces to ensure all names have the same displayed width
		// This creates a uniform column width for better readability
		for (size_t j = visibleCharacters; j < 24; j++)
		{
			strcat(buffer, " ");
		}

		// Write the formatted player name and MVP count to the file
		fprintf(fptr, "%s|\t%d\n", buffer, sortedItems[i].value);
	}

	fclose(fptr);

	// Free the memory allocated for the sortable array
	// I don't free the individual keys because they are still owned by the hash table
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
int getLineCountFromFile(const char *fileName)
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

/**
 * Thread function to count player occurrences in a specific range of lines
 * This function is executed by each thread created in the main function.
 *
 * @param arg Pointer to the thread arguments (ThreadArgs structure)
 * @return NULL
 */
void *countPlayerOccurrences(void *arg)
{
	// Typecast the void pointer arg as a pointer to ThreadArgs
	// Necessary because pthread_create only accept functions with *void params
	// But we need our own specific structure (ThreadArgs)
	ThreadArgs *threadArgs = (ThreadArgs *)arg;

	// Get assigned line range
	size_t lineCount = threadArgs->end - threadArgs->start;

	// Extract player names from the specified range of lines in the file
	char **fileContent = extractMVPNamesFromFileRange(threadArgs->fileName, threadArgs->start, threadArgs->end);
	if (!fileContent)
	{
		fprintf(stderr, "Thread %d: Failed to read file content\n", threadArgs->tid);
		pthread_exit(NULL);
	}

	// Increment (if exists) or Insert in the hash table each mvp from the assigned range
	// The incrementOrInsertHashItem function handles mutex locking internally
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
 * Comparison function for qsort to arrange hash items in descending order by value.
 *
 * @param a First item to compare (as void pointer)
 * @param b Second item to compare (as void pointer)
 * @return Negative if b < a, positive if b > a (for descending sort)
 */
int compareHashItems(const void *a, const void *b)
{
	// Cast the generic void pointers to SortableItem pointers
	// This is necessary because qsort() uses void pointers for type-agnostic sorting
	SortableItem *itemA = (SortableItem *)a;
	SortableItem *itemB = (SortableItem *)b;

	// Subtract a from b to sort in descending order
	return itemB->value - itemA->value;
}
