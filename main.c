/**
 * LAB 2 Paralelización con hebras: pthread y mutex en Premios MVP - UEFA Champions League 2023/24
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
// #include <unistd.h>
#include <pthread.h>

// Mutex to protect the shared hash table, to avoid race conditions
// when multiple threads try to update the player counts simultaneously
pthread_mutex_t tableMutex;

/* Represents a single item in the hash table with key-value pair and chaining support. */
typedef struct HashItem {
    char *key;               // String key (player name)
    int value;               // Count value (number of MVP awards)
    struct HashItem *next; // Pointer to next item (for collision chaining)
} HashItem;

/* Represents a hash table with an array of pointers to HashItem structures.
 * The size is the capacity of the table, and count is the number of items in it. */
typedef struct HashTable {
    HashItem **items;
    size_t size;
    size_t count;
} HashTable;

/* Struct to facilitate the sorting of mvps by their count. */
typedef struct SortableItem {
    char *key; // Reference to original key in hash table
    int value; // Count value for sorting
} SortableItem;

/* Arguments passed to each thread to define its work range. */
typedef struct ThreadData {
    int tid;          // Thread ID for identification
    char *fileName;      // Input file to process
    int startLine; // Starting line in the file
    int endLine;      // Ending line in the file
    HashTable *table; // Shared hash table reference
} ThreadData;

// Function prototypes
int countVisibleCharacters(const char *str);

int ceilDivision(int numerator, int divisor);

int getLineCountFromFile(const char *fileName);

unsigned int hashGenerator(char *key, int size);

HashTable *createHashTable(int size);

HashItem *createHashItem(char *key, int value);

void incrementOrInsertHashItem(HashTable *table, char *key, int value);

void freeHashTable(HashTable *table);

char **extractMVPNamesFromLineRange(const char *fileName, int startLine, int endLine);

int compareHashItems(const void *a, const void *b);

void writeReportOfPlayersSortedByMVPCount(HashTable *table);

void *countPlayerOccurrences(void *arg);

int main(int argc, char *argv[]) {
    // Check if the user provided the correct number of arguments
    if (argc != 3) {
        // Print message with instructions if the number of arguments is incorrect
        fprintf(stderr, "Uso: %s archivo.txt num_hebras\n", argv[0]);
        return EXIT_FAILURE;
    }

    // Extract command line arguments filename and number of threads
    char *fileName = argv[1];
    int numberOfThreads = atoi(argv[2]);
    if (numberOfThreads <= 0) {
        fprintf(stderr, "Error: threads number must be greater than 0.\n");
        return EXIT_FAILURE;
    }

    // Count total lines in the input file
    int lineCount = getLineCountFromFile(fileName);
    if (lineCount == -1) {
        fprintf(stderr, "Error while counting lines in the file.\n");
        return EXIT_FAILURE;
    }

    // Calculate how many lines each thread should process (rounded up)
    // ex: 125 lines and 3 threads = 125/3 => 41.66 => 42
    int linesPerThread = ceilDivision(lineCount, numberOfThreads);

    // Create a hash table with a size equal to the number of lines in the file
    HashTable *table = createHashTable(lineCount);
    if (table == NULL) {
        fprintf(stderr, "Error creating hash table.\n");
        return EXIT_FAILURE;
    }

    // Initialize mutex that will be used to ensure that only one thread can
    // access the hash table at a time protecting it
    pthread_mutex_init(&tableMutex, NULL);

    /* Dynamically allocate memory for an  array of pthread_t to store ids */
    pthread_t *threads = malloc(numberOfThreads * sizeof(pthread_t));
    if (threads == NULL) {
        fprintf(stderr, "Error allocating memory for threads.\n");
        freeHashTable(table);
        return EXIT_FAILURE;
    }

    /* Dynamically allocate memory for an array of data (ThreadData) for threads */
    ThreadData *threadData = malloc(numberOfThreads * sizeof(ThreadData));
    if (threadData == NULL) {
        fprintf(stderr, "Error allocating memory for threads data.\n");
        free(threads);
        freeHashTable(table);
        return EXIT_FAILURE;
    }

    /* Distribute work among threads by assigning a range of lines to each thread */
    int startLine = 0;
    for (int i = 0; i < numberOfThreads; i++) {
        // Calculate end position for current thread's work chunk
        int endLine = startLine + linesPerThread;
        // If the end index exceeds the total number of lines, set it to lineCount
        // Ex: 125 / 3 = 41.66 => 42 but 42 * 3 = 126 > 125 so we set it to 125
        // to avoid out of bounds access
        if (endLine > lineCount) {
            endLine = lineCount;
        }

        // Configure thread parameters: thread ID, file to process, line range, and shared table
        threadData[i].tid = i;
        threadData[i].fileName = fileName;
        threadData[i].startLine = startLine;
        threadData[i].endLine = endLine;
        threadData[i].table = table;

        // Update start position for next thread
        startLine = endLine;
    }

    // Create threads to count player occurrences in the file
    // Each thread will process a range of lines from the file
    for (int i = 0; i < numberOfThreads; i++) {
        pthread_create(&threads[i], NULL, countPlayerOccurrences, (void *) &threadData[i]);
    }

    // Wait for all threads to complete their processing before continuing
    // This ensures all MVP data has been processed before printing results
    for (int i = 0; i < numberOfThreads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Write the results to file report mvp.txt in sorted order
    writeReportOfPlayersSortedByMVPCount(table);

    // Clean up resources
    free(threads);
    free(threadData);
    freeHashTable(table);

    // Destroy the mutex after all threads have finished
    pthread_mutex_destroy(&tableMutex);

    return EXIT_SUCCESS;
}

/* Calculates the ceiling division of two integers (division rounded up) */
int ceilDivision(int numerator, int divisor) {
    if (numerator % divisor == 0) {
        return numerator / divisor;
    } else {
        return (numerator / divisor) + 1;
    }
}

/* Creates an initializes a hash table with capacity defined by the given size
 * Memory is zero initialized
 * Returns a pointer to the table or NULL if it fails */
HashTable *createHashTable(int size) {
    // Allocate memory for the hash table
    HashTable *table = malloc(sizeof(HashTable));
    if (table == NULL) {
        perror("Failed to allocate memory for hash table.");
        return NULL;
    }

    // Allocate and zero initializes the array of items pointers
    table->items = calloc(size, sizeof(HashItem *));
    if (table->items == NULL) {
        perror("Failed to allocate memory for hash table items.");
        free(table);
        return NULL;
    }

    // Initializes table properties
    table->count = 0;
    table->size = size;

    return table;
}

/* Creates and initializes a hash item for insertion into the hash table.
 * Items use a pseudo linked list to handle collisions.
 *
 * Collision handling is used because:
 * 1. Different player names may hash to the same index (hash collision)
 * 2. Collision handling avoid newer players overwriting previous ones
 * with the same hash
 */
HashItem *createHashItem(char *key, int value) {
    // Allocate memory for the hash item
    HashItem *item = malloc(sizeof(HashItem));
    if (item == NULL) {
        perror("Failed to allocate memory for hash item.");
        return NULL;
    }

    // Copy the key string into the allocated memory and append null terminator
    item->key = strdup(key);
    if (item->key == NULL) {
        perror("Failed to allocate memory for hash item key.");
        free(item);
        return NULL;
    }

    // Set initial value and next pointer for collision handling
    item->value = value;
    item->next = NULL;

    return item;
}

/* Generate a hash value for a given key using a polynomial rolling hash. */
unsigned int hashGenerator(char *key, int size) {
    unsigned int hashValue = 0;
    for (size_t i = 0; key[i] != '\0'; i++) {
        // Multiply by 31, add current char, and keep within bounds with modulo
        hashValue = (hashValue * 31 + key[i]) % size;
    }

    return hashValue;
}

/* Increments count for an existing key or inserts new item in the hash table.
 * This function is thread-safe by using mutex locking and unlocking. */
void incrementOrInsertHashItem(HashTable *table, char *key, int value) {
    // Lock mutex to protect the shared hash table
    // Prevents race conditions when multiple threads update the table
    pthread_mutex_lock(&tableMutex);

    // debug to check which thread is locking the mutex
     printf("Thread %lu lock thread for key: %s\n", (unsigned long)pthread_self(), key);

    // Calculates item index for this key
    unsigned int index = hashGenerator(key, table->size);

    // Search for the item in the hash table
    HashItem *current = table->items[index];

    // Traverse the linked list at this item to find matching key
    // This handles hash collisions by using chaining method
    while (current != NULL) {
        // check if the key already exists in the hash table
        if (strcmp(current->key, key) == 0) {
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
    // TODO implement error checking, should atleast print something and return
    HashItem *newItem = createHashItem(key, value);

    // Insert the new item at the beginning of the ll (chaining)
    newItem->next = table->items[index];
    table->items[index] = newItem;

    // Increments the total items on the hash table
    table->count++;

    // Unlock the mutex before returning to allow other threads to access the table
    pthread_mutex_unlock(&tableMutex);
}

/**
 * Counts the number of visible characters in a UTF-8 encoded string
 * This function counts characters, not bytes, it handles multi-byte characters
 * correctly to avoid overflowing the columns in the report. (happens with
 * characters like ñ, á, é, ü, etc.)
 *
 * @param str The string to count visible characters in
 * @return The number of visible characters in the string
 */
int countVisibleCharacters(const char *str) {
    int count = 0;
    int i = 0;

    // Iterate through the string and count visible characters
    while (str[i] != '\0') {
        // Check if the current byte is the start of a multibyte character
        if ((str[i] & 0xC0) != 0x80) {
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
void writeReportOfPlayersSortedByMVPCount(HashTable *table) {

    // Allocate memory for an array to hold items for sorting
    // An array is needed because the hash table uses linked lists to handle collisions
    // And a linked list is not suitable for sorting
    SortableItem *sortedItems = malloc(table->count * sizeof(SortableItem));
    int itemIndex = 0;

    // Traverse the entire hash table and copy all items to the sortable array
    for (size_t i = 0; i < table->size; i++) {
        // Get the first item at this hash index
        HashItem *current = table->items[i];

        // Traverse the linked list at this hash position (handling hash collisions)
        // if current is NULL, it means there are no items at this index
        while (current != NULL) {
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

    // Open a reporte_mvp.txt for writing the sorted results
    FILE *fptr;
    fptr = fopen("reporte_mvp.txt", "w");
    if (fptr == NULL) {
        perror("Error creating report file");
        // Free the memory allocated for the sortable array
        free(sortedItems);
        return;
    }
    // Header for the report
    fprintf(fptr, "Jugador MVP%*s|\tPremios\n", 13, "");
    fprintf(fptr, "-----------------------------------\n");

    // Iterate through each sorted item and write it to the file
    for (size_t i = 0; i < table->count; i++) {
        // Create a buffer to store the key (player name) and ensure it is null-terminated
        char buffer[100] = {0};
        // Copy the key (player name) to the buffer
        strcpy(buffer, sortedItems[i].key);

        // Count the visible characters in the player name
        // This is useful for UTF-8 strings with non-ASCII characters
        // to avoid moving the columns in the report.
        int visibleCharacters = countVisibleCharacters(buffer);

        // Pad the player name with spaces to ensure all names have the same displayed width
        // This creates a uniform column width for better readability
        for (size_t j = visibleCharacters; j < 24; j++) {
            strcat(buffer, " ");
        }

        // Write the formatted player name and MVP count to the file
        fprintf(fptr, "%s|\t%d\n", buffer, sortedItems[i].value);
    }

    fclose(fptr);

    // Free the memory allocated for the sortable array
    free(sortedItems);
}

/* Frees the memory allocated for the hash table and its items */
void freeHashTable(HashTable *table) {
    if (table == NULL) {
        return;
    }

    // Free each item in the hash table
    for (size_t i = 0; i < table->size; i++) {
        HashItem *item = table->items[i];

        // Traverse the linked list and free each item
        // This handles hash collisions by using chaining method
        // This is the same method used in incrementOrInsertHashItem
        while (item != NULL) {
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
char **extractMVPNamesFromLineRange(const char *fileName, int startLine, int endLine) {
    FILE *file = fopen(fileName, "r");
    if (file == NULL) {
        perror("Error opening file");
        return NULL;
    }

    char buffer[1024];
    size_t rangeOfLines = endLine - startLine;

    // Allocate memory for an array of strings to store player names
    char **playerNames = malloc(rangeOfLines * sizeof(char *));
    if (playerNames == NULL) {
        perror("Error allocating memory for array of player names");
        fclose(file);
        return NULL;
    }

    // Skip lines before the start position
    int currentLine = 0;
    while (currentLine < startLine && fgets(buffer, sizeof(buffer), file)) {
        currentLine++;
    }

    // Read the specified range of lines and extract player names
    size_t i = 0;
    while (i < rangeOfLines && fgets(buffer, sizeof(buffer), file)) {
        // Picks only the player name without the comma before his name
        playerNames[i] = strdup(strrchr(buffer, ',') + 1);
        i++;
    }

    // Navigate each line(player name) char by char and replace \r or \n with \0
    for (size_t j = 0; j < rangeOfLines; j++) {
        if (playerNames[j]) {
            // find \r or \n and replace it with \0
            // ex: "Player Name\n" => "Player Name\0"
            playerNames[j][strcspn(playerNames[j], "\r\n")] = '\0';
        }
    }

    fclose(file);

    return playerNames;
}

/* Counts the total lines in a file, return -1 on error */
int getLineCountFromFile(const char *fileName) {
    FILE *file = fopen(fileName, "r");
    if (file == NULL) {
        perror("Error opening file");
        return -1;
    }

    char buffer[1024];
    int lineCount = 0;

    // Count lines until EOF
    while (fgets(buffer, sizeof(buffer), file)) {
        lineCount++;
    }

    fclose(file);

    return lineCount;
}

/**
 * Thread function to count player occurrences in a specific range of lines
 * This function is executed by each thread created in the main function.
 *
 * @param arg Pointer to the thread arguments (ThreadData structure)
 * @return NULL
 */
void *countPlayerOccurrences(void *arg) {
    // Typecast the void pointer arg as a pointer to ThreadData
    // Necessary because pthread_create only accept functions with *void params
    // This arg corresponds to the ThreadData structure passed while using pthread_create (the last param)
    ThreadData *threadData = (ThreadData *) arg;

    // Get assigned line range
    size_t lineCount = threadData->endLine - threadData->startLine;

    // Extract player names from the specified range of lines in the file
    char **fileContent = extractMVPNamesFromLineRange(threadData->fileName, threadData->startLine, threadData->endLine);
    if (!fileContent) {
        fprintf(stderr, "Thread %d: Failed to read file content\n", threadData->tid);
        pthread_exit(NULL);
    }

    // Increment (if exists) or Insert in the hash table each mvp from the assigned range
    // The incrementOrInsertHashItem function handles mutex locking internally
    for (size_t j = 0; j < lineCount; j++) {
        // Insert MVP on the HashTable or Update his value if it exists
        incrementOrInsertHashItem(threadData->table, fileContent[j], 1);

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
int compareHashItems(const void *a, const void *b) {
    // Cast the generic void pointers to SortableItem pointers
    // This is necessary because qsort() uses void pointers for type-agnostic sorting
    SortableItem *itemA = (SortableItem *) a;
    SortableItem *itemB = (SortableItem *) b;

    // Subtract a from b to sort in descending order
    return itemB->value - itemA->value;
}