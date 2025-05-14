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
#include <pthread.h>

// Mutex to protect the shared hash table, to avoid race conditions
pthread_mutex_t tableMutex;

/* Represents a single item in the hash table */
typedef struct HashItem {
    char *key;  // String key (player name)
    int value;  // Count value (number of MVP awards)
    struct HashItem *next;  // Pointer to next item (for collision chaining)
} HashItem;

/* Represents a hash table with an array of pointers to HashItem structures.
 * Size is the capacity of the table, and count is the number of items in it. */
typedef struct HashTable {
    HashItem **items;
    size_t size;
    size_t count;
} HashTable;

/* Struct to facilitate the sorting of MVP by their count. */
typedef struct SortableItem {
    char *key;  // Reference to original key in hash table
    int value;  // Count value for sorting
} SortableItem;

/* Parameters passed to each thread to define its work range. */
typedef struct ThreadData {
    int tid;    // Thread ID for identification
    char *fileName; // Input file to process
    int startLine;  // Starting line in the file
    int endLine;    // Ending line in the file
    HashTable *table;   // Shared hash table reference
} ThreadData;

// Function forward declarations
int countVisibleCharacters(const char *str);

int ceilDivision(int numerator, int divisor);

int getLineCountFromFile(const char *fileName);

unsigned int hashGenerator(char *key, int size);

HashTable *createHashTable(int size);

HashItem *createHashItem(char *key, int value);

void incrementOrInsertHashItem(HashTable *table, char *key, int value);

void freeHashTable(HashTable *table);

char **extractMVPNamesFromLineRange(const char *fileName, int startLine, int endLine);

int compareByMVPCounts(const void *a, const void *b);

int writeReportOfPlayersSortedByMVPCount(HashTable *table);

void *countPlayerOccurrences(void *arg);

int main(int argc, char *argv[]) {
    // Check if the user provided the correct number of arguments
    if (argc != 3) {
        // Print message with instructions if the number of arguments is incorrect
        fprintf(stderr, "Usage: %s archivo.txt num_hebras\n", argv[0]);
        return EXIT_FAILURE;
    }

    // Parse input parameters: filename and number of threads
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
    // ex: 125 lines and 3 threads = 125/3 => 41.666 => 42
    int linesPerThread = ceilDivision(lineCount, numberOfThreads);

    // Create a hash table with a size equal to the number of lines in the file
    HashTable *table = createHashTable(lineCount);
    if (table == NULL) {
        fprintf(stderr, "Error creating hash table.\n");
        return EXIT_FAILURE;
    }

    // Initialize mutex that will be used to ensure that only one thread can
    // access the hash table at a time preventing race condition
    pthread_mutex_init(&tableMutex, NULL);

    // Dynamically allocate memory for an  array of pthread_t to store ids
    pthread_t *threads = malloc(numberOfThreads * sizeof(pthread_t));
    if (threads == NULL) {
        fprintf(stderr, "Error allocating memory for threads.\n");
        freeHashTable(table);
        return EXIT_FAILURE;
    }

    // Dynamically allocate memory for an array of data (ThreadData) for threads
    ThreadData *threadData = malloc(numberOfThreads * sizeof(ThreadData));
    if (threadData == NULL) {
        fprintf(stderr, "Error allocating memory for threads data.\n");
        free(threads);
        freeHashTable(table);
        return EXIT_FAILURE;
    }

    // Distribute work among threads by assigning a range of lines to each thread
    int startLine = 0;
    for (int i = 0; i < numberOfThreads; i++) {
        // Calculate end position for current thread's work chunk
        int endLine = startLine + linesPerThread;

        // If the end line exceeds the total number of lines, set it to lineCount
        // Ex: 125 / 3 = 41.66 => 42 but 42 * 3 = 126 > 125 so we set it to 125
        // to avoid out of bounds access
        if (endLine > lineCount) {
            endLine = lineCount;
        }

        // Configure thread parameters
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
    int report = writeReportOfPlayersSortedByMVPCount(table);
    if (report == -1) {
        fprintf(stderr, "Error while writing the sorted report.\n");
        free(threads);
        free(threadData);
        freeHashTable(table);
        return EXIT_FAILURE;
    }

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
 * Returns a pointer to the table or NULL if it fails */
HashTable *createHashTable(int size) {
    // Allocate memory for the table structure
    HashTable *table = malloc(sizeof(HashTable));
    if (table == NULL) {
        perror("Failed to allocate memory for hash table.");
        return NULL;
    }

    // Create a zero-initialized items array
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
 * Handles collision by including a next pointer to chain items
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

    // Set initial value (mvp count) and next pointer for collision handling
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
    // Calculates item index for this key
    unsigned int index = hashGenerator(key, table->size);

    // Lock mutex to prevent race conditions on the shared hash table
    // Lock the entire table since it has to look for the key and navigate
    // the chain, determine if it should increment or add another item
    // all of this has to be done in a single lock since is an atomic operation
    pthread_mutex_lock(&tableMutex);

    // Search for the key
    HashItem *current = table->items[index];
    while (current != NULL) {
        // check if the key already exists in the hash table
        if (strcmp(current->key, key) == 0) {
            // Key found, increment his value by 1
            current->value += 1;

            // Unlock the mutex to allow other threads to access the table
            pthread_mutex_unlock(&tableMutex);
            return;
        }
        // Moves to the next item in chain
        current = current->next;
    }

    // Key doesn't exist so create a new item with the provided key and value
    HashItem *newItem = createHashItem(key, value);
    if (newItem == NULL) {
        perror("Failed to create a hash item.");
        pthread_mutex_unlock(&tableMutex);
        return;
    }

    // Insert the new item at the beginning of the collision chaiun
    newItem->next = table->items[index];
    table->items[index] = newItem;
    table->count++;

    // Unlock the mutex to allow other threads to access the table
    pthread_mutex_unlock(&tableMutex);
}

/* Counts visible UTF-8 characters (not bytes) so it handles multibyte chars
 * to avoid displacing the columns in the report. (happens with
 * characters like ñ, á, é, ü, etc.) */
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

/* Writes a report of players sorted by their MVP counts (descending)
 * return 0 on success or -1 on error */
int writeReportOfPlayersSortedByMVPCount(HashTable *table) {

    // Allocate memory for an array to hold items for sorting
    SortableItem *sortedItems = malloc(table->count * sizeof(SortableItem));
    if (sortedItems == NULL) {
        return -1;
    }


    // Copy all hash table entries to the sortable array (including collisions)
    int itemIndex = 0;
    for (size_t i = 0; i < table->size; i++) {
        HashItem *current = table->items[i];

        // Process all entries in this hash item collision chain
        while (current != NULL) {
            sortedItems[itemIndex].key = current->key;
            sortedItems[itemIndex].value = current->value;
            itemIndex++;

            // Move to the next item in the collision chain
            current = current->next;
        }
    }

    // Sort by MVP count (descending)
    qsort(sortedItems, table->count, sizeof(SortableItem), compareByMVPCounts);

    // Write the sorted result to reporte_mvp.txt
    FILE *fptr;
    fptr = fopen("reporte_mvp.txt", "w");
    if (fptr == NULL) {
        perror("Error creating report file");
        free(sortedItems);
        return -1;
    }

    // Report header
    fprintf(fptr, "Jugador MVP%*s|\tPremios\n", 13, "");
    fprintf(fptr, "-----------------------------------\n");

    // Write each entry procuring aligned columns
    for (size_t i = 0; i < table->count; i++) {
        char buffer[100] = {0};
        strcpy(buffer, sortedItems[i].key);

        // Count the visible characters (not bytes) in the player name
        int visibleCharacters = countVisibleCharacters(buffer);

        // Pad player name with spaces so all names have the same display width
        for (size_t j = visibleCharacters; j < 24; j++) {
            strcat(buffer, " ");
        }

        fprintf(fptr, "%s|\t%d\n", buffer, sortedItems[i].value);
    }

    fclose(fptr);
    free(sortedItems);

    return 0;
}

/* Frees the memory allocated for the hash table and its items */
void freeHashTable(HashTable *table) {
    if (table == NULL) {
        return;
    }

    // Free each item in the hash table (including collisions)
    for (size_t i = 0; i < table->size; i++) {
        HashItem *item = table->items[i];

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

/* Extract player names from a specific range of lines in a file
 * returns an array of player names (strings) or NULL on error */
char **extractMVPNamesFromLineRange(const char *fileName, int startLine, int endLine) {
    FILE *file = fopen(fileName, "r");
    if (file == NULL) {
        perror("Error opening file");
        return NULL;
    }

    char buffer[1024];
    size_t numLinesInRange = endLine - startLine;

    // Allocate memory for an array of strings to store player names
    char **playerNames = malloc(numLinesInRange * sizeof(char *));
    if (playerNames == NULL) {
        perror("Error allocating memory for array of player names");
        fclose(file);
        return NULL;
    }

    // Skip lines before the start line
    int currentLine = 0;
    while (currentLine < startLine && fgets(buffer, sizeof(buffer), file)) {
        currentLine++;
    }

    // Extract player names from the specified range of lines
    size_t i = 0;
    while (i < numLinesInRange && fgets(buffer, sizeof(buffer), file)) {
        // Picks only the player name without the comma before his name
        playerNames[i] = strdup(strrchr(buffer, ',') + 1);
        i++;
    }

    // Trim trailing whitespace from the players names
    for (size_t j = 0; j < numLinesInRange; j++) {
        if (playerNames[j]) {
            // find \r or \n and replace it with \0
            // ex: "Player Name\n" => "Player Name\0"
            playerNames[j][strcspn(playerNames[j], "\r\n")] = '\0';
        }
    }

    fclose(file);

    return playerNames;
}

/* Counts the total lines in a file, return line count or -1 on error */
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

/** Thread function to count player occurrences in a specific range of lines */
void *countPlayerOccurrences(void *arg) {
    // Cast void* arg to ThreadData*, required because pthread_create passes
    // arguments as void*
    ThreadData *threadData = (ThreadData *) arg;

    size_t numLinesInRange = threadData->endLine - threadData->startLine;

    // Extract player names from the specified range of lines in the file
    char **playerNames = extractMVPNamesFromLineRange(threadData->fileName, threadData->startLine, threadData->endLine);
    if (!playerNames) {
        fprintf(stderr, "Thread %d: Failed to read file content\n", threadData->tid);
        pthread_exit(NULL);
    }

    // Process each player name in range
    // incrementOrInsertHashItem manages thge mutex to prevent race conditions
    for (size_t i = 0; i < numLinesInRange; i++) {
        incrementOrInsertHashItem(threadData->table, playerNames[i], 1);
        free(playerNames[i]);
    }

    free(playerNames);

    // Terminates thread and return a void pointer as required by pthread API
    pthread_exit(NULL);
}

/* Comparison function for qsort to sort players in descending order by mvp count
 * Return negative if b < a, positive if b > a or 0 for equal */
int compareByMVPCounts(const void *a, const void *b) {
    // Cast the generic void pointers to SortableItem pointers
    // This is necessary because qsort() uses void pointers
    SortableItem *itemA = (SortableItem *) a;
    SortableItem *itemB = (SortableItem *) b;

    // Subtract a from b to sort in descending order
    return itemB->value - itemA->value;
}