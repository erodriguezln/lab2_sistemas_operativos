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

int ceilDivision(int numerator, int divisor);
int getLineCount(const char *fileName);
unsigned int hashGenerator(char *key, int size);
HashTable *createHashTable(int size);
HashItem *createHashItem(char *key, int value);
HashItem *searchHashTable(HashTable *table, char *key);
void incrementOrInsertHashItem(HashTable *table, char *key, int value);
void freeHashItem(HashItem *item);
void freeHashTable(HashTable *table);
char **readFileContent(const char *fileName, int startLine, int endLine);
int compareHashItems(const void *a, const void *b);
void printSortedHashTable(HashTable *table);
void *countPlayerOccurrences(void *arg);

int main(int argc, char *argv[])
{
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

	pthread_mutex_init(&tableMutex, NULL);

	pthread_t *threads = malloc(N * sizeof(pthread_t));

	ThreadArgs *arrThreads = malloc(N * sizeof(ThreadArgs));

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

	for (size_t i = 0; i < N; i++)
	{
		pthread_create(&(threads[i]), NULL, countPlayerOccurrences, (void *)&(arrThreads[i]));
	}

	for (size_t i = 0; i < N; i++)
	{
		pthread_join(threads[i], NULL);
	}

	printSortedHashTable(table);

	free(threads);
	free(arrThreads);
	freeHashTable(table);

	pthread_mutex_destroy(&tableMutex);

	return EXIT_SUCCESS;
}

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

HashTable *createHashTable(int size)
{
	HashTable *table = malloc(sizeof(HashTable));

	table->items = calloc(size, sizeof(HashItem *));
	table->count = 0;
	table->size = size;

	return table;
}

HashItem *createHashItem(char *key, int value)
{
	HashItem *item = malloc(sizeof(HashItem));
	item->key = malloc((strlen(key) + 1) * sizeof(char));
	strcpy(item->key, key);
	item->value = value;
	item->next = NULL;

	return item;
}

HashItem *searchHashTable(HashTable *table, char *key)
{
	unsigned int index = hashGenerator(key, table->size);
	HashItem *current = table->items[index];
	while (current != NULL)
	{
		if (strcmp(current->key, key) == 0)
		{
			return current;
		}
		current = current->next;
	}

	return NULL;
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

void incrementOrInsertHashItem(HashTable *table, char *key, int value)
{
	pthread_mutex_lock(&tableMutex);

	unsigned int index = hashGenerator(key, table->size);
	HashItem *current = table->items[index];

	while (current != NULL)
	{
		// checks if the key already exists
		// if it does, increases the value in 1
		if (strcmp(current->key, key) == 0)
		{
			current->value += 1;
			pthread_mutex_unlock(&tableMutex);
			return;
		}
		current = current->next;
	}

	// Key doesn't exist so create a new item
	HashItem *newItem = createHashItem(key, value);
	newItem->next = table->items[index];

	table->items[index] = newItem;
	table->count++;

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

void freeHashItem(HashItem *item)
{
	free(item->key);
	free(item);
}

void freeHashTable(HashTable *table)
{
	for (size_t i = 0; i < table->size; i++)
	{
		HashItem *item = table->items[i];

		while (item != NULL)
		{
			HashItem *temp = item;
			item = item->next;
			freeHashItem(temp);
		}
	}

	free(table->items);
	free(table);
}

char **readFileContent(const char *fileName, int startLine, int endLine)
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
	int currentLine = 0;
	while (currentLine < startLine && fgets(buffer, sizeof(buffer), file))
	{
		currentLine++;
	}

	size_t i = 0;
	while (i < lineCount && fgets(buffer, sizeof(buffer), file))
	{
		// Picks only the player name without the comma before his name
		lines[i] = strdup(strrchr(buffer, ',') + 1);
		i++;
	}

	// Removes \r and \n
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

int getLineCount(const char *fileName)
{
	FILE *file = fopen(fileName, "r");
	if (file == NULL)
	{
		perror("Error opening file");
		return -1;
	}

	char buffer[1024];
	int lineCount = 0;

	while (fgets(buffer, sizeof(buffer), file))
	{
		lineCount++;
	}

	rewind(file);
	fclose(file);

	return lineCount;
}

void *countPlayerOccurrences(void *arg)
{
	ThreadArgs *p = (ThreadArgs *)arg;

	char **fileContent = readFileContent(p->fileName, p->start, p->end);
	for (size_t j = 0; j < p->end - p->start; j++)
	{
		incrementOrInsertHashItem(p->table, fileContent[j], 1);

		free(fileContent[j]);
	}

	free(fileContent);

	pthread_exit(NULL);
}

// qsort needs the void pointer type
int compareHashItems(const void *a, const void *b)
{
	SortableItem *itemA = (SortableItem *)a;
	SortableItem *itemB = (SortableItem *)b;
	return itemB->value - itemA->value;
}
