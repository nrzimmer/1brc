#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#define Z_UTILS_IMPLEMENTATION
#include "zutils.h"

#define TABLE_SIZE 16384
#define WORKERS 17

typedef struct City {
    char *key;
    int min;
    int max;
    long mean;
    size_t count;
} City;

typedef struct {
    City *items;
    size_t count;
    size_t capacity;
} Bucket;

typedef struct {
    Bucket items[TABLE_SIZE];
} HashTable;

typedef struct {
    char *items;
    size_t count;
    size_t capacity;
} StringBuffer;

static char buff[256];

void print_city(const City *city, StringBuffer *output) {
    const double mean = (double) city->mean / (double) (city->count * 10);
    const double min = (double) city->min / 10.0f;
    const double max = (double) city->max / 10.0f;
    sprintf(buff, "%s=%.1f/%.1f/%.1f, ", city->key, min, mean, max);
    z_da_append_cstr(output, buff);
}

#define hash_base 5381

size_t table_index(const char *str, const size_t len) {
    unsigned long hash = hash_base;
    for (size_t i = 0; i < len; i++) {
        const int hash_char = *str++;
        hash = ((hash << 5) + hash) + hash_char;
    }
    return hash & (TABLE_SIZE - 1);
}

static int num_cities = 0;

Bucket *get_bucket(HashTable *hash_table, const char *key, const size_t len) {
    const size_t index = table_index(key, len);
    return &hash_table->items[index];
}

void add(HashTable *hash_table, const char *key, const size_t len, const int value) {
    Bucket *s = get_bucket(hash_table, key, len);
    for (City *it = s->items; it < s->items + s->count; ++it) {
        if (strncmp(it->key, key, len) == 0) {
            if (it->min > value) {
                it->min = value;
            }
            if (it->max < value) {
                it->max = value;
            }
            it->mean += value;
            it->count++;
            return;
        }
    }

    const City new = {.key = strndup(key, len), .count = 1, .min = value, .max = value, .mean = value};
    z_da_append(s, new);
}

void join_entry(Bucket *hash_table, const City *src) {
    const size_t len = strlen(src->key);
    const char *key = src->key;
    const size_t index = table_index(key, len);
    Bucket *s = &hash_table[index];
    for (City *it = s->items; it < s->items + s->count; ++it) {
        if (strcmp(it->key, key) == 0) {
            if (it->min > src->min) {
                it->min = src->min;
            }
            if (it->max < src->max) {
                it->max = src->max;
            }
            it->mean += src->mean;
            it->count += src->count;
            return;
        }
    }
    num_cities++;
    const City new = {.key = strdup(key), .count = src->count, .min = src->min, .max = src->max, .mean = src->mean};
    z_da_append(s, new);
}

Bucket *join_tables(HashTable **tables, const size_t count) {
    Bucket *result = calloc(TABLE_SIZE, sizeof(Bucket));
    for (size_t i = 0; i < count; i++) {
        HashTable *hash_table = tables[i];
        Bucket *buckets = hash_table->items;
        for (size_t j = 0; j < TABLE_SIZE; j++) {
            Bucket *bucket = &buckets[j];
            const City *cities = bucket->items;
            for (size_t k = 0; k < bucket->count; k++) {
                join_entry(result, &cities[k]);
                free(cities[k].key);
            }
            z_da_free(bucket);
        }
        free(hash_table);
    }
    return result;
}

inline void rewind_file(FILE *in, const size_t start, const size_t end) {
    const size_t diff = end - start;
    fseek(in, -(long int) diff, SEEK_CUR);
}

int comp_func(const void *a, const void *b) {
    const City *city_a = *(City **) a;
    const City *city_b = *(City **) b;
    return strcmp(city_a->key, city_b->key);
}

void extract_key_value_pairs(HashTable *hash_table, size_t start, size_t end, const char *buffer,
                             size_t upper_boundary) {
    size_t len = 0;
    if (start > 0) {
        while (buffer[start] != '\n') {
            start++;
        }
        start++;
        end = start;
    }
    while (true) {
        // Find key
        while (buffer[end] != ';') {
            end++;
        }
        const char *key = &buffer[start];
        len = end - start;

        start = end + 1;
        end = start;

        //find value
        bool neg = false;
        int value = 0;
        while (buffer[end] != '\n') {
            if (buffer[end] == '-') {
                neg = true;
                end++;
                continue;
            }
            if (buffer[end] != '.') {
                value = value * 10 + buffer[end] - '0';
            }
            end++;
        }

        if (neg) {
            value = -value;
        }
        add(hash_table, key, len, value);
        start = end + 1;
        end = start;
        if (end >= upper_boundary) {
            break;
        }
    }
}

typedef struct {
    size_t start;
    size_t end;
    char *buffer;
    size_t upper_boundary;
} WorkerArgs;

void *worker(void *arg) {
    HashTable *hash_table = calloc(1, sizeof(HashTable));
    const WorkerArgs *args = (WorkerArgs *) arg;
    extract_key_value_pairs(hash_table, args->start, args->end, args->buffer, args->upper_boundary);
    return hash_table;
}

int usage(char **argv) {
    printf("Usage: %s <file>\n", argv[0]);
    exit(1);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        return usage(argv);
    }
    FILE *in = fopen(argv[1], "r");
    if (in == NULL) {
        printf("File not found: %s\n", argv[1]);
        exit(1);
    }

    fseek(in, 0, SEEK_END);
    const size_t file_end = ftell(in);
    fseek(in, 0, SEEK_SET);
    size_t read = 0;
    char *buffer = malloc(file_end + 1);

    read = fread(buffer, sizeof(char), file_end, in);
    buffer[file_end] = '\0';

    if (read <= 0) {
        exit(1);
    }

    const unsigned long long t_total_start = now_ns();

    WorkerArgs args[WORKERS];
    pthread_t *threads[WORKERS];
    for (int i = 0; i < WORKERS; i++) {
        args[i].start = file_end / WORKERS * (i);
        args[i].end = args[i].start;
        args[i].buffer = buffer;
        args[i].upper_boundary = file_end / WORKERS * (i + 1);
        threads[i] = z_create_thread(worker, &args[i]);
    }

    HashTable **tables = malloc(WORKERS * sizeof(HashTable *));
    assert(tables != NULL);
    for (int i = 0; i < WORKERS; i++) {
        tables[i] = z_join_thread(threads[i]);
        z_free_thread(threads[i]);
    }
    free(buffer);

    Bucket *table = join_tables(tables, WORKERS);
    free(tables);

    City *list[num_cities];

    int index = 0;
    for (size_t i = 0; i < TABLE_SIZE; i++) {
        const Bucket *arr = &table[i];
        if (arr->count > 0) {
            for (City *it = arr->items; it < arr->items + arr->count; ++it) {
                list[index++] = it;
            }
        }
    }
    qsort(list, num_cities, sizeof(list[0]), comp_func);
    StringBuffer output = {0};
    z_da_reserve(&output, TABLE_SIZE);
    for (int i = 0; i < num_cities; i++) {
        print_city(list[i], &output);
        free(list[i]->key);
    }


    output.count -= 2;
    z_da_append(&output, '\0');
    printf("{%s}\n", output.items);
    z_da_free(&output);

    z_da_free(table);
    free(table);

    const unsigned long long t_total_end = now_ns();
    const double total_ms = (t_total_end - t_total_start) / 1e6;
    printf("total time: %.3f ms\n", total_ms);

    return 0;
}
