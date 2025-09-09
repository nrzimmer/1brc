#ifndef Z_UTILS_H
#define Z_UTILS_H

#include <assert.h>
#include <pthread.h>
#include <string.h>

#ifndef Z_DA_CAPACITY
#define Z_DA_CAPACITY 256
#endif

#define z_da_append(da, item)                                                                                           \
    do {                                                                                                                \
        z_da_reserve((da), (da)->count + 1);                                                                            \
        (da)->items[(da)->count++] = (item);                                                                            \
    } while (0)

#define z_da_reserve(da, expected_capacity)                                                                             \
    do {                                                                                                                \
        if (expected_capacity > (da)->capacity) {                                                                       \
            if ((da)->capacity == 0) {                                                                                  \
                (da)->capacity = Z_DA_CAPACITY;                                                                         \
            }                                                                                                           \
            while (expected_capacity > (da)->capacity) {                                                                \
                (da)->capacity *= 2;                                                                                    \
            }                                                                                                           \
            (da)->items = realloc((da)->items, ((da)->capacity) * sizeof(*(da)->items));                                \
            if ((*(void **) &(da)->items) == NULL) {                                                                    \
                printf("%s[%d] %s: Cannot allocate memory for dynamic array", __FILE__, __LINE__, __ASSERT_FUNCTION);   \
            }                                                                                                           \
        }                                                                                                               \
    }                                                                                                                   \
    while(0)

#define z_da_resize(da, new_size)                                                                                       \
    do {                                                                                                                \
        z_da_reserve((da), (new_size));                                                                                 \
        (da)->count = (new_size);                                                                                       \
    } while (0)

#define z_da_append_many(da, new_items, new_items_count)                                                                \
    do {                                                                                                                \
        size_t _z_da_n = (size_t)(new_items_count);                                                                     \
        z_da_reserve((da), (da)->count + _z_da_n);                                                                      \
        memcpy((da)->items + (da)->count,                                                                               \
               (const void*)(new_items),                                                                                \
               _z_da_n * sizeof(*(da)->items));                                                                         \
        (da)->count += _z_da_n;                                                                                         \
    } while (0)

#define z_da_append_null(da) z_da_append((da), '\0')
#define z_da_append_cstr(da, text) z_da_append_many((da), (text), strlen((const char*)(text)))
#define z_free(item) free(item); (item) = nullptr
#define z_da_free(da) z_free((da)->items); (da)->capacity = 0; (da)->count = 0

pthread_t *z_create_thread(void *(*func)(void *), void *arg);

void *z_join_thread(const pthread_t *thread);

#define z_free_thread(thread) z_free(thread)

unsigned long long now_ns(void);

#ifdef Z_UTILS_IMPLEMENTATION
#include <time.h>

inline unsigned long long now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (unsigned long long) ts.tv_sec * 1000000000ull + (unsigned long long) ts.tv_nsec;
}


inline pthread_t *z_create_thread(void *(*func)(void *), void *arg) {
    pthread_t *thread = malloc(sizeof(pthread_t));
    if (pthread_create(thread, NULL, func, arg) == 0) {
        return thread;
    }
    free(thread);
    return nullptr;
}

inline void *z_join_thread(const pthread_t *thread) {
    void *ret = nullptr;
    pthread_join(*thread, &ret);
    return ret;
}

#endif //Z_UTILS_IMPLEMENTATION

#endif //Z_UTILS_H
