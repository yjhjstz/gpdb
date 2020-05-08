#pragma once
#include <stddef.h>

typedef struct ItemPointerSet
{
	void* set;
	void* mutex;
} ItemPointerSet;


typedef struct {
    void        *elts;
    size_t   	nelts;
    size_t       size;
    int    		nalloc;
} pg_array_t;


pg_array_t *pg_array_create(int n, size_t size);
void pg_array_destroy(pg_array_t *a);
void *pg_array_push(pg_array_t *a);

