#include "util.h"

#include "postgres.h"

static int 
pg_array_init(pg_array_t *array, int n, size_t size)
{
    /*
     * set "array->nelts" before "array->elts", otherwise MSVC thinks
     * that "array->nelts" may be used without having been initialized
     */

    array->nelts = 0;
    array->size = size;
    array->nalloc = n;

    array->elts = palloc0(n * size);
    if (array->elts == NULL) {
        return -1;
    }

    return 0;
}


pg_array_t *
pg_array_create(int n, size_t size)
{
    pg_array_t *a;

    a = palloc(sizeof(pg_array_t));
    if (a == NULL) {
        return NULL;
    }

    if (pg_array_init(a, n, size) != 0) {
        return NULL;
    }

    return a;
}


void
pg_array_destroy(pg_array_t *a)
{
	pfree(a->elts);
    pfree(a);

}


void *
pg_array_push(pg_array_t *a)
{
    void        *elt, *new;
    size_t       size;

    if (a->nelts == a->nalloc) {

        /* the array is full */

        size = a->size * a->nalloc;
        
        /* allocate a new array */

        new = palloc(2 * size);
        if (new == NULL) {
            return NULL;
        }

        memcpy(new, a->elts, size);
        a->elts = new;
        a->nalloc *= 2;
        
    }

    elt = (char *) a->elts + a->size * a->nelts;
    a->nelts++;

    return elt;
}

