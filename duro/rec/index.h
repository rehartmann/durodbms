#ifndef RDB_INDEX_H
#define RDB_INDEX_H

/* $Id$ */

#include "recmap.h"

/*
 * Secondary index.
 */

typedef struct {
    RDB_recmap *rmp;
    DB *dbp;
    char *namp;
    char *filenamp;
    int fieldc;
    int *fieldv;
    RDB_bool unique;  
} RDB_index;

int
RDB_create_index(RDB_recmap *, const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const int fieldv[],
        RDB_bool unique, DB_TXN *, RDB_index **);

int
RDB_open_index(RDB_recmap *, const char *namp, const char *filenamp,
        RDB_environment *dsp, int fieldc, const int fieldv[],
        RDB_bool unique, DB_TXN *, RDB_index **);

/* Close an index. */
int
RDB_close_index(RDB_index *);

/* Delete an index. */
int
RDB_delete_index(RDB_index *, RDB_environment *);

#endif
