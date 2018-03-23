/*
 * Index internals
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef REC_INDEXIMPL_H_
#define REC_INDEXIMPL_H_

#include "index.h"
#include "recmap.h"
#include <db.h>

typedef struct RDB_index {
    RDB_recmap *rmp;
    DB *dbp;
    char *namp;
    char *filenamp;
    int fieldc;
    int *fieldv;
    /* For ordered indexes */
    RDB_compare_field *cmpv;

    int (*close_index_fn)(RDB_index *, RDB_exec_context *);
    int (*delete_index_fn)(RDB_index *, RDB_environment *, RDB_rec_transaction *,
            RDB_exec_context *);
} RDB_index;

#endif /* REC_INDEXIMPL_H_ */
