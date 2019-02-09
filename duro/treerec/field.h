/*
 * Recmap field functions shared by tree and BDB recmaps.
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef TREEREC_FIELD_H_
#define TREEREC_FIELD_H_

#include <gen/types.h>
#include <rec/recmap.h>

#include <stdlib.h>

enum {
    RECLEN_SIZE = 4
};

int
RDB_get_field(RDB_recmap *, int, const void *, size_t,
        size_t *, int *);

int
RDB_fields_to_mem(RDB_recmap *, int, const RDB_field[], void **, size_t *);

int
RDB_get_mem_fields(RDB_recmap *, void *, size_t,
        void *, size_t, int fieldc, RDB_field[]);

int
RDB_set_field_mem(RDB_recmap *, void **, size_t *,
        const RDB_field *, int);

size_t
RDB_get_vflen(uint8_t *, size_t, int, int);

#endif /* TREEREC_FIELD_H_ */
