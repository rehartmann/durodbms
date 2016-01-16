/*
 * varmap.h
 *
 *  Created on: 16.01.2016
 *      Author: rene
 */

#ifndef DURO_VARMAP_H_
#define DURO_VARMAP_H_

#include <gen/types.h>
#include <gen/hashtable.h>

typedef struct RDB_object RDB_object;
typedef struct RDB_exec_context RDB_exec_context;

typedef struct {
    RDB_hashtable hashtab;
} Duro_varmap;

typedef struct {
    char *name;
    RDB_object *varp;
    RDB_bool is_const;
} Duro_var_entry;

void
Duro_init_varmap(Duro_varmap *, int);

void
Duro_destroy_varmap(Duro_varmap *);

int
Duro_varmap_put(Duro_varmap *, const char *, RDB_object *, RDB_bool,
        RDB_exec_context *);

Duro_var_entry *
Duro_varmap_get(const Duro_varmap *, const char *);

#endif /* DURO_VARMAP_H_ */
