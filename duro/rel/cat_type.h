/*
 * Catalog functions dealing with types.
 *
 * Copyright (C) 2012, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */
#ifndef CAT_TYPE_H_
#define CAT_TYPE_H_

typedef struct RDB_type RDB_type;
typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_transaction RDB_transaction;
typedef struct RDB_object RDB_object;

int
RDB_cat_get_type(const char *, RDB_exec_context *, RDB_transaction *,
        RDB_type **);

int
RDB_cat_check_type_used(RDB_type *, RDB_exec_context *, RDB_transaction *);

int
RDB_cat_insert_subtype(const char *, const char *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_cat_get_supertypes(const char *, RDB_exec_context *, RDB_transaction *,
        RDB_object *);

#endif /* CAT_TYPE_H_ */
