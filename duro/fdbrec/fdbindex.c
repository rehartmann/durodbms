/*
 * Copyright (C) 2004-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "fdbindex.h"
#include <obj/excontext.h>

/*
 * Create index.
 */
RDB_index *
RDB_create_fdb_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const RDB_field_descriptor fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
	RDB_raise_not_supported("indexes not available", ecp);
    return NULL;
}

RDB_index *
RDB_open_fdb_index(RDB_recmap *rmp, const char *namp, const char *filenamp,
        RDB_environment *envp, int fieldc, const int fieldv[],
        const RDB_compare_field cmpv[], int flags, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
	RDB_raise_not_supported("indexes not available", ecp);
    return NULL;
}

int
RDB_close_fdb_index(RDB_index *ixp, RDB_exec_context *ecp)
{
	RDB_raise_not_supported("indexes not available", ecp);
    return RDB_ERROR;
}

/* Delete an index. */
int
RDB_delete_fdb_index(RDB_index *ixp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
	RDB_raise_not_supported("indexes not available", ecp);
    return RDB_ERROR;
}

int
RDB_fdb_index_get_fields(RDB_index *ixp, RDB_field keyv[], int fieldc,
        RDB_rec_transaction *rtxp, RDB_field retfieldv[], RDB_exec_context *ecp)
{
	RDB_raise_not_supported("indexes not available", ecp);
    return RDB_ERROR;
}

int
RDB_fdb_index_delete_rec(RDB_index *ixp, RDB_field keyv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
	RDB_raise_not_supported("indexes not available", ecp);
    return RDB_ERROR;
}
