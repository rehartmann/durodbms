/*
 * Sequence functions
 *
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 */

#include "fdbsequence.h"
#include <obj/excontext.h>

RDB_sequence *
RDB_open_fdb_sequence(const char *cname, const char *filename,
        RDB_environment *envp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
	RDB_raise_not_supported("sequences not available", ecp);
	return RDB_ERROR;
}

int
RDB_close_fdb_sequence(RDB_sequence *seqp, RDB_exec_context *ecp)
{
	return RDB_OK;
}

/*
 * Delete the sequence and its BDB db.
 */
int
RDB_delete_fdb_sequence(RDB_sequence *seqp, RDB_environment *envp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
	RDB_raise_not_supported("sequences not available", ecp);
	return RDB_ERROR;
}

int
RDB_fdb_sequence_next(RDB_sequence *seqp, RDB_rec_transaction *rtxp, RDB_int *valp,
        RDB_exec_context *ecp)
{
	RDB_raise_not_supported("sequences not available", ecp);
	return RDB_ERROR;
}

int
RDB_rename_fdb_sequence(const char *oldname, const char *newname,
        const char *filename, RDB_environment *envp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
	RDB_raise_not_supported("sequences not available", ecp);
	return RDB_ERROR;
}
