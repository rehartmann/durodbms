/*
 * Sequence functions
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 */

#include "sequenceimpl.h"
#include "envimpl.h"

/*
 * Open a sequence. Create the sequence if it does not exist.
 */
RDB_sequence *
RDB_open_sequence(const char *cname, const char *filename,
        RDB_environment *envp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    return (*envp->open_sequence_fn)(cname, filename, envp, rtxp, ecp);
}

int
RDB_close_sequence(RDB_sequence *seqp, RDB_exec_context *ecp)
{
    return (*seqp->close_fn)(seqp, ecp);
}

/*
 * Delete the sequence.
 */
int
RDB_delete_sequence(RDB_sequence *seqp, RDB_environment *envp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    return (*seqp->delete_sequence_fn)(seqp, envp, rtxp, ecp);
}

int
RDB_sequence_next(RDB_sequence *seqp, RDB_rec_transaction *rtxp, RDB_int *valp,
        RDB_exec_context *ecp)
{
    return (*seqp->next_fn)(seqp, rtxp, valp, ecp);
}

/*
 * Rename a sequence. The sequence must not be open.
 */
int
RDB_rename_sequence(const char *oldname, const char *newname,
        const char *filename, RDB_environment *envp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    return (*envp->rename_sequence_fn)(oldname, newname, filename, envp,
            rtxp, ecp);
}
