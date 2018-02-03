/*
 * Sequence functions implemented using Berkeley DB
 *
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 */

#include "bdbsequence.h"
#include <rec/envimpl.h>
#include <rec/sequenceimpl.h>
#include <gen/strfns.h>
#include <obj/excontext.h>

#include <stdlib.h>
#include <string.h>
#include <errno.h>

RDB_sequence *
RDB_open_bdb_sequence(const char *cname, const char *filename,
        RDB_environment *envp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    DBT key;
    int ret;
    DB *dbp;

    RDB_sequence *seqp = malloc(sizeof(RDB_sequence));
    if (seqp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    seqp->filenamp = RDB_dup_str(filename);
    if (seqp->filenamp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    seqp->cnamp = RDB_dup_str(cname);
    if (seqp->cnamp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    seqp->close_fn = &RDB_close_bdb_sequence;
    seqp->next_fn = &RDB_bdb_sequence_next;
    seqp->delete_sequence_fn = &RDB_delete_bdb_sequence;

    ret = db_create(&dbp, envp->env.envp, 0);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return NULL;
    }

    /*
     * Use a database for each sequence because only databases can be renamed.
     * The sequence key is always the same.
     */

    ret = dbp->open(dbp, (DB_TXN *) rtxp, filename,
            cname, DB_HASH, DB_CREATE, 0664);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return NULL;
    }

    memset(&key, 0, sizeof(DBT));
    key.data = "SEQ";
    key.size = (u_int32_t) sizeof("SEQ");

    ret = db_sequence_create(&seqp->seq, dbp, 0);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return NULL;
    }

    /* Set initial value to 1 */
    ret = seqp->seq->initial_value(seqp->seq, (db_seq_t) 1);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return NULL;
    }

    ret = seqp->seq->open(seqp->seq, (DB_TXN *) rtxp, &key, DB_CREATE);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return NULL;
    }
    return seqp;
}

int
RDB_close_bdb_sequence(RDB_sequence *seqp, RDB_exec_context *ecp)
{
    DB *dbp;
    int ret = seqp->seq->get_db(seqp->seq, &dbp);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return ret;
    }
    ret = seqp->seq->close(seqp->seq, 0);
    dbp->close(dbp, 0);
    free(seqp->filenamp);
    free(seqp->cnamp);
    free(seqp);
    return ret;
}

/*
 * Delete the sequence and its BDB db.
 */
int
RDB_delete_bdb_sequence(RDB_sequence *seqp, RDB_environment *envp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    DB *dbp;
    DB_TXN *txn = (DB_TXN *) rtxp;
    int ret = seqp->seq->get_db(seqp->seq, &dbp);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }

    ret = seqp->seq->remove(seqp->seq, txn, 0);
    dbp->close(dbp, 0);
    envp->env.envp->dbremove(envp->env.envp, txn, seqp->filenamp, seqp->cnamp, 0);
    free(seqp->filenamp);
    free(seqp->cnamp);
    free(seqp);

    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_bdb_sequence_next(RDB_sequence *seqp, RDB_rec_transaction *rtxp, RDB_int *valp,
        RDB_exec_context *ecp)
{
    db_seq_t seqval;
    int ret = seqp->seq->get(seqp->seq, (DB_TXN *) rtxp, (int32_t) 1, &seqval, 0);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return ret;
    }
    *valp = (RDB_int) seqval;
    return 0;
}

/*
 * Rename a sequence. The sequence must not be open.
 */
int
RDB_rename_bdb_sequence(const char *oldname, const char *newname,
        const char *filename, RDB_environment *envp, RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    /* Rename database */
    int ret = envp->env.envp->dbrename(envp->env.envp, (DB_TXN *) rtxp, filename,
            oldname, newname, 0);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}
