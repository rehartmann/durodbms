/*
 * sequence.c
 *
 *  Created on: 15.11.2013
 *      Author: Rene Hartmann
 */

#include "env.h"
#include "sequence.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>

int
RDB_open_sequence(const char *cname, const char *filename,
        RDB_environment *envp, DB_TXN *txn, RDB_sequence **seqpp)
{
    DBT key;
    int ret;
    DB *dbp;

    RDB_sequence *seqp = malloc(sizeof(RDB_sequence));
    if (seqp == NULL)
        return ENOMEM;
    seqp->filenamp = RDB_dup_str(filename);
    if (seqp->filenamp == NULL)
        return ENOMEM;
    seqp->cnamp = RDB_dup_str(cname);
    if (seqp->cnamp == NULL)
        return ENOMEM;

    ret = db_create(&dbp, envp->envp, 0);
    if (ret != 0)
        return ret;

    /*
     * Use a database for each sequence because only databases can be renamed.
     * The sequence key is always the same.
     */

    ret = dbp->open(dbp, txn, filename,
            cname, DB_HASH, DB_CREATE, 0664);
    if (ret != 0)
        return ret;

    memset(&key, 0, sizeof(DBT));
    key.data = "SEQ";
    key.size = (u_int32_t) sizeof("SEQ");

    ret = db_sequence_create(&seqp->seq, dbp, 0);
    if (ret != 0)
        return ret;

    /* Set initial value to 1 */
    ret = seqp->seq->initial_value(seqp->seq, (db_seq_t) 1);
    if (ret != 0)
        return ret;

    ret = seqp->seq->open(seqp->seq, txn, &key, DB_CREATE);
    if (ret != 0)
        return ret;
    *seqpp = seqp;
    return 0;
}

int
RDB_close_sequence(RDB_sequence *seqp)
{
    DB *dbp;
    int ret = seqp->seq->get_db(seqp->seq, &dbp);
    if (ret != 0)
        return ret;
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
RDB_delete_sequence(RDB_sequence *seqp, RDB_environment *envp, DB_TXN *txn)
{
    DB *dbp;
    int ret = seqp->seq->get_db(seqp->seq, &dbp);
    if (ret != 0)
        return ret;

    ret = seqp->seq->remove(seqp->seq, txn, 0);
    dbp->close(dbp, 0);
    envp->envp->dbremove(envp->envp, txn, seqp->filenamp, seqp->cnamp, 0);
    free(seqp->filenamp);
    free(seqp->cnamp);
    free(seqp);
    return ret;
}

int
RDB_sequence_next(RDB_sequence *seqp, DB_TXN *txn, RDB_int *valp)
{
    db_seq_t seqval;
    int ret = seqp->seq->get(seqp->seq, txn, (int32_t) 1, &seqval, 0);
    if (ret != 0)
        return ret;
    *valp = (RDB_int) seqval;
    return 0;
}

/*
 * Rename a sequence. The sequence must not be open.
 */
int
RDB_rename_sequence(const char *oldname, const char *newname,
        const char *filename, RDB_environment *envp, DB_TXN *txn)
{
    /* Rename database */
    return envp->envp->dbrename(envp->envp, txn, filename, oldname, newname, 0);
}
