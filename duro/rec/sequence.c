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
RDB_open_sequence(const char *name, const char *filename,
        RDB_environment *envp, DB_TXN *txn, RDB_sequence **seqpp)
{
    DBT key;
    int ret;
    RDB_sequence *seqp = malloc(sizeof(RDB_sequence));
    if (seqp == NULL)
        return ENOMEM;

    if (envp->seq_dbp == NULL) {
        ret = db_create(&envp->seq_dbp, envp->envp, 0);
        if (ret != 0)
            return ret;
        ret = envp->seq_dbp->open(envp->seq_dbp, txn, filename,
                "$SEQUENCE", DB_HASH, DB_CREATE, 0664);
        if (ret != 0)
            return ret;
    }

    memset(&key, 0, sizeof(DBT));
    key.data = (char *) name;
    key.size = (u_int32_t) strlen(name);

    ret = db_sequence_create(&seqp->seq, envp->seq_dbp, 0);
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
    return seqp->seq->close(seqp->seq, 0);
}

int
RDB_delete_sequence(RDB_sequence *seqp, DB_TXN *txn)
{
    return seqp->seq->remove(seqp->seq, txn, 0);
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
