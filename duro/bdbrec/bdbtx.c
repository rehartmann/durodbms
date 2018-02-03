/*
 * Internal transaction
 *
 * Copyright (C) 2016, 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <bdbrec/bdbtx.h>
#include <rec/envimpl.h>
#include <obj/excontext.h>

#include <db.h>

RDB_rec_transaction *
RDB_bdb_begin_tx(RDB_environment *envp,
        RDB_rec_transaction *parent_rtxp, RDB_exec_context *ecp)
{
    DB_TXN *txp;
    int ret = ((DB_ENV *)envp->env.envp)->txn_begin(envp->env.envp,
            (DB_TXN *) parent_rtxp, &txp, 0);
    if (ret != 0) {
        RDB_raise_system("too many transactions", ecp);
        return NULL;
    }
    return (RDB_rec_transaction *)txp;
}

int
RDB_bdb_commit(RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    int ret = ((DB_TXN *) rtxp)->commit((DB_TXN *) rtxp, 0);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_bdb_abort(RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    int ret = ((DB_TXN *) rtxp)->abort((DB_TXN *) rtxp);
    if (ret != 0) {
        RDB_errcode_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_bdb_tx_id(RDB_rec_transaction *rtxp) {
    return ((DB_TXN *) rtxp)->id((DB_TXN *) rtxp);
}
