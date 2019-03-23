/*
 * Internal transactions
 *
 * Copyright (C) 2019 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "fdbtx.h"
#include "fdbenv.h"
#include <rec/envimpl.h>
#include <obj/excontext.h>

#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

RDB_rec_transaction *
RDB_fdb_begin_tx(RDB_environment *envp,
        RDB_rec_transaction *parent_rtxp, RDB_exec_context *ecp)
{
	FDBTransaction *txp;
	fdb_error_t err;

	if (parent_rtxp != NULL) {
		RDB_raise_not_supported("nested transactions not available", ecp);
		return NULL;
	}

	err = fdb_database_create_transaction(envp->env.fdb, &txp);
    if (err != 0) {
        RDB_handle_fdb_errcode(err, ecp, NULL);
		return NULL;
    }
    return (RDB_rec_transaction *) txp;
}

int
RDB_fdb_commit(RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
	FDBTransaction *tx = (FDBTransaction *) rtxp;
	FDBFuture *f = fdb_transaction_commit(tx);
	fdb_error_t err = fdb_future_block_until_ready(f);
    if (err != 0) {
        RDB_handle_fdb_errcode(err, ecp, NULL);
        return RDB_ERROR;
    }
    err = fdb_future_get_error(f);
    fdb_future_destroy(f);
    if (err != 0) {
        RDB_handle_fdb_errcode(err, ecp, (FDBTransaction*)rtxp);
        if (!RDB_err_retryable(ecp)) {
            fdb_transaction_destroy(tx);
        }
        return RDB_ERROR;
	}
    fdb_transaction_destroy(tx);
    return RDB_OK;
}

int
RDB_fdb_abort(RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
	fdb_transaction_destroy((FDBTransaction *) rtxp);
	return RDB_OK;
}

int
RDB_fdb_tx_id(RDB_rec_transaction *rtxp) {
    return -1;
}
