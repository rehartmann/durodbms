/*
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <string.h>

int
RDB_table_contains(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    int i;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Check if tuple contains all attributes
     */
    for (i = 0; i < tbp->typ->var.basetyp->var.tuple.attrc; i++) {
        char *attrname = tbp->typ->var.basetyp->var.tuple.attrv[i].name;

        if (strchr(attrname, '$') == NULL
                && RDB_tuple_get(tplp, attrname) == NULL) {
            RDB_raise_invalid_argument("incomplete tuple", ecp);
            return RDB_ERROR;
        }
    }

    return _RDB_matching_tuple(tbp, tplp, ecp, txp, resultp);
}
