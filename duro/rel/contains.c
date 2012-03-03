/*
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <string.h>

/** @addtogroup table
 * @{
 */

/**
 * RDB_table_contains checks if the tuple specified by <var>tplp</var>
is an element of the table specified by <var>tbp</var>
and stores the result at the location pointed to by <var>resultp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

On success, RDB_OK is returned.
If an error occurred, RDB_ERROR is returned.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>invalid_argument_error
<dd>A table attribute is missing in the tuple.
<dt>type_mismatch_error
<dd>The type of a tuple attribute does not match the type of the
corresponding table attribute.
<dt>operator_not_found_error
<dd>The definition of the table specified by <var>tbp</var>
refers to a non-existing operator.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_table_contains(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    int i;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Check if tuple contains all attributes
     */
    for (i = 0; i < tbp->typ->def.basetyp->def.tuple.attrc; i++) {
        char *attrname = tbp->typ->def.basetyp->def.tuple.attrv[i].name;

        if (strchr(attrname, '$') == NULL
                && RDB_tuple_get(tplp, attrname) == NULL) {
            RDB_raise_invalid_argument("incomplete tuple", ecp);
            return RDB_ERROR;
        }
    }

    if (RDB_obj_type(tplp) == NULL)
        RDB_obj_set_typeinfo((RDB_object *) tplp, RDB_base_type(RDB_obj_type(tbp)));

    return _RDB_matching_tuple(tbp, tplp, ecp, txp, resultp);
}

/*@}*/
