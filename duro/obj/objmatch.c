/*
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "objmatch.h"
#include "excontext.h"
#include "object.h"
#include "type.h"
#include "tuple.h"
#include "array.h"
#include "builtintypes.h"

#include <stdlib.h>

static RDB_bool
array_matches_type(const RDB_object *arrp, RDB_type *typ)
{
    RDB_exec_context ec;
    RDB_object *elemp;
    RDB_bool result;

    if (typ->kind != RDB_TP_ARRAY)
        return RDB_FALSE;

    RDB_init_exec_context(&ec);

    elemp = RDB_array_get(arrp, (RDB_int) 0, &ec);
    if (elemp == NULL) {
        result = (RDB_bool) (RDB_obj_type(RDB_get_err(&ec)) == &RDB_NOT_FOUND_ERROR);
    } else {
        result = RDB_obj_matches_type(elemp, typ->def.basetyp);
    }
    RDB_destroy_exec_context(&ec);
    return result;
}

/*
 * Check if *objp has type *typ.
 * If *objp does not carry type information it must be a tuple.
 */
RDB_bool
RDB_obj_matches_type(const RDB_object *objp, RDB_type *typ)
{
    int i;
    RDB_object *attrobjp;
    RDB_type *objtyp = RDB_obj_type(objp);
    if (objtyp != NULL) {
        return RDB_type_matches(objtyp, typ);
    }

    switch (objp->kind) {
    case RDB_OB_INITIAL:
    case RDB_OB_TUPLE:
        if (typ->kind != RDB_TP_TUPLE)
            return RDB_FALSE;
        if ((int) RDB_tuple_size(objp) != typ->def.tuple.attrc)
            return RDB_FALSE;
        for (i = 0; i < typ->def.tuple.attrc; i++) {
            attrobjp = RDB_tuple_get(objp, typ->def.tuple.attrv[i].name);
            if (attrobjp == NULL)
                return RDB_FALSE;
            if (!RDB_obj_matches_type(attrobjp, typ->def.tuple.attrv[i].typ))
                return RDB_FALSE;
        }
        return RDB_TRUE;
    case RDB_OB_ARRAY:
        return array_matches_type(objp, typ);
    default:
        ;
    }
    abort();
}

