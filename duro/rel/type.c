/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include "cat_type.h"
#include "cat_op.h"
#include "serialize.h"
#include "io.h"
#include <gen/strfns.h>

#include <string.h>
#include <locale.h>
#include <assert.h>

int
RDB_getter_name(const RDB_type *typ, const char *compname,
        RDB_object *strobjp, RDB_exec_context *ecp)
{
    if (RDB_string_to_obj(strobjp, typ->name, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(strobjp, RDB_GETTER_INFIX, ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_append_string(strobjp, compname, ecp);
}

static int
load_getter(RDB_type *typ, const char *compname, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object opnameobj;
    RDB_int cnt;

    RDB_init_obj(&opnameobj);
    if (RDB_getter_name(typ, compname, &opnameobj, ecp) != RDB_OK)
        goto error;

    cnt = RDB_cat_load_ro_op(RDB_obj_string(&opnameobj), ecp, txp);
    if (cnt == (RDB_int) RDB_ERROR)
        goto error;
    if (cnt == 0) {
        RDB_raise_operator_not_found(RDB_obj_string(&opnameobj), ecp);
        goto error;
    }
    return RDB_destroy_obj(&opnameobj, ecp);

error:
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

int
RDB_setter_name(const RDB_type *typ, const char *compname,
        RDB_object *strobjp, RDB_exec_context *ecp)
{
    if (RDB_string_to_obj(strobjp, typ->name, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(strobjp, RDB_SETTER_INFIX, ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_append_string(strobjp, compname, ecp);
}

static int
load_setter(RDB_type *typ, const char *compname, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object opnameobj;
    RDB_int cnt;

    RDB_init_obj(&opnameobj);
    if (RDB_setter_name(typ, compname, &opnameobj, ecp) != RDB_OK)
        goto error;

    cnt = RDB_cat_load_upd_op(RDB_obj_string(&opnameobj), ecp, txp);
    if (cnt == (RDB_int) RDB_ERROR)
        goto error;
    if (cnt == 0) {
        RDB_raise_operator_not_found(RDB_obj_string(&opnameobj), ecp);
        goto error;
    }
    return RDB_destroy_obj(&opnameobj, ecp);

error:
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

/*
 * Load selector, getters and setters of user-defined type
 */
int
RDB_load_type_ops(RDB_type *typ, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;

    for (i = 0; i < typ->def.scalar.repc; i++) {
        int j;

        if (RDB_cat_load_ro_op(typ->def.scalar.repv[i].name, ecp, txp)
                == (RDB_int) RDB_ERROR)
            return RDB_ERROR;

        if (!typ->def.scalar.sysimpl) {
            /* Load getters and setters */
            for (j = 0; j < typ->def.scalar.repv[i].compc; j++) {
                if (load_getter(typ, typ->def.scalar.repv[i].compv[j].name, ecp, txp)
                        != RDB_OK)
                    return RDB_ERROR;
                if (load_setter(typ, typ->def.scalar.repv[i].compv[j].name, ecp, txp)
                        != RDB_OK)
                    return RDB_ERROR;
            }
        }
    }
    return RDB_OK;
}

/** @addtogroup type Type functions
 * @{
 */

/**
 * Check if a type is numeric.
 *
 * @returns
 * RDB_TRUE if the type is integer or float, RDB_FALSE otherwise.
 */
RDB_bool
RDB_type_is_numeric(const RDB_type *typ) {
    return (RDB_bool) (typ == &RDB_INTEGER || typ == &RDB_FLOAT);
}

/**
 * Check if a type is valid, that is, if it can be used for declaring variables.
 * A scalar type is valid if it is implemented (either a built-in type
 * or a user-defined type on which RDB_implement_type() has been called
 * successfully).
 * A non-scalar type is valid if its base/attribute types are valid.
 *
 * @returns
 * RDB_TRUE if the type is valid, RDB_FALSE otherwise.
 */
RDB_bool
RDB_type_is_valid(const RDB_type *typ) {
    int i;

    switch (typ->kind) {
        case RDB_TP_TUPLE:
            for (i = 0; i < typ->def.tuple.attrc; i++) {
                if (!RDB_type_is_valid(typ->def.tuple.attrv[i].typ))
                    return RDB_FALSE;
            }
            return RDB_TRUE;
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            return RDB_type_is_valid(typ->def.basetyp);
        case RDB_TP_SCALAR:
            return (RDB_bool) (typ->ireplen != RDB_NOT_IMPLEMENTED);
    }
    abort();
}

/**
 * If *<var>typ</var> is non-scalar, RDB_dup_nonscalar_creates a copy of it.

@returns

A pointer to a copy of *<var>typ</var>, if *<var>typ</var> is non-scalar.
<var>typ</var>, if *<var>typ</var> is scalar.

 If the operation fails, NULL is returned.
 */
RDB_type *
RDB_dup_nonscalar_type(RDB_type *typ, RDB_exec_context *ecp)
{
    RDB_type *restyp;

    switch (typ->kind) {
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            restyp = RDB_alloc(sizeof (RDB_type), ecp);
            if (restyp == NULL) {
                return NULL;
            }
            restyp->name = NULL;
            restyp->kind = typ->kind;
            restyp->ireplen = RDB_VARIABLE_LEN;
            restyp->def.basetyp = RDB_dup_nonscalar_type(typ->def.basetyp,
                    ecp);
            if (restyp->def.basetyp == NULL) {
                RDB_free(restyp);
                return NULL;
            }
            return restyp;
        case RDB_TP_TUPLE:
            return RDB_new_tuple_type(typ->def.tuple.attrc,
                    typ->def.tuple.attrv, ecp);
        case RDB_TP_SCALAR:
            return typ;
    }
    abort();
}

/**
 * Create a RDB_type struct for a tuple type
 * and returns a pointer to it.
The attributes are specified by <var>attrc</var> and <var>attrv</var>.
The fields defaultp and options of RDB_attr are ignored.

@returns

A pointer to the RDB_type struct, or NULL if an error occured.

@par Errors:

<dl>
<dt>invalid_argument_error
<dd><var>attrv</var> contains two attributes with the same name.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
RDB_type *
RDB_new_tuple_type(int attrc, const RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *tuptyp;
    int i, j;

    tuptyp = RDB_alloc(sizeof(RDB_type), ecp);
    if (tuptyp == NULL) {
        return NULL;
    }
    tuptyp->name = NULL;
    tuptyp->compare_op = NULL;
    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->ireplen = RDB_VARIABLE_LEN;
    if (attrc > 0) {
        tuptyp->def.tuple.attrv = RDB_alloc(sizeof(RDB_attr) * attrc, ecp);
        if (tuptyp->def.tuple.attrv == NULL) {
            RDB_free(tuptyp);
            return NULL;
        }
    }
    for (i = 0; i < attrc; i++) {
        tuptyp->def.tuple.attrv[i].typ = NULL;
        tuptyp->def.tuple.attrv[i].name = NULL;
    }
    for (i = 0; i < attrc; i++) {
        /* Check if name appears twice */
        for (j = i + 1; j < attrc; j++) {
            if (strcmp(attrv[i].name, attrv[j].name) == 0) {
                RDB_raise_invalid_argument("duplicate attribute name", ecp);
                goto error;
            }
        }

        tuptyp->def.tuple.attrv[i].typ = RDB_dup_nonscalar_type(
                attrv[i].typ, ecp);
        if (tuptyp->def.tuple.attrv[i].typ == NULL) {
            goto error;
        }
        tuptyp->def.tuple.attrv[i].name = RDB_dup_str(attrv[i].name);
        if (tuptyp->def.tuple.attrv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }
    tuptyp->def.tuple.attrc = attrc;

    return tuptyp;

error:
    for (i = 0; i < attrc; i++) {
        RDB_attr *attrp = &tuptyp->def.tuple.attrv[i];
        if (attrp->name != NULL)
            RDB_free(attrp->name);
        if (attrp->typ != NULL) {
            if (!RDB_type_is_scalar(attrp->typ))
                RDB_del_nonscalar_type(attrp->typ, ecp);
        }
    }
    RDB_free(tuptyp->def.tuple.attrv);
    RDB_free(tuptyp);

    return NULL;
}

/**
 * Create a RDB_type struct for a relation type and return a pointer to it.
The attributes are specified by <var>attrc</var> and <var>attrv</var>.
The fields defaultp and options of RDB_attr are ignored.

@returns

On success, a pointer to the RDB_type struct. On failure, NULL is returned.

@par Errors:

<dl>
<dt>invalid_argument_error
<dd><var>attrv</var> contains two attributes with the same name.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
RDB_type *
RDB_new_relation_type(int attrc, const RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *tpltyp = RDB_new_tuple_type(attrc, attrv, ecp);
    if (tpltyp == NULL) {
        return NULL;
    }

    return RDB_new_relation_type_from_base(tpltyp, ecp);
}

/**
 * Create a RDB_type struct for a relation type from a tuple type.

@returns

On success, a pointer to the RDB_type struct. On failure, NULL is returned.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
RDB_type *
RDB_new_relation_type_from_base(RDB_type *tpltyp, RDB_exec_context *ecp)
{
    RDB_type *typ = RDB_alloc(sizeof (RDB_type), ecp);
    if (typ == NULL) {
        return NULL;
    }

    typ->name = NULL;
    typ->compare_op = NULL;
    typ->kind = RDB_TP_RELATION;
    typ->ireplen = RDB_VARIABLE_LEN;
    typ->def.basetyp = tpltyp;
    return typ;
}

/**
 * Creates a RDB_type struct for an array type.
The base type is specified by <var>typ</var>.

@returns

A pointer to a RDB_type structure for the new array type,
or NULL if the creation failed.
 */
RDB_type *
RDB_new_array_type(RDB_type *basetyp, RDB_exec_context *ecp)
{
    RDB_type *typ = RDB_alloc(sizeof (RDB_type), ecp);
    if (typ == NULL) {
        return NULL;
    }
    
    typ->name = NULL;
    typ->compare_op = NULL;
    typ->kind = RDB_TP_ARRAY;
    typ->ireplen = RDB_VARIABLE_LEN;
    typ->def.basetyp = basetyp;

    return typ;
}

/**
RDB_type_is_scalar checks if a type is scalar.

@returns

RDB_TRUE if *<var>typ</var> is scalar, RDB_FALSE if not.
*/
RDB_bool
RDB_type_is_scalar(const RDB_type *typ)
{
    return (RDB_bool) (typ->kind == RDB_TP_SCALAR);
}

/**
 * Checks if a type is a relation type.

@returns

RDB_TRUE if *<var>typ</var> is a relation type, RDB_FALSE if not.
*/
RDB_bool
RDB_type_is_relation(const RDB_type *typ)
{
    return (RDB_bool) (typ->kind == RDB_TP_RELATION);
}

/**
 * Checks if a type is a tuple type.

@returns

RDB_TRUE if *<var>typ</var> is a tuple type, RDB_FALSE if not.
*/
RDB_bool
RDB_type_is_tuple(const RDB_type *typ)
{
    return (RDB_bool) (typ->kind == RDB_TP_TUPLE);
}

/**
 * Checks if a type is an array type.
 *
 * @returns
 * RDB_TRUE if *<var>typ</var> is an array type, RDB_FALSE if not.
 */
RDB_bool
RDB_type_is_array(const RDB_type *typ)
{
    return (RDB_bool) (typ->kind == RDB_TP_ARRAY);
}

/**
 * Return the base type of a relation or array type.
 *
 * @returns
 * The base type if <var>typ</var> is a relation or array type,
 * NULL otherwise.
 */
RDB_type *
RDB_base_type(RDB_type *typ)
{
    return typ->kind == RDB_TP_ARRAY || typ->kind == RDB_TP_RELATION
            ? typ->def.basetyp : NULL;
}

/**
 * RDB_type_attrs returns a pointer to an array of
RDB_attr structs
describing the attributes of the tuple or relation type
specified by *<var>typ</var> and stores the number of attributes in
*<var>attrcp</var>.

The pointer returned must no longer be used if the RDB_type structure
has been destroyed.

@returns

A pointer to an array of RDB_attr structs or NULL if the type
is not a tuple or relation type.
 */
RDB_attr *
RDB_type_attrs(RDB_type *typ, int *attrcp)
{
    if (typ->kind == RDB_TP_RELATION) {
        typ = typ->def.basetyp;
    }
    if (typ->kind != RDB_TP_TUPLE) {
        return NULL;
    }
    *attrcp = typ->def.tuple.attrc;
    return typ->def.tuple.attrv;
}

/** @struct RDB_possrep rdb.h <rel/rdb.h>
 * Specifies a possible representation.
 */

/**
Return a pointer to RDB_type structure which
represents the type with the name <var>name</var>.

@returns

The pointer to the type on success, or NULL if an error occured.

@par Errors:

<dl>
<dt>name_error</dt>
<dd>A type with the name <var>name</var> could not be found.
</dd>
<dt></dt>
<dd>The type <var>name</var> is not a built-in type and <var>txp</var> is NULL.
</dd>
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
RDB_type *
RDB_get_type(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *typv[2];
    RDB_operator *cmpop;
    RDB_type *typ;
    int ret;

    /*
     * search type in built-in type map
     */
    typ = RDB_hashmap_get(&RDB_builtin_type_map, name);
    if (typ != NULL) {
        return typ;
    }

    if (txp == NULL) {
        RDB_raise_name(name, ecp);
        return NULL;
    }

    /*
     * Search type in dbroot type map
     */
    typ = RDB_hashmap_get(&txp->dbp->dbrootp->typemap, name);
    if (typ != NULL) {
        return typ;
    }

    /*
     * Search type in catalog
     */
    ret = RDB_cat_get_type(name, ecp, txp, &typ);
    if (ret != RDB_OK) {
        RDB_type *errtyp = RDB_obj_type(RDB_get_err(ecp));
        if (errtyp != NULL && errtyp == &RDB_NOT_FOUND_ERROR) {
            RDB_raise_name(name, ecp);
        }
        return NULL;
    }

    /*
     * Put type into type map
     */
    ret = RDB_hashmap_put(&txp->dbp->dbrootp->typemap, name, typ);
    if (ret != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    if (typ->ireplen != RDB_NOT_IMPLEMENTED) {
        /* Evaluate init expression */
        RDB_init_obj(&typ->def.scalar.init_val);
        if (RDB_evaluate(typ->def.scalar.initexp, NULL, NULL, NULL,
                ecp, txp, &typ->def.scalar.init_val) != RDB_OK) {
            RDB_destroy_obj(&typ->def.scalar.init_val, ecp);
            return NULL;
        }
        typ->def.scalar.init_val_is_valid = RDB_TRUE;

        /* Load selector, getters, and setters */
        if (RDB_load_type_ops(typ, ecp, txp) != RDB_OK)
            return NULL;
    }

    /*
     * Search for comparison function (after type was put into type map
     * so the type is available)
     */
    typv[0] = typ;
    typv[1] = typ;
    cmpop = RDB_get_ro_op("cmp", 2, typv, NULL, ecp, txp);
    if (cmpop != NULL) {
        typ->compare_op = cmpop;
    } else {
        RDB_object *errp = RDB_get_err(ecp);
        if (errp != NULL
                && RDB_obj_type(errp) != &RDB_OPERATOR_NOT_FOUND_ERROR
                && RDB_obj_type(errp) != &RDB_TYPE_MISMATCH_ERROR) {
            return NULL;
        }
        RDB_clear_err(ecp);
    }

    return typ;
}

/**
 * Delete *typ from memory. The type must be non-scalar.
 */
int
RDB_del_nonscalar_type(RDB_type *typ, RDB_exec_context *ecp)
{
    int i;
    int ret = RDB_OK;

    switch (typ->kind) {
        case RDB_TP_TUPLE:
            for (i = 0; i < typ->def.tuple.attrc; i++) {
                RDB_type *attrtyp = typ->def.tuple.attrv[i].typ;

                RDB_free(typ->def.tuple.attrv[i].name);
                if (!RDB_type_is_scalar(attrtyp))
                    ret = RDB_del_nonscalar_type(attrtyp, ecp);
            }
            if (typ->def.tuple.attrc > 0)
                RDB_free(typ->def.tuple.attrv);
            break;
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            if (!RDB_type_is_scalar(typ->def.basetyp))
                ret = RDB_del_nonscalar_type(typ->def.basetyp, ecp);
            break;
        case RDB_TP_SCALAR:
            RDB_raise_invalid_argument("type is scalar", ecp);
            return RDB_ERROR;
        default:
            /* !! RDB_raise_internal("invalid type structure", ecp); */
            abort();
    }
    typ->kind = (enum RDB_tp_kind) -1; /* for debugging */
    RDB_free(typ);
    return ret;
}

/**
 * Check if two types are equal.

Nonscalar types are equal if their definition is the same.

@returns

RDB_TRUE if the types are equal, RDB_FALSE otherwise.
 */
RDB_bool
RDB_type_equals(const RDB_type *typ1, const RDB_type *typ2)
{
    if (typ1 == typ2)
        return RDB_TRUE;
    if (typ1->kind != typ2->kind)
        return RDB_FALSE;

    /* If the two types both have a name, they are equal iff their name
     * is equal */
    if (typ1->name != NULL && typ2->name != NULL)
         return (RDB_bool) (strcmp(typ1->name, typ2->name) == 0);
    
    switch (typ1->kind) {
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            return RDB_type_equals(typ1->def.basetyp, typ2->def.basetyp);
        case RDB_TP_TUPLE:
            {
                int i, j;
                int attrcnt = typ1->def.tuple.attrc;

                if (attrcnt != typ2->def.tuple.attrc)
                    return RDB_FALSE;
                    
                /* check if all attributes of typ1 also appear in typ2 */
                for (i = 0; i < attrcnt; i++) {
                    for (j = 0; j < attrcnt; j++) {
                        if (RDB_type_equals(typ1->def.tuple.attrv[i].typ,
                                typ2->def.tuple.attrv[j].typ)
                                && (strcmp(typ1->def.tuple.attrv[i].name,
                                typ2->def.tuple.attrv[j].name) == 0))
                            break;
                    }
                    if (j >= attrcnt) {
                        /* not found */
                        return RDB_FALSE;
                    }
                }
                return RDB_TRUE;
            }
        default:
            ;
    }
    abort();
}  

/**
 * Return the name of a type.

@returns

A pointer to the name of the type or NULL if the type has no name.
 */
char *
RDB_type_name(const RDB_type *typ)
{
    return typ->name;
}

/*@}*/

RDB_type *
RDB_type_attr_type(const RDB_type *typ, const char *name)
{
    RDB_attr *attrp;

    switch (typ->kind) {
        case RDB_TP_RELATION:
            attrp = RDB_tuple_type_attr(typ->def.basetyp, name);
            break;
        case RDB_TP_TUPLE:
            attrp = RDB_tuple_type_attr(typ, name);
            break;
        case RDB_TP_ARRAY:
        case RDB_TP_SCALAR:
            return NULL;
    }
    if (attrp == NULL)
        return NULL;
    return attrp->typ;
}

RDB_type *
RDB_extend_tuple_type(const RDB_type *typ, int attrc, RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    int i;
    RDB_type *newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->def.tuple.attrc = typ->def.tuple.attrc + attrc;
    newtyp->def.tuple.attrv = RDB_alloc(sizeof (RDB_attr)
            * (newtyp->def.tuple.attrc), ecp);
    if (newtyp->def.tuple.attrv == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        newtyp->def.tuple.attrv[i].name = NULL;
    }
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        newtyp->def.tuple.attrv[i].name =
                RDB_dup_str(typ->def.tuple.attrv[i].name);
        if (newtyp->def.tuple.attrv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtyp->def.tuple.attrv[i].typ =
                RDB_dup_nonscalar_type(typ->def.tuple.attrv[i].typ, ecp);
        if (newtyp->def.tuple.attrv[i].typ == NULL) {
            goto error;
        }
    }
    for (i = 0; i < attrc; i++) {
        /*
         * Check if the attribute is already present in the original tuple type
         */
        if (RDB_tuple_type_attr(typ, attrv[i].name) != NULL) {
            RDB_raise_invalid_argument("attribute exists", ecp);
            goto error;
        }

        newtyp->def.tuple.attrv[typ->def.tuple.attrc + i].name =
                RDB_dup_str(attrv[i].name);
        if (newtyp->def.tuple.attrv[typ->def.tuple.attrc + i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
        newtyp->def.tuple.attrv[typ->def.tuple.attrc + i].typ =
                RDB_dup_nonscalar_type(attrv[i].typ, ecp);
        if (newtyp->def.tuple.attrv[typ->def.tuple.attrc + i].typ == NULL) {
            goto error;
        }
    }
    return newtyp;

error:
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        RDB_free(newtyp->def.tuple.attrv[i].name);
        if (newtyp->def.tuple.attrv[i].typ != NULL
                && !RDB_type_is_scalar(newtyp->def.tuple.attrv[i].typ)) {
            RDB_del_nonscalar_type(newtyp->def.tuple.attrv[i].typ, ecp);
        }
    }
    RDB_free(newtyp->def.tuple.attrv);
    RDB_free(newtyp);
    return NULL;
}

RDB_type *
RDB_extend_relation_type(const RDB_type *typ, int attrc, RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;
    newtyp->def.basetyp = RDB_extend_tuple_type(typ->def.basetyp, attrc, attrv,
            ecp);
    if (newtyp->def.basetyp == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    return newtyp;
}

RDB_type *
RDB_union_tuple_types(const RDB_type *typ1, const RDB_type *typ2,
        RDB_exec_context *ecp)
{
    RDB_type *newtyp;
    int attrc;
    int i, j;

    /* Create new tuple type */
    newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    
    /* calculate new # of attributes as the sum of the # of attributes
     * of both types.
     * That often will be too high; in this case it is reduced later.
     */
    newtyp->def.tuple.attrc = typ1->def.tuple.attrc
            + typ2->def.tuple.attrc;
    newtyp->def.tuple.attrv = RDB_alloc(sizeof (RDB_attr)
            * newtyp->def.tuple.attrc, ecp);
    if (newtyp->def.tuple.attrv == NULL)
        return NULL;

    for (i = 0; i < typ1->def.tuple.attrc; i++)
        newtyp->def.tuple.attrv[i].name = NULL;

    /* copy attributes from first tuple type */
    for (i = 0; i < typ1->def.tuple.attrc; i++) {
        newtyp->def.tuple.attrv[i].name = RDB_dup_str(
                typ1->def.tuple.attrv[i].name);
        newtyp->def.tuple.attrv[i].typ =
                RDB_dup_nonscalar_type(typ1->def.tuple.attrv[i].typ, ecp);
        if (newtyp->def.tuple.attrv[i].typ == NULL)
            goto error;
    }
    attrc = typ1->def.tuple.attrc;

    /* add attributes from second tuple type */
    for (i = 0; i < typ2->def.tuple.attrc; i++) {
        for (j = 0; j < typ1->def.tuple.attrc; j++) {
            if (strcmp(typ2->def.tuple.attrv[i].name,
                    typ1->def.tuple.attrv[j].name) == 0) {
                /* If two attributes match by name, they must be of
                   the same type */
                if (!RDB_type_equals(typ2->def.tuple.attrv[i].typ,
                        typ1->def.tuple.attrv[j].typ)) {
                    RDB_raise_type_mismatch("JOIN attribute types do not match",
                            ecp);
                    goto error;
                }
                break;
            }
        }
        if (j >= typ1->def.tuple.attrc) {
            /* attribute not found, so add it to result type */
            newtyp->def.tuple.attrv[attrc].name = RDB_dup_str(
                    typ2->def.tuple.attrv[i].name);
            newtyp->def.tuple.attrv[attrc].typ =
                    RDB_dup_nonscalar_type(typ2->def.tuple.attrv[i].typ, ecp);
            if (newtyp->def.tuple.attrv[attrc].typ == NULL)
                goto error;
            attrc++;
        }
    }

    /* adjust array size, if necessary */    
    if (attrc < newtyp->def.tuple.attrc) {
        void *p = RDB_realloc(newtyp->def.tuple.attrv,
                sizeof(RDB_attr) * attrc, ecp);
        if (p == NULL)
            goto error;

        newtyp->def.tuple.attrc = attrc;
        newtyp->def.tuple.attrv = p;
    }
    return newtyp;

error:
    for (i = 0; i < typ1->def.tuple.attrc; i++)
        RDB_free(newtyp->def.tuple.attrv[i].name);

    RDB_free(newtyp);
    return NULL;
}

RDB_type *
RDB_join_relation_types(const RDB_type *typ1, const RDB_type *typ2,
                     RDB_exec_context *ecp)
{
    RDB_type *newtyp;

    newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }
    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->def.basetyp = RDB_union_tuple_types(typ1->def.basetyp,
            typ2->def.basetyp, ecp);
    if (newtyp->def.basetyp == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    return newtyp;
}

/* Return a pointer to the RDB_attr strcuture of the attribute with name attrname in the tuple
   type *tpltyp. */
RDB_attr *
RDB_tuple_type_attr(const RDB_type *tpltyp, const char *attrname)
{
    int i;
    
    for (i = 0; i < tpltyp->def.tuple.attrc; i++) {
        if (strcmp(tpltyp->def.tuple.attrv[i].name, attrname) == 0)
            return &tpltyp->def.tuple.attrv[i];
    }
    /* not found */
    return NULL;
}

RDB_type *
RDB_project_tuple_type(const RDB_type *typ, int attrc, char *attrv[],
                       RDB_exec_context *ecp)
{
    RDB_type *tuptyp;
    int i;

    tuptyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (tuptyp == NULL) {
        return NULL;
    }
    tuptyp->name = NULL;
    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->def.tuple.attrc = attrc;
    if (attrc > 0) {
        tuptyp->def.tuple.attrv = RDB_alloc(attrc * sizeof (RDB_attr), ecp);
        if (tuptyp->def.tuple.attrv == NULL) {
            RDB_free(tuptyp);
            return NULL;
        }
    }
    for (i = 0; i < attrc; i++)
        tuptyp->def.tuple.attrv[i].name = NULL;

    for (i = 0; i < attrc; i++) {
        RDB_attr *attrp;
        char *attrname = RDB_dup_str(attrv[i]);
        if (attrname == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }

        tuptyp->def.tuple.attrv[i].name = attrname;

        attrp = RDB_tuple_type_attr(typ, attrname);
        if (attrp == NULL) {
            RDB_raise_name(attrname, ecp);
            goto error;
        }
        tuptyp->def.tuple.attrv[i].typ = RDB_dup_nonscalar_type(attrp->typ, ecp);
        if (tuptyp->def.tuple.attrv[i].typ == NULL)
            goto error;
    }

    return tuptyp;

error:
    for (i = 0; i < attrc; i++)
        RDB_free(tuptyp->def.tuple.attrv[i].name);
    RDB_free(tuptyp->def.tuple.attrv);
    RDB_free(tuptyp);
    return NULL;
}

RDB_type *
RDB_project_relation_type(const RDB_type *typ, int attrc, char *attrv[],
                          RDB_exec_context *ecp)
{
    RDB_type *reltyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (reltyp == NULL) {
        return NULL;
    }

    reltyp->def.basetyp = RDB_project_tuple_type(typ->def.basetyp, attrc,
            attrv, ecp);
    if (reltyp->def.basetyp == NULL) {
        RDB_free(reltyp);
        return NULL;
    }
    reltyp->name = NULL;
    reltyp->kind = RDB_TP_RELATION;

    return reltyp;
}

int
RDB_find_rename_from(int renc, const RDB_renaming renv[], const char *name)
{
    int i;

    for (i = 0; i < renc && strcmp(renv[i].from, name) != 0; i++);
    if (i >= renc)
        return -1; /* not found */
    /* found */
    return i;
}

RDB_type *
RDB_rename_tuple_type(const RDB_type *typ, int renc, const RDB_renaming renv[],
        RDB_exec_context *ecp)
{
    RDB_type *newtyp;
    int i, j;

    /*
     * Check arguments
     */
    for (i = 0; i < renc; i++) {
        /* Check if source attribute exists */
        if (RDB_tuple_type_attr(typ, renv[i].from) == NULL) {
            RDB_raise_name(renv[i].from, ecp);
            return NULL;
        }

        /* Check if the dest attribute does not exist */
        if (RDB_tuple_type_attr(typ, renv[i].to) != NULL) {
            RDB_raise_name(renv[i].to, ecp);
            return NULL;
        }

        for (j = i + 1; j < renc; j++) {
            /* Check if source or dest appears twice */
            if (strcmp(renv[i].from, renv[j].from) == 0
                    || strcmp(renv[i].to, renv[j].to) == 0) {
                RDB_raise_invalid_argument("invalid RENAME arguments", ecp);
                return NULL;
            }
        }
    }

    newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->def.tuple.attrc = typ->def.tuple.attrc;
    newtyp->def.tuple.attrv = RDB_alloc (typ->def.tuple.attrc * sizeof(RDB_attr), ecp);
    if (newtyp->def.tuple.attrv == NULL) {
        goto error;
    }
    for (i = 0; i < typ->def.tuple.attrc; i++)
        newtyp->def.tuple.attrv[i].name = NULL;
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        char *attrname = typ->def.tuple.attrv[i].name;
        int ai = RDB_find_rename_from(renc, renv, attrname);

        /* check if the attribute has been renamed */
        newtyp->def.tuple.attrv[i].typ = RDB_dup_nonscalar_type(
                typ->def.tuple.attrv[i].typ, ecp);
        if (newtyp->def.tuple.attrv[i].typ == NULL)
            goto error;

        if (ai >= 0)
            newtyp->def.tuple.attrv[i].name = RDB_dup_str(renv[ai].to);
        else
            newtyp->def.tuple.attrv[i].name = RDB_dup_str(attrname);
        if (newtyp->def.tuple.attrv[i].name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
     }
     return newtyp;

error:
    if (newtyp->def.tuple.attrv != NULL) {
        for (i = 0; i < newtyp->def.tuple.attrc; i++)
            RDB_free(newtyp->def.tuple.attrv[i].name);
        RDB_free(newtyp->def.tuple.attrv);
    }

    RDB_free(newtyp);
    return NULL;
}

/**
 * Rename the attributes of the relation type *<var>typ</var> as specified by <var>renc</var>
 * and <var>renv<var> and return the new relation type.
 */
RDB_type *
RDB_rename_relation_type(const RDB_type *typ, int renc, const RDB_renaming renv[],
        RDB_exec_context *ecp)
{
    RDB_type *newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->def.basetyp = RDB_rename_tuple_type(typ->def.basetyp, renc, renv,
            ecp);
    if (newtyp->def.basetyp == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    return newtyp;
}

/*
 * Return a pointer to the RDB_attr structure that represents
 * the component. typ must be scalar.
 */
RDB_attr *
RDB_get_comp_attr(RDB_type *typ, const char *compname)
{
    int i, j;

    for (i = 0; i < typ->def.scalar.repc; i++) {
        for (j = 0; j < typ->def.scalar.repv[i].compc; j++) {
            if (strcmp(typ->def.scalar.repv[i].compv[j].name, compname) == 0)
                return &typ->def.scalar.repv[i].compv[j];
        }
    }
    return NULL;
}

static int
copy_attr(RDB_attr *dstp, const RDB_attr *srcp, RDB_exec_context *ecp)
{
    dstp->name = RDB_dup_str(srcp->name);
    if (dstp->name == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    dstp->typ = RDB_dup_nonscalar_type(srcp->typ, ecp);
    if (dstp->typ == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static RDB_type *
aggr_type(const RDB_expression *exp, const RDB_type *tpltyp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (exp->kind != RDB_EX_RO_OP) {
        RDB_raise_invalid_argument("invalid summarize argument", ecp);
        return NULL;
    }

    if (strcmp(exp->def.op.name, "count") == 0) {
        return &RDB_INTEGER;
    } else if (strcmp(exp->def.op.name, "avg") == 0) {
        return &RDB_FLOAT;
    } else if (strcmp(exp->def.op.name, "sum") == 0
            || strcmp(exp->def.op.name, "max") == 0
            || strcmp(exp->def.op.name, "min") == 0) {
        if (exp->def.op.args.firstp == NULL
                || exp->def.op.args.firstp->nextp != NULL) {
            RDB_raise_invalid_argument("invalid number of aggregate arguments", ecp);
            return NULL;
        }
        return RDB_expr_type_tpltyp(exp->def.op.args.firstp, tpltyp, NULL, ecp, txp);
    } else if (strcmp(exp->def.op.name, "any") == 0
            || strcmp(exp->def.op.name, "all") == 0) {
        return &RDB_BOOLEAN;
    }
    RDB_raise_operator_not_found(exp->def.op.name, ecp);
    return NULL;
}

RDB_type *
RDB_summarize_type(RDB_expr_list *expsp,
        int avgc, char **avgv, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_type *newtyp;
    int addc, attrc;
    RDB_expression *argp;
    RDB_attr *attrv = NULL;
    RDB_type *tb1typ = NULL;
    RDB_type *tb2typ = NULL;
    int expc = RDB_expr_list_length(expsp);

    if (expc < 2 || (expc % 2) != 0) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return NULL;
    }

    addc = (expc - 2) / 2;
    attrc = addc + avgc;

    tb1typ = RDB_expr_type(expsp->firstp, NULL, NULL, NULL, ecp, txp);
    if (tb1typ == NULL)
        return NULL;
    tb2typ = RDB_expr_type(expsp->firstp->nextp, NULL, NULL, NULL, ecp, txp);
    if (tb2typ == NULL)
        goto error;
   
    if (tb2typ->kind != RDB_TP_RELATION) {
        RDB_raise_invalid_argument("relation type required", ecp);
        goto error;
    }

    attrv = RDB_alloc(sizeof (RDB_attr) * attrc, ecp);
    if (attrv == NULL) {
        goto error;
    }

    argp = expsp->firstp->nextp->nextp;
    for (i = 0; i < addc; i++) {
        attrv[i].typ = aggr_type(argp, tb1typ->def.basetyp,
                ecp, txp);
        if (attrv[i].typ == NULL)
            goto error;
        if (argp->nextp->kind != RDB_EX_OBJ) {
            RDB_raise_invalid_argument("invalid SUMMARIZE argument", ecp);
            goto error;
        }
        attrv[i].name = RDB_obj_string(&argp->nextp->def.obj);
        if (attrv[i].name == NULL) {
            RDB_raise_invalid_argument("invalid SUMMARIZE argument", ecp);
            goto error;
        }
        argp = argp->nextp->nextp;
    }
    for (i = 0; i < avgc; i++) {
        attrv[addc + i].name = avgv[i];
        attrv[addc + i].typ = &RDB_INTEGER;
    }

    newtyp = RDB_extend_relation_type(tb2typ, attrc, attrv, ecp);
    if (newtyp == NULL) {
        goto error;
    }

    RDB_free(attrv);
    return newtyp;

error:
    RDB_free(attrv);    
    return NULL;
}

RDB_type *
RDB_wrap_tuple_type(const RDB_type *typ, int wrapc, const RDB_wrapping wrapv[],
        RDB_exec_context *ecp)
{
    int i, j, k;
    int ret;
    RDB_attr *attrp;
    int attrc;
    RDB_type *newtyp;

    /* Compute # of attributes */
    attrc = typ->def.tuple.attrc;
    for (i = 0; i < wrapc; i++)
        attrc += 1 - wrapv[i].attrc;

    newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->def.tuple.attrc = attrc;
    newtyp->def.tuple.attrv = RDB_alloc(attrc * sizeof(RDB_attr), ecp);
    if (newtyp->def.tuple.attrv == NULL) {
        RDB_free(newtyp);
        return NULL;
    }

    for (i = 0; i < attrc; i++) {
        newtyp->def.tuple.attrv[i].name = NULL;
        newtyp->def.tuple.attrv[i].typ = NULL;
    }

    /*
     * Copy attributes from wrapv
     */
    for (i = 0; i < wrapc; i++) {
        RDB_type *tuptyp = RDB_alloc(sizeof(RDB_type), ecp);
        if (tuptyp == NULL) {
            goto error;
        }
        tuptyp->name = NULL;
        tuptyp->kind = RDB_TP_TUPLE;
        tuptyp->def.tuple.attrc = wrapv[i].attrc;
        tuptyp->def.tuple.attrv = RDB_alloc(sizeof(RDB_attr) * wrapv[i].attrc, ecp);
        if (tuptyp->def.tuple.attrv == NULL) {
            RDB_free(tuptyp);
            goto error;
        }

        for (j = 0; j < wrapv[i].attrc; j++) {
            attrp = RDB_tuple_type_attr(typ, wrapv[i].attrv[j]);
            if (attrp == NULL) {
                RDB_raise_name(wrapv[i].attrv[j], ecp);
                RDB_free(tuptyp->def.tuple.attrv);
                RDB_free(tuptyp);
                goto error;
            }

            ret = copy_attr(&tuptyp->def.tuple.attrv[j], attrp, ecp);
            if (ret != RDB_OK) {
                RDB_free(tuptyp->def.tuple.attrv);
                RDB_free(tuptyp);
                goto error;
            }
        }
        newtyp->def.tuple.attrv[i].name = RDB_dup_str(wrapv[i].attrname);
        if (newtyp->def.tuple.attrv[i].name == NULL)
            goto error;

        newtyp->def.tuple.attrv[i].typ = tuptyp;
    }

    /*
     * Copy remaining attributes
     */
    k = wrapc;
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        /* Copy attribute if it does not appear in wrapv */
        for (j = 0; j < wrapc && RDB_find_str(wrapv[j].attrc, wrapv[j].attrv,
                typ->def.tuple.attrv[i].name) == -1; j++);
        if (j == wrapc) {
            /* Not found */
            ret = copy_attr(&newtyp->def.tuple.attrv[k],
                    &typ->def.tuple.attrv[i], ecp);
            if (ret != RDB_OK)
                goto error;
            k++;
        }
    }
    if (k != attrc) {
        RDB_raise_invalid_argument("invalid WRAP", ecp);
        goto error;
    }

    return newtyp;

error:
    for (i = 0; i < attrc; i++) {
        attrp = &newtyp->def.tuple.attrv[i];
        if (attrp->name != NULL)
            RDB_free(attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ == NULL && !RDB_type_is_scalar(attrp->typ))
                RDB_del_nonscalar_type(attrp->typ, ecp);
        }
    }
    RDB_free(newtyp->def.tuple.attrv);
    RDB_free(newtyp);
    return NULL;
}

RDB_type *
RDB_wrap_relation_type(const RDB_type *typ, int wrapc,
        const RDB_wrapping wrapv[], RDB_exec_context *ecp)
{
    RDB_type *newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->def.basetyp = RDB_wrap_tuple_type(typ->def.basetyp, wrapc, wrapv,
            ecp);
    if (newtyp->def.basetyp == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    return newtyp;
}

RDB_type *
RDB_unwrap_tuple_type(const RDB_type *typ, int attrc, char *attrv[],
        RDB_exec_context *ecp)
{
    int nattrc;
    int i, j, k;
    int ret;
    RDB_attr *attrp;
    RDB_type *newtyp;

    /* Compute # of attributes */
    nattrc = typ->def.tuple.attrc;
    for (i = 0; i < attrc; i++) {
        RDB_type *tuptyp = RDB_type_attr_type(typ, attrv[i]);
        if (tuptyp == NULL) {
            RDB_raise_name(attrv[i], ecp);
            goto error;
        }
        if (tuptyp->kind != RDB_TP_TUPLE) {
            RDB_raise_invalid_argument("not a tuple", ecp);
            goto error;
        }        
        nattrc += tuptyp->def.tuple.attrc - 1;
    }

    newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->def.tuple.attrc = nattrc;
    newtyp->def.tuple.attrv = RDB_alloc(nattrc * sizeof(RDB_attr), ecp);
    if (newtyp->def.tuple.attrv == NULL) {
        RDB_free(newtyp);
        return NULL;
    }

    for (i = 0; i < nattrc; i++) {
        newtyp->def.tuple.attrv[i].name = NULL;
        newtyp->def.tuple.attrv[i].typ = NULL;
    }

    k = 0;

    /* Copy sub-attributes of attrv */
    for (i = 0; i < attrc; i++) {
        RDB_type *tuptyp = RDB_type_attr_type(typ, attrv[i]);

        for (j = 0; j < tuptyp->def.tuple.attrc; j++) {
            ret = copy_attr(&newtyp->def.tuple.attrv[k],
                    &tuptyp->def.tuple.attrv[j], ecp);
            if (ret != RDB_OK)
                goto error;
            k++;
        }
    }

    /* Copy remaining attributes */
    for (i = 0; i < typ->def.tuple.attrc; i++) {
        /* Copy attribute if it does not appear in attrv */
        if (RDB_find_str(attrc, attrv, typ->def.tuple.attrv[i].name) == -1) {
            ret = copy_attr(&newtyp->def.tuple.attrv[k],
                    &typ->def.tuple.attrv[i], ecp);
            if (ret != RDB_OK)
                goto error;
            k++;
        }
    }

    if (k != nattrc) {
        RDB_raise_invalid_argument("invalid UNWRAP", ecp);
        goto error;
    }

    return newtyp;

error:
    for (i = 0; i < attrc; i++) {
        attrp = &newtyp->def.tuple.attrv[i];
        if (attrp->name != NULL)
            free (attrp->name);
        if (attrp->typ != NULL) {
            if (!RDB_type_is_scalar(attrp->typ))
                RDB_del_nonscalar_type(attrp->typ, ecp);
        }
    }
    RDB_free(newtyp->def.tuple.attrv);
    RDB_free(newtyp);
    return NULL;
}    

RDB_type *
RDB_unwrap_relation_type(const RDB_type *typ, int attrc, char *attrv[],
        RDB_exec_context *ecp)
{
    RDB_type *newtyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (newtyp == NULL) {
        return NULL;
    }

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    newtyp->def.basetyp = RDB_unwrap_tuple_type(typ->def.basetyp, attrc, attrv,
            ecp);
    if (newtyp->def.basetyp == NULL) {
        RDB_free(newtyp);
        return NULL;
    }
    return newtyp;
}

RDB_type *
RDB_group_type(RDB_type *typ, int attrc, char *attrv[], const char *gattr,
        RDB_exec_context *ecp)
{
    int i, j;
    int ret;
    RDB_attr *rtattrv;
    RDB_attr *attrp;
    RDB_type *tuptyp;
    RDB_type *gattrtyp;
    RDB_type *newtyp;

    /*
     * Create relation type for attribute gattr
     */
    rtattrv = RDB_alloc(sizeof(RDB_attr) * attrc, ecp);
    if (rtattrv == NULL) {
        return NULL;
    }
    for (i = 0; i < attrc; i++) {
        attrp = RDB_tuple_type_attr(typ->def.basetyp, attrv[i]);
        if (attrp == NULL) {
            RDB_free(rtattrv);
            RDB_raise_name(attrv[i], ecp);
            return NULL;
        }
        rtattrv[i].typ = attrp->typ;
        rtattrv[i].name = attrp->name;
    }
    gattrtyp = RDB_new_relation_type(attrc, rtattrv, ecp);
    RDB_free(rtattrv);
    if (gattrtyp == NULL)
        return NULL;

    /*
     * Create tuple type
     */
    tuptyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (tuptyp == NULL) {
        RDB_del_nonscalar_type(gattrtyp, ecp);
        return NULL;
    }

    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->name = NULL;
    tuptyp->def.tuple.attrc = typ->def.basetyp->def.tuple.attrc + 1 - attrc;
    tuptyp->def.tuple.attrv = RDB_alloc(tuptyp->def.tuple.attrc * sizeof(RDB_attr), ecp);
    if (tuptyp->def.tuple.attrv == NULL) {
        RDB_free(tuptyp);
        RDB_del_nonscalar_type(gattrtyp, ecp);
        return NULL;
    }

    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        tuptyp->def.tuple.attrv[i].name = NULL;
        tuptyp->def.tuple.attrv[i].typ = NULL;
    }

    j = 0;
    for (i = 0; i < typ->def.basetyp->def.tuple.attrc; i++) {
        char *attrname = typ->def.basetyp->def.tuple.attrv[i].name;

        if (RDB_tuple_type_attr(gattrtyp->def.basetyp, attrname) == NULL) {
            if (strcmp(attrname, gattr) == 0) {
                RDB_del_nonscalar_type(gattrtyp, ecp);
                RDB_raise_invalid_argument("invalid GROUP", ecp);
                goto error;
            }
            ret = copy_attr(&tuptyp->def.tuple.attrv[j],
                    &typ->def.basetyp->def.tuple.attrv[i], ecp);
            if (ret != RDB_OK) {
                RDB_del_nonscalar_type(gattrtyp, ecp);
                goto error;
            }
            j++;
        }
    }
    tuptyp->def.tuple.attrv[j].typ = gattrtyp;
    tuptyp->def.tuple.attrv[j].name = RDB_dup_str(gattr);
    if (tuptyp->def.tuple.attrv[j].name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    /*
     * Create relation type
     */
    newtyp = RDB_alloc(sizeof(RDB_type), ecp);
    if (newtyp == NULL) {
        goto error;
    }
    newtyp->kind = RDB_TP_RELATION;
    newtyp->name = NULL;
    newtyp->def.basetyp = tuptyp;

    return newtyp;

error:
    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        RDB_attr *attrp = &tuptyp->def.tuple.attrv[i];
        if (attrp->name != NULL)
            RDB_free(attrp->name);
        if (attrp->typ != NULL) {
            if (!RDB_type_is_scalar(attrp->typ))
                RDB_del_nonscalar_type(attrp->typ, ecp);
        }
    }
    RDB_free(tuptyp->def.tuple.attrv);
    RDB_free(tuptyp);

    return NULL;
}

RDB_type *
RDB_ungroup_type(RDB_type *typ, const char *attr, RDB_exec_context *ecp)
{
    int i, j;
    int ret;
    RDB_type *tuptyp;
    RDB_type *newtyp;
    RDB_attr *relattrp = RDB_tuple_type_attr(typ->def.basetyp, attr);

    if (relattrp == NULL) {
        RDB_raise_name(attr, ecp);
        return NULL;
    }
    if (relattrp->typ->kind != RDB_TP_RELATION) {
        RDB_raise_invalid_argument("attribute is not a relation", ecp);
        return NULL;
    }

    tuptyp = RDB_alloc(sizeof (RDB_type), ecp);
    if (tuptyp == NULL) {
        return NULL;
    }

    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->name = NULL;

    /* Compute # of attributes */
    tuptyp->def.tuple.attrc = typ->def.basetyp->def.tuple.attrc
            + relattrp->typ->def.basetyp->def.tuple.attrc - 1;

    /* Allocate tuple attributes */
    tuptyp->def.tuple.attrv = RDB_alloc(tuptyp->def.tuple.attrc * sizeof(RDB_attr), ecp);
    if (tuptyp->def.tuple.attrv == NULL) {
        RDB_free(tuptyp);
        return NULL;
    }

    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        tuptyp->def.tuple.attrv[i].name = NULL;
        tuptyp->def.tuple.attrv[i].typ = NULL;
    }

    /* Copy attributes from the original type */
    j = 0;
    for (i = 0; i < typ->def.basetyp->def.tuple.attrc; i++) {
        if (strcmp(typ->def.basetyp->def.tuple.attrv[i].name,
                attr) != 0) {
            ret = copy_attr(&tuptyp->def.tuple.attrv[j],
                    &typ->def.basetyp->def.tuple.attrv[i], ecp);
            if (ret != RDB_OK)
                goto error;
            j++;
        }
    }

    /* Copy attributes from the attribute type */
    for (i = 0; i < relattrp->typ->def.basetyp->def.tuple.attrc; i++) {
        char *attrname = relattrp->typ->def.basetyp->def.tuple.attrv[i].name;

        /* Check for attribute name clash */
        if (strcmp(attrname, attr) != 0
                && RDB_tuple_type_attr(typ->def.basetyp, attrname)
                != NULL) {
            RDB_raise_invalid_argument("invalid UNGROUP", ecp);
            goto error;
        }
        ret = copy_attr(&tuptyp->def.tuple.attrv[j],
                    &relattrp->typ->def.basetyp->def.tuple.attrv[i], ecp);
        if (ret != RDB_OK)
            goto error;
        j++;
    }

    /*
     * Create relation type
     */
    newtyp = RDB_alloc(sizeof(RDB_type), ecp);
    if (newtyp == NULL) {
        goto error;
    }
    newtyp->kind = RDB_TP_RELATION;
    newtyp->name = NULL;
    newtyp->def.basetyp = tuptyp;

    return newtyp;    

error:
    for (i = 0; i < tuptyp->def.tuple.attrc; i++) {
        RDB_attr *attrp = &tuptyp->def.tuple.attrv[i];
        if (attrp->name != NULL)
            RDB_free(attrp->name);
        if (attrp->typ != NULL) {
            if (!RDB_type_is_scalar(attrp->typ))
                RDB_del_nonscalar_type(attrp->typ, ecp);
        }
    }
    RDB_free(tuptyp->def.tuple.attrv);
    RDB_free(tuptyp);

    return NULL;
}

/**
 * Assume the attributes were ordered by the alphabetical order of their names.
 * Return the next attribute after *lastname.
 * If lastname is NULL, return the first.
 */
int
RDB_next_attr_sorted(const RDB_type *typ, const char *lastname) {
    int i;
    int attridx = -1;

    for (i = 0; i < typ->def.tuple.attrc; i++) {
        if ((lastname == NULL
                    || strcmp(typ->def.tuple.attrv[i].name, lastname) > 0)
                && (attridx == -1
                    || strcmp(typ->def.tuple.attrv[i].name,
                            typ->def.tuple.attrv[attridx].name) < 0)) {
            attridx = i;
        }
    }
    return attridx;
}

RDB_bool
array_matches_type(RDB_object *arrp, RDB_type *typ)
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
RDB_obj_matches_type(RDB_object *objp, RDB_type *typ)
{
    int i;
    RDB_object *attrobjp;
    RDB_type *objtyp = RDB_obj_type(objp);
    if (objtyp != NULL) {
        return RDB_type_equals(objtyp, typ);
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
