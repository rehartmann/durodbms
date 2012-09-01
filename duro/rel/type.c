/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include "catalog.h"
#include "serialize.h"
#include "io.h"
#include <gen/strfns.h>

#include <string.h>
#include <locale.h>

static int
del_type(RDB_type *typ, RDB_exec_context *ecp)
{
    int ret = RDB_OK;

    if (RDB_type_is_scalar(typ)) {
        RDB_free(typ->name);
        if (typ->def.scalar.repc > 0) {
            int i, j;
            
            for (i = 0; i < typ->def.scalar.repc; i++) {
                for (j = 0; j < typ->def.scalar.repv[i].compc; j++) {
                    RDB_free(typ->def.scalar.repv[i].compv[j].name);
                }
                RDB_free(typ->def.scalar.repv[i].compv);
            }
            RDB_free(typ->def.scalar.repv);
        }
        if (typ->def.scalar.arep != NULL
                && !RDB_type_is_scalar(typ->def.scalar.arep)) {
            ret = RDB_del_nonscalar_type(typ->def.scalar.arep, ecp);
        }
        RDB_free(typ);
    } else {
        ret = RDB_del_nonscalar_type(typ, ecp);
    }
    return ret;
}    

static RDB_possrep *
RDB_get_possrep(const RDB_type *typ, const char *repname)
{
    int i;

    if (!RDB_type_is_scalar(typ))
        return NULL;
    for (i = 0; i < typ->def.scalar.repc
            && strcmp(typ->def.scalar.repv[i].name, repname) != 0;
            i++);
    if (i >= typ->def.scalar.repc)
        return NULL;
    return &typ->def.scalar.repv[i];
}

/**
 * Implements a system-generated selector
 */
int
RDB_sys_select(int argc, RDB_object *argv[], RDB_operator *op, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_possrep *prp;

    /* Find possrep */
    prp = RDB_get_possrep(op->rtyp, op->name);
    if (prp == NULL) {
        RDB_raise_invalid_argument("component name is NULL", ecp);
        return RDB_ERROR;
    }

    /* If *retvalp carries a value, it must match the type */
    if (retvalp->kind != RDB_OB_INITIAL
            && (retvalp->typ == NULL
                || !RDB_type_equals(retvalp->typ, op->rtyp))) {
        RDB_raise_type_mismatch("invalid selector return type", ecp);
        return RDB_ERROR;
    }

    if (argc == 1) {
        /* Copy value */
        if (RDB_copy_obj_data(retvalp, argv[0], ecp, NULL) != RDB_OK)
            return RDB_ERROR;
    } else {
        /* Copy tuple attributes */
        int i;

        for (i = 0; i < argc; i++) {
            if (RDB_tuple_set(retvalp, op->rtyp->def.scalar.repv[0].compv[i].name,
                    argv[i], ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
    }
    retvalp->typ = op->rtyp;
    return RDB_OK;
}

/** @addtogroup type Type functions
 * @{
 */

/**
 * RDB_type_is_numeric checks if a type is numeric.

@returns

RDB_TRUE if the type is INTEGER or FLOAT, RDB_FALSE otherwise.
 */
RDB_bool
RDB_type_is_numeric(const RDB_type *typ) {
    return (RDB_bool)(typ == &RDB_INTEGER || typ == &RDB_FLOAT);
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

static int
load_getter(RDB_type *typ, const char *compname, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object opnameobj;
    RDB_int cnt;

    RDB_init_obj(&opnameobj);
    if (RDB_string_to_obj(&opnameobj, typ->name, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&opnameobj, "_get_", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&opnameobj, compname, ecp) != RDB_OK)
        goto error;

    cnt = RDB_cat_load_ro_op(RDB_obj_string(&opnameobj), ecp, txp);
    if (cnt == (RDB_int) RDB_ERROR)
        goto error;
    return RDB_destroy_obj(&opnameobj, ecp);

error:
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

static int
load_setter(RDB_type *typ, const char *compname, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object opnameobj;
    RDB_int cnt;

    RDB_init_obj(&opnameobj);
    if (RDB_string_to_obj(&opnameobj, typ->name, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&opnameobj, "_set_", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&opnameobj, compname, ecp) != RDB_OK)
        goto error;

    cnt = RDB_cat_load_upd_op(RDB_obj_string(&opnameobj), ecp, txp);
    if (cnt == (RDB_int) RDB_ERROR)
        goto error;
    return RDB_destroy_obj(&opnameobj, ecp);

error:
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

/*
 * Load selector, getters and setters of user-defined type
 */
static int
load_type_ops(RDB_type *typ, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;

    for (i = 0; i < typ->def.scalar.repc; i++) {
        int j;

        if (RDB_cat_load_ro_op(typ->def.scalar.repv[i].name, ecp, txp)
                == (RDB_int) RDB_ERROR)
            return RDB_ERROR;

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
    return RDB_OK;
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

    if (load_type_ops(typ, ecp, txp) != RDB_OK)
        return NULL;

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
 *
RDB_define_type defines a type with the name <var>name</var> and
<var>repc</var> possible representations.
The individual possible representations are
described by the elements of <var>repv</var>.

If <var>constraintp</var> is not NULL, it specifies the type constraint.
When the constraint is evaluated, the value to check is made available
as an attribute with the same name as the type.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>element_exists_error
<dd>There is already a type with name <var>name</var>.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_define_type(const char *name, int repc, const RDB_possrep repv[],
                RDB_expression *constraintp, RDB_exec_context *ecp,
                RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object conval;
    RDB_object typedata;
    int i, j;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);
    RDB_init_obj(&conval);
    RDB_init_obj(&typedata);

    if (RDB_binary_set(&typedata, 0, NULL, 0, ecp) != RDB_OK)
        return RDB_ERROR;

    /*
     * Insert tuple into sys_types
     */

    if (RDB_tuple_set_string(&tpl, "typename", name, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set(&tpl, "i_arep_type", &typedata, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set_int(&tpl, "i_arep_len", RDB_NOT_IMPLEMENTED, ecp)
            != RDB_OK)
        goto error;
    if (RDB_tuple_set_bool(&tpl, "i_sysimpl", RDB_FALSE, ecp) != RDB_OK)
        goto error;

    /* Store constraint in tuple */
    if (RDB_expr_to_binobj(&conval, constraintp, ecp) != RDB_OK)
        goto error;
    if (RDB_tuple_set(&tpl, "i_constraint", &conval, ecp) != RDB_OK)
        goto error;

    if (RDB_insert(txp->dbp->dbrootp->types_tbp, &tpl, ecp, txp) != RDB_OK)
        goto error;

    /*
     * Insert tuple into sys_possreps
     */   

    for (i = 0; i < repc; i++) {
        char *prname = repv[i].name;

        if (prname == NULL) {
            /* Possrep name may be NULL if there's only 1 possrep */
            if (repc > 1) {
                RDB_raise_invalid_argument("possrep name is NULL", ecp);
                goto error;
            }
            /* Make type name the possrep name */
            prname = (char *) name;
        }
        if (RDB_tuple_set_string(&tpl, "possrepname", prname, ecp) != RDB_OK)
            goto error;

        for (j = 0; j < repv[i].compc; j++) {
            char *cname = repv[i].compv[j].name;

            if (cname == NULL) {
                if (repv[i].compc > 1) {
                    RDB_raise_invalid_argument("component name is NULL", ecp);
                    goto error;
                }
                cname = prname;
            }
            if (RDB_tuple_set_int(&tpl, "compno", (RDB_int)j, ecp) != RDB_OK)
                goto error;
            if (RDB_tuple_set_string(&tpl, "compname", cname, ecp) != RDB_OK)
                goto error;

            if (RDB_type_to_binobj(&typedata, repv[i].compv[j].typ,
                    ecp) != RDB_OK)
                goto error;
            if (RDB_tuple_set(&tpl, "comptype", &typedata, ecp) != RDB_OK)
                goto error;

            if (RDB_insert(txp->dbp->dbrootp->possrepcomps_tbp, &tpl, ecp, txp)
                    != RDB_OK)
                goto error;
        }
    }

    RDB_destroy_obj(&typedata, ecp);
    RDB_destroy_obj(&conval, ecp);    
    RDB_destroy_obj(&tpl, ecp);

    return RDB_OK;
    
error:
    RDB_destroy_obj(&typedata, ecp);
    RDB_destroy_obj(&conval, ecp);    
    RDB_destroy_obj(&tpl, ecp);

    return RDB_ERROR;
}

static int
create_selector(RDB_type *typ, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    int compc = typ->def.scalar.repv[0].compc;
    RDB_parameter *paramv = RDB_alloc(sizeof(RDB_parameter) * compc, ecp);
    if (paramv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < compc; i++)
        paramv[i].typ = typ->def.scalar.repv[0].compv[i].typ;
    ret = RDB_create_ro_op(typ->def.scalar.repv[0].name, compc, paramv, typ,
            "", "RDB_sys_select", typ->name,
            ecp, txp);
    RDB_free(paramv);
    return ret;
}

/**
 * Check if the operator *<var>op</var> is a selector.
 */
RDB_bool
RDB_is_selector(const RDB_operator *op)
{
    if (op->rtyp == NULL)
        return RDB_FALSE; /* Not a read-only operator */

    /* Check if there is a possrep with the same name as the operator */
    return (RDB_bool) (RDB_get_possrep(op->rtyp, RDB_operator_name(op)) != NULL);
}

/** @defgroup typeimpl Type implementation functions
 * \#include <rel/typeimpl.h>
 * @{
 */

/**
 * RDB_implement_type implements the user-defined type with name
<var>name</var>. The type must have been defined previously using
RDB_define_type(). After RDB_implement_type was inkoved successfully,
this type may be used for local variables and table attributes.

If <var>arep</var> is not NULL, it must point to a type which is used
as the physical representation. The getter, setter, and selector operators
must be provided by the caller.

If <var>arep</var> is NULL and <var>areplen</var> is not -1,
<var>areplen</var> specifies the length, in bytes,
of the physical representation, which then is a fixed-length array of bytes.
The getter, setter, and selector operators must be provided by the caller.
RDB_irep_to_obj() can be used by the selector to assign a value to an RDB_object.
RDB_obj_irep() can be used by setters and getters to access the actual representation.

If <var>arep</var> is NULL and <var>areplen</var> is -1,
the getter and setter operators and the selector operator are provided by Duro.
In this case, the type must have exactly one possible representation.
If this representation has exactly one property, the type of this representation will become
the physical representation. Otherwise the type will be represented by a tuple type with
one attribute for each property.

For user-provided setters, getters, and selectors,
the following conventions apply:

<dl>
<dt>Selectors
<dd>A selector is a read-only operator whose name is is the name of a possible
representation. It takes one argument for each component.
<dt>Getters
<dd>A getter is a read-only operator whose name consists of the
type and a component name, separated by "_get_".
It takes one argument. The argument must be of the user-defined type in question.
The return type must be the component type.
<dt>Setters
<dd>A setter is an update operator whose name consists of the
type and a component name, separated by "_set_".
It takes two arguments. The first argument is an update argument
and must be of the user-defined type in question.
The second argument is read-only and must be of the type of
the component.
</dl>

A user-defined comparison operator <code>cmp</code> returning an
<code>integer</code> may be supplied.
<code>cmp</code> must have two arguments, both of the user-defined type
for which the comparison is to be defined.

<code>cmp</code> must return -1, 0, or 1 if the first argument is lower than,
equal to, or greater than the second argument, respectively.

If <code>cmp</code> has been defined, it will be called by the built-in comparison
operators =, <>, <= etc. 

@returns

On success, RDB_OK is returned. Any other return value indicates an error.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>txp</var> is not a running transaction.
<dt>not_found_error
<dd>The type has not been previously defined.
<dt>invalid_argument_error
<dd><var>arep</var> is NULL and <var>areplen</var> is -1,
and the type was defined with more than one possible representation.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_implement_type(const char *name, RDB_type *arep, RDB_int areplen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *exp, *wherep, *argp;
    RDB_attr_update upd[3];
    RDB_object typedata;
    int ret;
    int i;
    RDB_type *typ = NULL;
    RDB_bool sysimpl = (arep == NULL) && (areplen == -1);

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (sysimpl) {
        /*
         * No actual rep given, so selector and getters/setters must be provided
         * by the system
         */
        int compc;

        typ = RDB_get_type(name, ecp, txp);
        if (typ == NULL)
            return RDB_ERROR;

        /* # of possreps must be one */
        if (typ->def.scalar.repc != 1) {
            RDB_raise_invalid_argument("invalid # of possreps", ecp);
            return RDB_ERROR;
        }

        compc = typ->def.scalar.repv[0].compc;
        if (compc == 1) {
            arep = typ->def.scalar.repv[0].compv[0].typ;
        } else {
            /* More than one component, so internal rep is a tuple */
            arep = RDB_new_tuple_type(typ->def.scalar.repv[0].compc,
                    typ->def.scalar.repv[0].compv, ecp);
            if (arep == NULL)
                return RDB_ERROR;
        }

        typ->def.scalar.arep = arep;
        typ->def.scalar.sysimpl = sysimpl;
        typ->ireplen = arep->ireplen;

        ret = create_selector(typ, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }

    /*
     * Update catalog
     */

    exp = RDB_var_ref("typename", ecp);
    if (exp == NULL) {
        return RDB_ERROR;
    }
    wherep = RDB_ro_op("=", ecp);
    if (wherep == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, exp);
    argp = RDB_string_to_expr(name, ecp);
    if (argp == NULL) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, argp);

    upd[0].exp = upd[1].exp = upd[2].exp = NULL;

    upd[0].name = "i_arep_len";
    upd[0].exp = RDB_int_to_expr(arep == NULL ? areplen : arep->ireplen, ecp);
    if (upd[0].exp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    upd[1].name = "i_sysimpl";
    upd[1].exp = RDB_bool_to_expr(sysimpl, ecp);
    if (upd[1].exp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    if (arep != NULL) {
        RDB_init_obj(&typedata);
        ret = RDB_type_to_binobj(&typedata, arep, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&typedata, ecp);
            goto cleanup;
        }

        upd[2].name = "i_arep_type";
        upd[2].exp = RDB_obj_to_expr(&typedata, ecp);
        RDB_destroy_obj(&typedata, ecp);
        if (upd[2].exp == NULL) {
            RDB_raise_no_memory(ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
    }

    ret = RDB_update(txp->dbp->dbrootp->types_tbp, wherep,
            arep != NULL ? 3 : 2, upd, ecp, txp);
    if (ret != RDB_ERROR)
        ret = RDB_OK;

    if (typ == NULL) {
        typ = RDB_get_type(name, ecp, txp);
        if (typ == NULL)
            return RDB_ERROR;
    }

    /* Load selector etc. */
    ret = load_type_ops(typ, ecp, txp);

cleanup:    
    for (i = 0; i < 3; i++) {
        if (upd[i].exp != NULL)
            RDB_drop_expr(upd[i].exp, ecp);
    }
    RDB_drop_expr(wherep, ecp);

    return ret;
}

/*@}*/

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
        default:
            RDB_raise_invalid_argument("type is scalar", ecp);
            ret = RDB_ERROR;
    }
    typ->kind = (enum RDB_tp_kind) -1; /* for error detection */
    RDB_free(typ);
    return ret;
}

/**
 * Delete the user-defined type with name specified by <var>name</var>.

It is not possible to destroy built-in types.

@returns

On success, RDB_OK is returned. Any other return value indicates an error.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>*<var>txp</var> is not a running transaction.
<dt>name_error
<dd>A type with name <var>name</var> was not found.
<dt>invalid_argument_error
<dd>The type is not user-defined.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_drop_type(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_int cnt;
    RDB_type *typ;
    RDB_expression *wherep, *argp;
    RDB_type *ntp = NULL;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Check if the type is a built-in type
     */
    if (RDB_hashmap_get(&RDB_builtin_type_map, name) != NULL) {
        RDB_raise_invalid_argument("cannot drop a built-in type", ecp);
        return RDB_ERROR;
    }

    /* !! should check if the type is still used by a table */

    /* Delete selector */
    ret = RDB_drop_op(name, ecp, txp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR) {
            return RDB_ERROR;
        }
        RDB_clear_err(ecp);
    }


    /* Delete type from database */
    wherep = RDB_ro_op("=", ecp);
    if (wherep == NULL) {
        return RDB_ERROR;
    }
    argp = RDB_var_ref("typename", ecp);
    if (argp == NULL) {
        RDB_drop_expr(wherep, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, argp);
    argp = RDB_string_to_expr(name, ecp);
    if (argp == NULL) {
        RDB_drop_expr(wherep, ecp);
        return RDB_ERROR;
    }
    RDB_add_arg(wherep, argp);

    cnt = RDB_delete(txp->dbp->dbrootp->types_tbp, wherep, ecp, txp);
    if (cnt == 0) {
        RDB_raise_name("type not found", ecp);
        return RDB_ERROR;
    }
    if (cnt == (RDB_int) RDB_ERROR) {
        RDB_drop_expr(wherep, ecp);
        return RDB_ERROR;
    }
    cnt = RDB_delete(txp->dbp->dbrootp->possrepcomps_tbp, wherep, ecp,
            txp);
    if (cnt == (RDB_int) RDB_ERROR) {
        RDB_drop_expr(wherep, ecp);
        return ret;
    }

    /*
     * Delete type in memory, if it's in the type map
     */
    typ = RDB_hashmap_get(&txp->dbp->dbrootp->typemap, name);
    if (typ != NULL) {
        /* Delete type from type map by puting a NULL pointer into it */
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->typemap, name, ntp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        /*
         * Delete RDB_type struct last because name may be identical
         * to typ->name
         */
        if (del_type(typ, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    return RDB_OK;
}

/**
 * RDB_type_equals checks if two types are equal.

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
 * RDB_type_name returns the name of a type.

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
    tuptyp->def.tuple.attrv = RDB_alloc(attrc * sizeof (RDB_attr), ecp);
    if (tuptyp->def.tuple.attrv == NULL) {
        RDB_free(tuptyp);
        return NULL;
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

RDB_attr *
RDB_get_icomp(RDB_type *typ, const char *compname)
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
        return RDB_expr_type_tpltyp(exp->def.op.args.firstp, tpltyp, ecp, txp);
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

    tb1typ = RDB_expr_type(expsp->firstp, NULL, NULL, ecp, txp);
    if (tb1typ == NULL)
        return NULL;
    tb2typ = RDB_expr_type(expsp->firstp->nextp, NULL, NULL, ecp, txp);
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
