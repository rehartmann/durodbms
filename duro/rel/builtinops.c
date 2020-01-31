/*
 * Built-in read-only operators.
 *
 * Copyright (C) 2009, 2011-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "tostr.h"
#include "serialize.h"
#include <obj/objinternal.h>
#include <obj/builtinscops.h>
#include <obj/datetimeops.h>

#include <stdlib.h>

RDB_op_map RDB_builtin_ro_op_map;

RDB_op_map RDB_builtin_upd_op_map;

/**

@page relops Built-in relational and related operators

<h3 id="op_relation">OPERATOR RELATION</h3>

OPERATOR RELATION(t TUPLE { * }, ...) RETURNS RELATION { * };

<h4>Description</h4>

The relation selector.

<hr>

<h3 id="op_is_empty">OPERATOR is_empty</h3>

OPERATOR is_empty(RELATION { * }) RETURNS boolean;

<h4>Description</h4>

Checks if a table is empty.

<h4>Return value</h4>

TRUE if the relation-valued operand is empty, RDB_FALSE
otherwise.

<hr>

<h3 id="op_count">OPERATOR count</h3>

OPERATOR count(RELATION { * }) RETURNS integer;

<h4>Description</h4>

Counts the tuples in a table.

<h4>Return value</h4>

The cardinality of the relation-valued operand.

<hr>

<h3 id="op_in">OPERATOR in</h3>

OPERATOR in (t TUPLE { * }, r RELATION { * }) RETURNS boolean;

<h4>Description</h4>

Checks if a table contains a given tuple.

<h4>Return value</h4>

TRUE if @a r contains @a t, RDB_FALSE otherwise.

<hr>

<h3 id="op_subset_of">OPERATOR subset_of</h3>

OPERATOR subset_of(r1 RELATION { * }, r2 RELATION { * }) RETURNS boolean;

<h4>Description</h4>

Checks if a table is a subset of another table.

<h4>Return value</h4>

TRUE if the @a r1 is a subset of @a r2, RDB_FALSE otherwise.

<hr>

<h3 id="op_any">OPERATOR any</h3>

OPERATOR any(r RELATION { * }, attr boolean) RETURNS boolean;

<h4>Description</h4>

The <code>any</code> aggregate operator.
For the semantics, see RDB_any().

<hr>

<h3 id="op_all">OPERATOR all</h3>

OPERATOR all(r RELATION { * }, attr boolean) RETURNS boolean;

<h4>Description</h4>

The <code>all</code> aggregate operator.
For the semantics, see RDB_all().

<hr>

<h3 id="op_avg">OPERATOR avg</h3>

OPERATOR avg(r RELATION { * }, attr integer) RETURNS float;

OPERATOR avg(r RELATION { * }, attr float) RETURNS float;

<h4>Description</h4>

The <code>avg</code> aggregate operator.
For the semantics, see RDB_avg().

<hr>

<h3 id="op_max">OPERATOR max</h3>

OPERATOR max(r RELATION { * }, attr integer) RETURNS integer;

OPERATOR max(r RELATION { * }, attr float) RETURNS float;

<h4>Description</h4>

The <code>max</code> aggregate operator.
For the semantics, see RDB_max().

<hr>

<h3 id="op_min">OPERATOR min</h3>

OPERATOR min(r RELATION { * }, attr integer) RETURNS integer;

OPERATOR min(r RELATION { * }, attr float) RETURNS float;

<h4>Description</h4>

The <code>min</code> aggregate operator.
For the semantics, see RDB_min().

<hr>

<h3 id="op_sum">OPERATOR sum</h3>

OPERATOR SUM(r RELATION { * }, attr integer) RETURNS integer;

OPERATOR SUM(r RELATION { * }, attr float) RETURNS float;

<h4>Description</h4>

The SUM aggregate operator.
For the semantics, see RDB_sum().

<hr>

<h3 id="op_divide">OPERATOR divide</h3>

OPERATOR divide(r1 RELATION { * }, r2 RELATION { * }, r2 RELATION { * }) RETURNS RELATION { * };

<h4>Description</h4>

The relational three-argument (small) DIVIDE operator.

<hr>

<h3 id="op_extend">OPERATOR extend</h3>

OPERATOR extend(r RELATION { * }, attrexp <em>ANY</em>, attrname string, ...) RETURNS RELATION { * };

<hr>

<h3 id="op_group">OPERATOR group</h3>

OPERATOR group(r RELATION { * }, attrname string, ..., dst_attr string) RETURNS RELATION { * };

<hr>

<h3 id="op_intersect">OPERATOR intersect</h3>

OPERATOR intersect(r1 RELATION { * }, r2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_join">OPERATOR join</h3>

OPERATOR join(r1 RELATION { * }, r2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_minus">OPERATOR minus</h3>

OPERATOR minus(r1 RELATION { * }, r2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_project">OPERATOR project</h3>

OPERATOR project(r1 RELATION { * }, attrname string ...) RETURNS RELATION { * };

<hr>

<h3 id="op_remove">OPERATOR remove</h3>

OPERATOR remove(r RELATION { * }, attrname string ...) RETURNS RELATION { * };

<hr>

<h3 id="op_rename">OPERATOR rename</h3>

OPERATOR rename(r RELATION { * }, scr_attrname string, dst_attrname string ...) RETURNS RELATION { * };

<hr>

<h3 id="op_ungroup">OPERATOR ungroup</h3>

OPERATOR ungroup(r RELATION { * }, attrname string) RETURNS RELATION { * };

<hr>

<h3 id="op_union">OPERATOR union</h3>

OPERATOR union(r1 RELATION { * }, r2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_d_union">OPERATOR d_union</h3>

OPERATOR d_union(r1 RELATION { * }, r2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_update">OPERATOR update</h3>

OPERATOR update(r1 RELATION { * }, dst_attrname string, src_expr <em>ANY</em>, ...) RETURNS RELATION { * };

<hr>

<h3 id="op_unwrap">OPERATOR unwrap</h3>

OPERATOR unwrap(attrname string, ...) RETURNS RELATION { * };

<hr>

<h3 id="op_semijoin">OPERATOR semijoin</h3>

OPERATOR semijoin(r1 RELATION { * }, r2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_semiminus">OPERATOR semiminus</h3>

OPERATOR semiminus(r1 RELATION { * }, r2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_summarize">OPERATOR summarize</h3>

OPERATOR summarize(r1 RELATION { * }, r2 RELATION { * }, expr <em>ANY</em>, attrname string, ...) RETURNS RELATION { * };

<hr>

<h3 id="op_tclose">OPERATOR tclose</h3>

OPERATOR tclose(r RELATION { * }) RETURNS RELATION { * };

The transitive closure operator.

<hr>

<h3 id="op_where">OPERATOR where</h3>

OPERATOR where(r RELATION { * }, b boolean) RETURNS RELATION { * };

The relational WHERE operator.

<hr>

<h3 id="op_wrap">OPERATOR wrap</h3>

OPERATOR wrap(r RELATION { * }, src_attrs ARRAY OF string, dst_attr string ...) RETURNS RELATION { * };

The relational WRAP operator.

@page comparison Built-in comparison operators

<h3 id="op_eq">OPERATOR =</h3>

OPERATOR = (<em>ANY</em>, <em>ANY</em>) RETURNS boolean;

The equality operator. Defined for every type. The arguments must be of the same type.

@returns

TRUE if the two arguments are equal, FALSE otherwise.  

<hr>

<h3 id="op_neq">OPERATOR <></h3>

OPERATOR <> (<em>ANY</em>, <em>ANY</em>) RETURNS boolean;

<h4>Description</h4>

The inequality operator. Defined for every type.
The arguments must be of the same type.

<h4>Return value</h4>

TRUE if the two arguments are not equal, FALSE otherwise.

<hr>

<h3 id="op_lt">OPERATOR &lt;</h3>

OPERATOR &lt; (integer, integer) RETURNS boolean;

OPERATOR &lt; (float, float) RETURNS boolean;

OPERATOR &lt; (string, string) RETURNS boolean;

<h4>Description</h4>

The lower-than operator.

<h4>Return value</h4>

TRUE if the first argument is lower than the first.
If the operands are strings, the strings will be compared using strcoll().

<hr>

<h3 id="op_lt">OPERATOR &lt;=</h3>

OPERATOR &lt;= (integer, integer) RETURNS boolean;

OPERATOR &lt;= (float, float) RETURNS boolean;

OPERATOR &lt;= (string, string) RETURNS boolean;

<h4>Description</h4>

The lower-than-or-equal operator.

<h4>Return value</h4>

TRUE if the first argument is lower than or equal to the second.
If the operands are strings, the strings will be compared using strcoll().

<hr>

<h3 id="op_gt">OPERATOR &gt;</h3>

OPERATOR &gt; (integer, integer) RETURNS boolean;

OPERATOR &gt; (float, float) RETURNS boolean;

OPERATOR &gt; (string, string) RETURNS boolean;

<h4>Description</h4>

The greater-than operator.

<h4>Return value</h4>

TRUE if the first argument is greater than the first.
If the operands are strings, the strings will be compared using strcoll().

<hr>

<h3 id="op_gte">OPERATOR &gt;=</h3>

OPERATOR &gt;= (integer, integer) RETURNS boolean;

OPERATOR &gt;= (float, float) RETURNS boolean;

OPERATOR &gt;= (string, string) RETURNS boolean;

<h4>Description</h4>

The greater-than-or-equal operator.

<h4>Return value</h4>

TRUE if the first argument is greater than or equal to the second.
If the operands are strings, the strings will be compared using strcoll().

@page logical Built-in logical operators

<h3 id="op_and">OPERATOR AND</h3>

OPERATOR and (boolean, boolean) RETURNS boolean;

<h4>Description</h4>

The boolean AND operator.

<hr>

<h3 id="op_or">OPERATOR OR</h3>

OPERATOR or (boolean, boolean) RETURNS boolean;

<h4>Description</h4>

The boolean OR operator.

<hr>

<h3 id="op_xor">OPERATOR xor</h3>

OPERATOR xor (boolean, boolean) RETURNS boolean;

<h4>Description</h4>

The boolean XOR operator.

<hr>

<h3 id="op_not">OPERATOR not</h3>

OPERATOR not (boolean) RETURNS boolean;

<h4>Description</h4>

The boolean NOT operator.

<hr>

<h3 id="op_if">OPERATOR if</h3>

OPERATOR IF (B boolean, V1 <em>ANY</em>, V2 <em>ANY</em>) RETURNS <em>ANY</em>;

<h4>Description</h4>

The IF-THEN-ELSE operator.

<h4>Return value</h4>

@a V1 if @a B is TRUE, @a V2 otherwise.

@page typeconv Built-in type conversion operators

<h3 id="op_integer">OPERATOR cast_as_integer</h3>

OPERATOR cast_as_integer (float) RETURNS integer;

OPERATOR cast_as_integer (string) RETURNS integer;

<h4>Description</h4>

Converts the operand to <code>integer</code>.

<h4>Return value</h4>

The operand, converted to <code>integer</code>.

<hr>

<h3 id="op_float">OPERATOR cast_as_float</h3>

OPERATOR cast_as_float (integer) RETURNS float;

OPERATOR cast_as_float (string) RETURNS float;

<h4>Description</h4>

Converts the operand to <code>float</code>.

<h4>Return value</h4>

The operand, converted to <code>float</code>.

<hr>

<h3 id="op_string">OPERATOR cast_as_string</h3>

OPERATOR cast_as_string (integer) RETURNS string;

OPERATOR cast_as_string (float) RETURNS string;

OPERATOR cast_as_string (binary) RETURNS string;

<h4>Description</h4>

Converts the operand to a string.

<h4>Return value</h4>

The operand, converted to string.

<hr>

<h3 id="cast_as_binary">OPERATOR cast_as_binary</h3>

OPERATOR cast_as_string (string) RETURNS binary;

<h4>Description</h4>

Converts the operand to a binary, without a terminating nullbyte.

<h4>Return value</h4>

The operand, converted to string.

<hr>

<h3 id="serialize">OPERATOR serialize</h3>

OPERATOR serialize (value <em>ANY</em>) RETURNS binary;

<h4>Description</h4>

Converts a value to a binary representation which includes the type.

<h4>Return value</h4>

The operand, converted to binary representation.

@page tupleop Built-in tuple operators

<h3 id="op_tuple">OPERATOR TUPLE</h3>

OPERATOR TUPLE(ATTRNAME string, ATTRVAL <em>ANY</em>, ...) RETURNS TUPLE { * };

<h4>Description</h4>

The tuple selector.

<hr>

<h3 id="op_to_tuple">OPERATOR to_tuple</h3>

OPERATOR to_tuple(r RELATION { * }) RETURNS TUPLE { * };

<h4>Description</h4>

Extracts a single tuple from a relation.

@page arrayop Built-in array operators

<h3 id="op_array">OPERATOR array</h3>

OPERATOR array(<em>ANY</em>, ...) RETURNS <em>ARRAY</em>;

<h4>Description</h4>

The array selector.
Returns an array containing the arguments passed to <code>array</code>, in order.

<hr>

<h3 id="op_array_length">OPERATOR length</h3>

OPERATOR length (arr <em>ARRAY</em>) RETURNS integer;

<h4>Description</h4>

The array length operator.

<h4>Return value</h4>

The length of the array @a arr.

<hr>

<h3 id="op_index_of">OPERATOR index_of</h3>

OPERATOR index_of (arr <em>ARRAY</em>, data <em>ANY</em>) RETURNS integer;

<h4>Description</h4>

Returns the index of the first occurrence of @a data in the array @a arr.

<h4>Return value</h4>

The index, or -1 if @a data does not appear in the array.

*/

/*
 * Functions implementing built-in read-only operators
 */
static int 
op_vtable(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    RDB_expression *argexp;
    RDB_expression *exp;

    /*
     * Convert arguments to expressions
     */
    exp = RDB_ro_op(RDB_operator_name(op), ecp);
    if (exp == NULL)
        return RDB_ERROR;

    for (i = 0; i < argc; i++) {
        if (argv[i]->kind == RDB_OB_TABLE) {
            if (RDB_table_is_persistent(argv[i])) {
                argexp = RDB_table_ref(argv[i], ecp);
            } else if (argv[i]->val.tbp->exp != NULL) {
                argexp = RDB_dup_expr(argv[i]->val.tbp->exp, ecp);
            } else {
                argexp = RDB_obj_to_expr(argv[i], ecp);
            }
        } else {
            argexp = RDB_obj_to_expr(argv[i], ecp);
        }
        if (argexp == NULL) {
            RDB_del_expr(exp, ecp);
            return RDB_ERROR;
        }
        RDB_add_arg(exp, argexp);
    }

    /*
     * Create virtual table
     */
    if (RDB_vtexp_to_obj(exp, ecp, txp, retvalp) != RDB_OK) {
        RDB_del_expr(exp, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_tuple(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int i;

    if (argc % 2 == 1) {
        RDB_raise_invalid_argument("Even number of arguments required by TUPLE", ecp);
        return RDB_OK;
    }

    /* Delete all attributes by destroying and initializing *retvalp */
    RDB_destroy_obj(retvalp, ecp);
    RDB_init_obj(retvalp);

    for (i = 0; i < argc; i += 2) {
        if (RDB_obj_type(argv[i]) != &RDB_STRING) {
            RDB_raise_invalid_argument("invalid TUPLE argument", ecp);
            return RDB_ERROR;
        }

        /* Check if an attribute name appears twice */
        if (RDB_tuple_get(retvalp, RDB_obj_string(argv[i])) != NULL) {
            RDB_raise_invalid_argument("double tuple attribute", ecp);
            return RDB_ERROR;
        }

        if (RDB_tuple_set(retvalp, RDB_obj_string(argv[i]), argv[i + 1], ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

/*
 * Create virtual table from RELATION selector arguments and *reltyp.
 * *reltyp is consumed even if RDB_ERROR is returned.
 */
int
RDB_op_type_relation(int argc, RDB_object *argv[], RDB_type *reltyp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int i;

    /*
     * If *retvalp has no type, turn it into a table of type *reltyp,
     * otherwise only clear it
     */
    if (RDB_obj_type(retvalp) == NULL) {
        if (RDB_init_table_from_type(retvalp, NULL, reltyp, 0, NULL,
                0, NULL, ecp) != RDB_OK) {
            RDB_del_nonscalar_type(reltyp, ecp);
            return RDB_ERROR;
        }
    } else {
        RDB_del_nonscalar_type(reltyp, ecp);
        if (RDB_delete(retvalp, NULL, ecp, txp) == RDB_ERROR)
            return RDB_ERROR;
    }

    for (i = 0; i < argc; i++) {
        if (RDB_insert(retvalp, argv[i], ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    return RDB_OK;
}

int
RDB_op_relation(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_type *rtyp;
    RDB_type *tuptyp;

    if (argc == 0) {
        RDB_raise_invalid_argument("At least one argument required", ecp);
        return RDB_ERROR;
    }

    if (argv[0]->kind != RDB_OB_TUPLE && argv[0]->kind != RDB_OB_INITIAL) {
        RDB_raise_not_supported("tuple required by RELATION", ecp);
        return RDB_ERROR;
    }

    /*
     * Create relation type using the first argument
     */
    tuptyp = RDB_tuple_type(argv[0], ecp);
    if (tuptyp == NULL) {
        return RDB_ERROR;
    }
    rtyp = RDB_new_relation_type_from_base(tuptyp, ecp);
    if (rtyp == NULL) {
        return RDB_ERROR;
    }

    return RDB_op_type_relation(argc, argv, rtyp, ecp, txp, retvalp);
}

static int
op_array(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int i;

    /* Set array length */
    if (RDB_set_array_length(retvalp, (RDB_int) argc, ecp) != RDB_OK)
        return RDB_ERROR;

    /* Set elements */
    for (i = 0; i < argc; i++) {
        if (RDB_array_set(retvalp, (RDB_int) i, argv[i], ecp) != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_project(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int i;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(argc, argv, op, ecp, txp, retvalp);

    for (i = 1; i < argc; i++) {
        char *attrname = RDB_obj_string(argv[i]);

        if (RDB_tuple_set(retvalp, attrname,
                RDB_tuple_get(argv[0], attrname), ecp) != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_remove(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    int ret;
    const char **attrv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(argc, argv, op, ecp, txp, retvalp);

    attrv = RDB_alloc(sizeof (char *) * (argc - 1), ecp);
    if (attrv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    ret = RDB_remove_tuple(argv[0], argc - 1, attrv, ecp, retvalp);
    RDB_free(attrv);
    return ret;
}

static int
op_rename(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    int ret;
    int renc = (argc - 1) / 2;
    RDB_renaming *renv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(argc, argv, op, ecp, txp, retvalp);

    renv = RDB_alloc(sizeof(RDB_renaming) * renc, ecp);
    if (renv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < renc; i++) {
        if (argv[1 + i]->typ != &RDB_STRING
                || argv[2 + i]->typ != &RDB_STRING) {
            RDB_free(renv);
            RDB_raise_type_mismatch("RENAME argument must be of type string", ecp);
            return RDB_ERROR;
        }
        renv[i].from = RDB_obj_string(argv[1 + i * 2]);
        renv[i].to = RDB_obj_string(argv[2 + i * 2]);
    }

    ret = RDB_rename_tuple(argv[0], renc, renv, ecp, retvalp);
    RDB_free(renv);
    return ret;
}

static int
op_union(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_type *typ1, *typ2;

    if (argc != 2) {
        RDB_raise_invalid_argument("invalid # of arguments", ecp);
        return RDB_ERROR;
    }

    typ1 = RDB_obj_type(argv[0]);
    typ2 = RDB_obj_type(argv[1]);

    if ((typ1 == NULL || RDB_type_is_tuple(typ1))
            && (typ2 == NULL || RDB_type_is_tuple(typ2))
            && (argv[0]->kind == RDB_OB_TUPLE || argv[0]->kind == RDB_OB_INITIAL)
            && (argv[1]->kind == RDB_OB_TUPLE || argv[1]->kind == RDB_OB_INITIAL)) {
        /* Tuple UNION */
        return RDB_union_tuples(argv[0], argv[1], ecp, txp, retvalp);
    }

    /* Relation UNION - check types */
    if (typ1 == NULL || typ2 == NULL) {
        RDB_raise_type_mismatch("relation argument required", ecp);
        return RDB_ERROR;
    }

    return op_vtable(argc, argv, op, ecp, txp, retvalp);
}

static int
op_tclose(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_type *argtyp = RDB_obj_type(argv[0]);

    if (argtyp->def.basetyp->def.tuple.attrc != 2) {
        RDB_raise_invalid_argument("TCLOSE argument must have 2 attributes",
                ecp);
        return RDB_ERROR;
    }
    if (!RDB_type_equals(argtyp->def.basetyp->def.tuple.attrv[0].typ,
            argtyp->def.basetyp->def.tuple.attrv[1].typ)) {
        RDB_raise_type_mismatch("TCLOSE argument attributes not of same type",
                ecp);
        return RDB_ERROR;
    }

    return op_vtable(argc, argv, op, ecp, txp, retvalp);
}

static int
op_wrap(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i, j;
    int wrapc;
    RDB_wrapping *wrapv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(argc, argv, op, ecp, txp, retvalp);

    if (argc < 1 || argc % 2 != 1) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return RDB_ERROR;
    }

    wrapc = argc / 2;
    if (wrapc > 0) {
        wrapv = RDB_alloc(sizeof(RDB_wrapping) * wrapc, ecp);
        if (wrapv == NULL) {
            return RDB_ERROR;
        }
    }
    for (i = 0; i < wrapc; i++) {
        wrapv[i].attrv = NULL;
    }

    for (i = 0; i < wrapc; i++) {
        RDB_object *objp;

        wrapv[i].attrc =  RDB_array_length(argv[i * 2 + 1], ecp);
        wrapv[i].attrv = RDB_alloc(sizeof (char *) * wrapv[i].attrc, ecp);
        if (wrapv[i].attrv == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        for (j = 0; j < wrapv[i].attrc; j++) {
            objp = RDB_array_get(argv[i * 2 + 1], (RDB_int) j, ecp);
            if (objp == NULL) {
                ret = RDB_ERROR;
                goto cleanup;
            }
            wrapv[i].attrv[j] = RDB_obj_string(objp);
        }
        wrapv[i].attrname = RDB_obj_string(argv[i * 2 + 2]);
    }

    ret = RDB_wrap_tuple(argv[0], wrapc, wrapv, ecp, retvalp);

cleanup:
    for (i = 0; i < wrapc; i++) {
        RDB_free(wrapv[i].attrv);
    }
    if (wrapc > 0)
        RDB_free(wrapv);

    return ret;
}

static int
op_unwrap(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i;
    char **attrv;

    if (argc < 1) {
        RDB_raise_invalid_argument("invalid argument to UNWRAP", ecp);
        return RDB_ERROR;
    }

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(argc, argv, op, ecp, txp, retvalp);

    attrv = RDB_alloc(sizeof(char *) * (argc - 1), ecp);
    if (attrv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    ret = RDB_unwrap_tuple(argv[0], argc - 1, attrv, ecp, retvalp);
    RDB_free(attrv);
    return ret;
}

static int
op_to_tuple(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_extract_tuple(argv[0], ecp, txp, retvalp);
}

static int
op_in(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool res;

    if (RDB_table_contains(argv[1], argv[0], ecp, txp, &res) != RDB_OK)
        return RDB_ERROR;
    RDB_bool_to_obj(retvalp, res);
    return RDB_OK;
}

static int
op_subset_of(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool res;

    if (RDB_subset(argv[0], argv[1], ecp, txp, &res) != RDB_OK)
        return RDB_ERROR;
    RDB_bool_to_obj(retvalp, res);
    return RDB_OK;
}

static int
op_subscript(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
	RDB_object *objp;

	if (RDB_is_array(argv[0])) {
		if (RDB_obj_type(argv[1]) != &RDB_INTEGER) {
			RDB_raise_type_mismatch("subscript argument must be integer", ecp);
			return RDB_ERROR;
		}
	    objp = RDB_array_get(argv[0], RDB_obj_int(argv[1]), ecp);
	    if (objp == NULL) {
	        return RDB_ERROR;
	    }
	} else if (RDB_is_tuple(argv[0])) {
		if (RDB_obj_type(argv[1]) != &RDB_STRING) {
			RDB_raise_type_mismatch("subscript argument must be string", ecp);
			return RDB_ERROR;
		}
	    objp = RDB_tuple_get(argv[0], RDB_obj_string(argv[1]));
	    if (objp == NULL) {
	    	RDB_raise_name(RDB_obj_string(argv[1]), ecp);
	        return RDB_ERROR;
	    }
	}
	else {
		RDB_raise_type_mismatch("array or tuple required", ecp);
		return RDB_ERROR;
	}
    return RDB_copy_obj(retvalp, objp, ecp);
}

static int
length_array(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int len;

    if (argc != 1) {
        RDB_raise_invalid_argument("invalid number of arguments to length()",
                ecp);
        return RDB_ERROR;
    }
    if (argv[0]->kind != RDB_OB_ARRAY && argv[0]->kind != RDB_OB_INITIAL) {
        RDB_raise_type_mismatch("invalid argument type", ecp);
        return RDB_ERROR;
    }

    len = RDB_array_length(argv[0], ecp);
    if (len == (RDB_int) RDB_ERROR)
        return RDB_ERROR;
    
    RDB_int_to_obj(retvalp, len);
    return RDB_OK;
}

static int
op_index_of(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int i;
    RDB_int len;
    RDB_object *objp;
    RDB_bool iseq;

    if (argc != 2) {
        RDB_raise_invalid_argument("invalid number of arguments to index_of()",
                ecp);
        return RDB_ERROR;
    }
    if (argv[0]->kind != RDB_OB_ARRAY && argv[0]->kind != RDB_OB_INITIAL) {
        RDB_raise_type_mismatch("invalid argument type", ecp);
        return RDB_ERROR;
    }

    len = RDB_array_length(argv[0], ecp);
    if (len == (RDB_int) RDB_ERROR)
        return RDB_ERROR;

    for (i = (RDB_int) 0; i < len; i++) {
        objp = RDB_array_get(argv[0], i, ecp);
        if (objp == NULL)
            return RDB_ERROR;
        if (RDB_obj_equals(objp, argv[1], ecp, txp, &iseq) != RDB_OK)
            return RDB_ERROR;
        if (iseq) {
            RDB_int_to_obj(retvalp, i);
            return RDB_OK;
        }
    }
    RDB_int_to_obj(retvalp, -1);
    return RDB_OK;
}

static int
op_serialize(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_obj_to_bin(retvalp, argv[0], ecp);
}

int
RDB_put_global_ro_op(const char *name, int argc, RDB_type **argtv,
        RDB_type *rtyp, RDB_ro_op_func *fp, RDB_exec_context *ecp)
{
    return RDB_put_ro_op(&RDB_builtin_ro_op_map, name, argc, argtv,
            rtyp, fp, ecp);
}

int
RDB_init_builtin_ops(RDB_exec_context *ecp)
{
    int ret;
    RDB_type *paramtv[3];
    RDB_attr genattr;
    RDB_type *genreltyp = NULL;
    RDB_type *gentuptyp = NULL;

    /*
     * No errors other than no_memory_error may be raised here because the other error
     * types are not initialized yet.
     */

    RDB_init_op_map(&RDB_builtin_ro_op_map);

    RDB_init_op_map(&RDB_builtin_upd_op_map);

    if (RDB_add_builtin_scalar_ro_ops(&RDB_builtin_ro_op_map, ecp) != RDB_OK)
        return RDB_ERROR;

    /* Datetime operators */
    if (RDB_add_datetime_ro_ops(&RDB_builtin_ro_op_map, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_add_datetime_upd_ops(&RDB_builtin_upd_op_map, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_global_ro_op("=", 2, paramtv, &RDB_BOOLEAN, RDB_dfl_obj_equals, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_global_ro_op("=", 2, paramtv, &RDB_BOOLEAN, RDB_dfl_obj_equals, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_global_ro_op("=", 2, paramtv, &RDB_BOOLEAN, RDB_dfl_obj_equals, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_DATETIME;
    paramtv[1] = &RDB_DATETIME;

    if (RDB_put_global_ro_op("=", 2, paramtv, &RDB_BOOLEAN, RDB_dfl_obj_equals, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_global_ro_op("<>", 2, paramtv, &RDB_BOOLEAN,
            &RDB_obj_not_equals, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_global_ro_op("<>", 2, paramtv, &RDB_BOOLEAN,
            &RDB_obj_not_equals, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_global_ro_op("<>", 2, paramtv, &RDB_BOOLEAN,
            &RDB_obj_not_equals, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_DATETIME;
    paramtv[1] = &RDB_DATETIME;

    if (RDB_put_global_ro_op("<>", 2, paramtv, &RDB_BOOLEAN,
            &RDB_obj_not_equals, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = NULL;
    if (RDB_put_global_ro_op("serialize", 1, paramtv, &RDB_BINARY, &op_serialize, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_global_ro_op("tuple", -1, NULL, NULL, &op_tuple, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_global_ro_op("relation", -1, NULL, NULL, &RDB_op_relation, ecp)
            != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_global_ro_op("array", -1, NULL, NULL, &op_array, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_global_ro_op("index_of", -1, NULL, NULL, &op_index_of, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_global_ro_op("project", -1, NULL, NULL, &op_project, ecp)
            != RDB_OK)
        return RDB_ERROR;

    ret = RDB_put_global_ro_op("remove", -1, NULL, NULL, &op_remove, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    ret = RDB_put_global_ro_op("rename", -1, NULL, NULL, &op_rename, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    genattr.name = NULL;
    genreltyp = RDB_new_relation_type(1, &genattr, ecp);
    if (genreltyp == NULL)
        goto error;
    gentuptyp = RDB_new_tuple_type(1, &genattr, ecp);
    if (gentuptyp == NULL)
        goto error;

    paramtv[0] = genreltyp;
    paramtv[1] = genreltyp;
    if (RDB_put_global_ro_op("join", 2, paramtv, NULL, &op_vtable, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("minus", 2, paramtv, NULL, &op_vtable, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("semiminus", 2, paramtv, NULL, &op_vtable, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("intersect", 2, paramtv, NULL, &op_vtable, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("semijoin", 2, paramtv, NULL, &op_vtable, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("d_union", 2, paramtv, NULL, &op_vtable, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("union", -1, NULL, NULL, &op_union, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("wrap", -1, NULL, NULL, &op_wrap, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("unwrap", -1, NULL, NULL, &op_unwrap, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("divide", -1, NULL, NULL, &op_vtable, ecp) != RDB_OK)
        goto error;

    paramtv[0] = genreltyp;
    if (RDB_put_global_ro_op("tclose", 1, paramtv, NULL, &op_tclose, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("group", -1, NULL, NULL, &op_vtable, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("ungroup", -1, NULL, NULL, &op_vtable, ecp) != RDB_OK)
        goto error;

    paramtv[0] = genreltyp;
    if (RDB_put_global_ro_op("to_tuple", 1, paramtv, NULL, &op_to_tuple, ecp) != RDB_OK)
        goto error;

    paramtv[0] = gentuptyp;
    paramtv[1] = genreltyp;
    if (RDB_put_global_ro_op("in", 2, paramtv, &RDB_BOOLEAN, &op_in, ecp) != RDB_OK)
        goto error;

    paramtv[0] = genreltyp;
    paramtv[1] = genreltyp;
    if (RDB_put_global_ro_op("subset_of", 2, paramtv, &RDB_BOOLEAN, &op_subset_of, ecp) != RDB_OK)
        goto error;

    if (RDB_put_global_ro_op("[]", -1, NULL, NULL, &op_subscript, ecp)
            != RDB_OK) {
        goto error;
    }

    if (RDB_put_global_ro_op("length", -1, NULL, NULL, &length_array, ecp)
            != RDB_OK) {
        goto error;
    }

    RDB_del_nonscalar_type(genreltyp, ecp);
    RDB_del_nonscalar_type(gentuptyp, ecp);

    return RDB_OK;

error:
    if (genreltyp != NULL)
        RDB_del_nonscalar_type(genreltyp, ecp);
    if (gentuptyp != NULL)
        RDB_del_nonscalar_type(gentuptyp, ecp);

    return RDB_ERROR;
}

/** @addtogroup generic
 * @{
 */

/**
 * Initialize built-in types and operators.
 * May be called more than once.
 *
 * It is called by RDB_create_db_from_env() and RDB_get_db_from_env().
 * If neither of these functions have been called, RDB_init_builtin()
 * must be called to make built-in types and operators available.
 *
 */
int
RDB_init_builtin(RDB_exec_context *ecp)
{
    static RDB_bool initialized = RDB_FALSE;

    if (initialized) {
        return RDB_OK;
    }

    if (RDB_init_builtin_basic_types(ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_init_builtin_ops(ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_add_builtin_pr_types(ecp) != RDB_OK)
        return RDB_ERROR;

    initialized = RDB_TRUE;
    return RDB_OK;
}

/*@}*/
