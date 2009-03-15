/*
 * $Id$
 *
 * Copyright (C) 2005-2009 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include <regex.h>
#include <string.h>
#include <stdlib.h>

RDB_op_map _RDB_builtin_ro_op_map;

/** @page builtin-ops Built-in operators
@section scalar-ops Built-in scalar operators

OPERATOR = (ANY, ANY) RETURNS BOOLEAN;

The equality operator. Defined for every type. The arguments must be of the same type.

@returns

TRUE if the two arguments are equal, FALSE otherwise.  

OPERATOR <> (<em>ANY</em>, <em>ANY</em>) RETURNS BOOLEAN;

<h4>Description</h4>

The inequality operator. Defined for every type.
The arguments must be of the same type.

<h4>Return value</h4>

TRUE if the two arguments are not equal, FALSE otherwise.

<hr>

<h3 id="op_lt">OPERATOR &lt;</h3>

OPERATOR &lt; (INTEGER, INTEGER) RETURNS BOOLEAN;

OPERATOR &lt; (FLOAT, FLOAT) RETURNS BOOLEAN;

OPERATOR &lt; (STRING, STRING) RETURNS BOOLEAN;

<h4>Description</h4>

The lower-than operator.

<h4>Return value</h4>

TRUE if the first argument is lower than the first.
If the operands are strings, the strings will be compared using strcoll().

<hr>

<h3 id="op_lte">OPERATOR &lt;=</h3>

OPERATOR &lt;= (INTEGER, INTEGER) RETURNS BOOLEAN;

OPERATOR &lt;= (FLOAT, FLOAT) RETURNS BOOLEAN;

OPERATOR &lt;= (STRING, STRING) RETURNS BOOLEAN;

<h4>Description</h4>

The lower-than-or-equal operator.

<h4>Return value</h4>

TRUE if the first argument is lower than or equal to the second.
If the operands are strings, the strings will be compared using strcoll().

<hr>

<h3 id="op_gt">OPERATOR &gt;</h3>

OPERATOR &gt; (INTEGER, INTEGER) RETURNS BOOLEAN;

OPERATOR &gt; (FLOAT, FLOAT) RETURNS BOOLEAN;

OPERATOR &gt; (STRING, STRING) RETURNS BOOLEAN;

<h4>Description</h4>

The greater-than operator.

<h4>Return value</h4>

TRUE if the first argument is greater than the first.
If the operands are strings, the strings will be compared using strcoll().

<hr>

<h3 id="op_gte">OPERATOR &gt;=</h3>

OPERATOR &gt;= (INTEGER, INTEGER) RETURNS BOOLEAN;

OPERATOR &gt;= (FLOAT, FLOAT) RETURNS BOOLEAN;

OPERATOR &gt;= (STRING, STRING) RETURNS BOOLEAN;

<h4>Description</h4>

The greater-than-or-equal operator.

<h4>Return value</h4>

TRUE if the first argument is greater than or equal to the second.
If the operands are strings, the strings will be compared using strcoll().

<hr>

<h3 id="op_plus">OPERATOR +</h3>

OPERATOR + (INTEGER, INTEGER) RETURNS INTEGER;

OPERATOR + (FLOAT, FLOAT) RETURNS FLOAT;

<h4>Description</h4>

The addition operator.

<h4>Return value</h4>

The sum of the two operands.

<hr>

<h3 id="op_uminus">OPERATOR - (unary)</h3>

OPERATOR - (INTEGER) RETURNS INTEGER;

OPERATOR - (FLOAT) RETURNS FLOAT;

<h4>Description</h4>

The unary minus operator.

<h4>Return value</h4>

The operand, sign inverted.

<hr>

<h3 id="op_bminus">OPERATOR - (binary)</h3>

OPERATOR - (INTEGER, INTEGER) RETURNS INTEGER;

OPERATOR - (FLOAT, FLOAT) RETURNS FLOAT;

<h4>Description</h4>

The subtraction operator.

<h4>Return value</h4>

The difference of the two operands.

<hr>

<h3 id="op_times">OPERATOR *</h3>

OPERATOR * (INTEGER, INTEGER) RETURNS INTEGER;;

OPERATOR * (FLOAT, FLOAT) RETURNS FLOAT;

<h4>Description</h4>

The multiplication operator.

<h4>Return value</h4>

The product of the two operands.

<hr>

<h3 id="op_div">OPERATOR /</h3>

OPERATOR / (INTEGER, INTEGER) RETURNS INTEGER;

OPERATOR / (FLOAT, FLOAT) RETURNS FLOAT;

<h4>Description</h4>

The division operator.

<h4>Return value</h4>

The quotient of the operators.

<h4>Errors</h4>

<dl>
<dt>INVALID_ARGUMENT_ERROR
<dd>The divisor is zero.
</dl>

<hr>

<h3 id="op_and">OPERATOR AND</h3>

OPERATOR AND (BOOLEAN, BOOLEAN) RETURNS BOOLEAN;

<h4>Description</h4>

The boolean AND operator.

<hr>

<h3 id="op_or">OPERATOR OR</h3>

OPERATOR OR (BOOLEAN, BOOLEAN) RETURNS BOOLEAN;

<h4>Description</h4>

The boolean OR operator.

<hr>

<h3 id="op_not">OPERATOR NOT</h3>

OPERATOR NOT (BOOLEAN) RETURNS BOOLEAN;

<h4>Description</h4>

The boolean NOT operator.

<hr>

<h3 id="op_concat">OPERATOR ||</h3>

OPERATOR || (STRING, STRING) RETURNS STRING;

<h4>Description</h4>

The string concatenation operator.

<h4>Return value</h4>

The result of the concatenation of the operands.

<hr>

<h3 id="op_length">OPERATOR LENGTH</h3>

OPERATOR LENGTH (STRING) RETURNS INTEGER;

<h4>Description</h4>

The string length operator.

<h4>Return value</h4>

The length of the operand.

<hr>

<h3 id="op_substring">OPERATOR SUBSTRING</h3>

OPERATOR SUBSTRING(S STRING, START INTEGER, LENGTH INTEGER) RETURNS
STRING;

<h4>Description</h4>

The substring operator.

<h4>Return value</h4>

The substring of S with length LENGTH starting at position
START.

<h4>Errors</h4>

<dl>
<dt>INVALID_ARGUMENT_ERROR
<dd>START is negative, or START + LENGTH is greater than LENGTH(S).
</dl>

<hr>

<h3 id="op_matches">OPERATOR MATCHES</h3>

OPERATOR MATCHES (S STRING, PATTERN STRING) RETURNS BOOLEAN;

<h4>Description</h4>

The regular expression matching operator.

<h4>Return value</h4>

RDB_TRUE if S matches PATTERN, RDB_FALSE otherwise.

<hr>

<h3 id="op_integer">OPERATOR INTEGER</h3>

OPERATOR INTEGER (FLOAT) RETURNS INTEGER;

OPERATOR INTEGER (STRING) RETURNS INTEGER;

<h4>Description</h4>

Converts the operand to INTEGER.

<h4>Return value</h4>

The operand, converted to INTEGER.

<hr>

<h3 id="op_float">OPERATOR FLOAT</h3>

OPERATOR FLOAT (INTEGER) RETURNS FLOAT;

OPERATOR FLOAT (STRING) RETURNS FLOAT;

<h4>Description</h4>

Converts the operand to FLOAT.

<h4>Return value</h4>

The operand, converted to FLOAT.

<hr>

<h3 id="op_string">OPERATOR STRING</h3>

OPERATOR STRING (INTEGER) RETURNS STRING;

OPERATOR STRING (FLOAT) RETURNS STRING;

<h4>Description</h4>

Converts the operand to a string.

<h4>Return value</h4>

The operand, converted to STRING.

<hr>

<h3 id="op_length">OPERATOR GETENV</h3>

OPERATOR GETENV (NAME STRING) RETURNS STRING;

<h4>Description</h4>

Reads the environment variable <var>NAME</var>.

<h4>Return value</h4>

The value of the environment variable <var>NAME</var>.

<hr>

<h3 id="op_is_empty">OPERATOR IS_EMPTY</h3>

OPERATOR IS_EMPTY (<em>RELATION</em>) RETURNS BOOLEAN;

<h4>Description</h4>

Checks if a table is empty.

<h4>Return value</h4>

RDB_TRUE if the relation-valued operand is empty, RDB_FALSE
otherwise.

<hr>

<h3 id="op_count">OPERATOR COUNT</h3>

OPERATOR COUNT (<em>RELATION</em>) RETURNS INTEGER;

<h4>Description</h4>

Counts the tuples in a table.

<h4>Return value</h4>

The cardinality of the relation-valued operand.

<hr>

<h3 id="op_in">OPERATOR IN</h3>

OPERATOR IN (T <em>TUPLE</em>, R <em>RELATION</em>) RETURNS BOOLEAN;

<h4>Description</h4>

Checks if a table contains a given tuple.

<h4>Return value</h4>

RDB_TRUE if <var>R</var> contains <var>T</var>, RDB_FALSE otherwise.

<hr>

<h3 id="op_subset_of">OPERATOR SUBSET_OF</h3>

OPERATOR SUBSET_OF (R1 <em>RELATION</em>, R2 <em>RELATION</em>) RETURNS BOOLEAN;

<h4>Description</h4>

Checks if a table is a subset of another table.

<h4>Return value</h4>

RDB_TRUE if the <var>R1</var> is a subset of <var>R2</var>, RDB_FALSE otherwise.

<hr>

<h3 id="op_any">OPERATOR ANY</h3>

OPERATOR ANY(R <em>RELATION</em>, ATTR BOOLEAN) RETURNS BOOLEAN;

<h4>Description</h4>

The ANY aggregate operator.
For the semantics, see RDB_any().

<hr>

<h3 id="op_all">OPERATOR ALL</h3>

OPERATOR ALL(R <em>RELATION</em>, ATTR BOOLEAN) RETURNS BOOLEAN;

<h4>Description</h4>

The ALL aggregate operator.
For the semantics, see RDB_all().

<hr>

<h3 id="op_avg">OPERATOR AVG</h3>

OPERATOR AVG(R <em>RELATION</em>, ATTR INTEGER) RETURNS FLOAT;

OPERATOR AVG(R <em>RELATION</em>, ATTR FLOAT) RETURNS FLOAT;

<h4>Description</h4>

The AVG aggregate operator.
For the semantics, see RDB_avg().

<hr>

<h3 id="op_max">OPERATOR MAX</h3>

OPERATOR MAX(R <em>RELATION</em>, ATTR INTEGER) RETURNS INTEGER;

OPERATOR MAX(R <em>RELATION</em>, ATTR FLOAT) RETURNS FLOAT;

<h4>Description</h4>

The MAX aggregate operator.
For the semantics, see RDB_max().

<hr>

<h3 id="op_min">OPERATOR MIN</h3>

OPERATOR MIN(R <em>RELATION</em>, ATTR INTEGER) RETURNS INTEGER;

OPERATOR MIN(R <em>RELATION</em>, ATTR FLOAT) RETURNS FLOAT;

<h4>Description</h4>

The MIN aggregate operator.
For the semantics, see RDB_min().

<hr>

<h3 id="op_sum">OPERATOR SUM</h3>

OPERATOR SUM(R <em>RELATION</em>, ATTR INTEGER) RETURNS INTEGER;

OPERATOR SUM(R <em>RELATION</em>, ATTR FLOAT) RETURNS FLOAT;

<h4>Description</h4>

The SUM aggregate operator.
For the semantics, see RDB_sum().

<hr>

<h3 id="op_if">OPERATOR IF</h3>

OPERATOR IF (B BOOLEAN, V1 <em>ANY</em>, V2 <em>ANY</em>) RETURNS <em>ANY</em>;

<h4>Description</h4>

The IF-THEN-ELSE operator.

<h4>Return value</h4>

<var>V1</var> if <var>B</var> is RDB_TRUE, <var>V2</var> otherwise.

<hr>

@section tup-rel-ops Built-in tuple and relational operators

<h3 id="op_tuple">OPERATOR TUPLE</h3>

OPERATOR TUPLE(ATTRNAME STRING, ATTRVAL <em>STRING</em>, ...) RETURNS <em>TUPLE</em>;

<h4>Description</h4>

The tuple selector.

<hr>

<h3 id="op_relation">OPERATOR RELATION</h3>

OPERATOR RELATION(T TUPLE, ...) RETURNS <em>RELATION</em>;

<h4>Description</h4>

The relation selector.

<hr>

<h3 id="op_tuple">OPERATOR TUPLE</h3>

OPERATOR ARRAY(<em>ANY</em>, ...) RETURNS <em>ARRAY</em>;

<h4>Description</h4>

The array selector.

<hr>

<h3 id="op_divide">OPERATOR DIVIDE</h3>

OPERATOR DIVIDE(R1 <em>RELATION</em>, R2 <em>RELATION</em>, R2 <em>RELATION</em>) RETURNS <em>RELATION</em>;

<h4>Description</h4>

The relational three-argument (small) DIVIDE operator.

<hr>

<h3 id="op_extend">OPERATOR EXTEND</h3>

OPERATOR EXTEND(R <em>RELATION</em>, ATTREXP <em>ANY</em>, ATTRNAME STRING, ...) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_group">OPERATOR GROUP</h3>

OPERATOR GROUP(R <em>RELATION</em>, ATTRNAME <em>STRING</em> ...) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_intersect">OPERATOR INTERSECT</h3>

OPERATOR INTERSECT(R1 <em>RELATION</em>, R2 <em>RELATION</em>) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_join">OPERATOR JOIN</h3>

OPERATOR JOIN(R1 <em>RELATION</em>, R2 <em>RELATION</em>) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_minus">OPERATOR MINUS</h3>

OPERATOR MINUS(R1 <em>RELATION</em>, R2 <em>RELATION</em>) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_project">OPERATOR PROJECT</h3>

OPERATOR PROJECT(R1 <em>RELATION</em>, ATTRNAME STRING ...) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_remove">OPERATOR REMOVE</h3>

OPERATOR REMOVE(R <em>RELATION</em>, ATTRNAME STRING ...) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_rename">OPERATOR RENAME</h3>

OPERATOR RENAME(R <em>RELATION</em>, SRC_ATTRNAME STRING, DST_ATTRNAME STRING ...) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_ungroup">OPERATOR UNGROUP</h3>

OPERATOR UNGROUP(R <em>RELATION</em>, ATTRNAME STRING) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_union">OPERATOR UNION</h3>

OPERATOR UNION(R1 <em>RELATION</em>, R2 <em>RELATION</em>) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_union">OPERATOR UPDATE</h3>

OPERATOR UPDATE(R1 <em>RELATION</em>, DST_ATTRNAME <em>STRING</em>, SRC_EXPR <em>ANY</em>) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_unwrap">OPERATOR UNWRAP</h3>

OPERATOR UNWRAP(ATTRNAME STRING, ...) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_semijoin">OPERATOR SEMIJOIN</h3>

OPERATOR SEMIJOIN(R1 <em>RELATION</em>, R2 <em>RELATION</em>) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_semiminus">OPERATOR SEMIMINUS</h3>

OPERATOR SEMIMINUS(R1 <em>RELATION</em>, R2 <em>RELATION</em>) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_summarize">OPERATOR SUMMARIZE</h3>

OPERATOR SUMMARIZE(R1 <em>RELATION</em>, R2 <em>RELATION</em>, EXPR <em>ANY</em>, ATTRNAME STRING, ...) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_where">OPERATOR WHERE</h3>

OPERATOR WHERE(R <em>RELATION</em>, B BOOLEAN) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_wrap">OPERATOR WRAP</h3>

OPERATOR WRAP(R <em>RELATION</em>, SRC_ATTRS ARRAY OF STRING, DST_ATTR STRING ...) RETURNS <em>RELATION</em>;
*/

/*
 * The following functions implement the built-in operators
 */
static int
neq_bool(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->var.bool_val != argv[1]->var.bool_val));
    return RDB_OK;
}

static int
neq_binary(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[0]->var.bin.len != argv[1]->var.bin.len)
        RDB_bool_to_obj(retvalp, RDB_TRUE);
    else if (argv[0]->var.bin.len == 0)
        RDB_bool_to_obj(retvalp, RDB_FALSE);
    else
        RDB_bool_to_obj(retvalp, (RDB_bool) (memcmp(argv[0]->var.bin.datap,
            argv[1]->var.bin.datap, argv[0]->var.bin.len) != 0));
    return RDB_OK;
}

static int 
op_vtable(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    RDB_expression *argexp;
    RDB_expression *exp;

    /*
     * Convert arguments to expressions
     */
    exp = RDB_ro_op(name, ecp);
    if (exp == NULL)
        return RDB_ERROR;

    for (i = 0; i < argc; i++) {
        if (argv[i]->kind == RDB_OB_TABLE) {
            if (argv[i]->var.tb.is_persistent) {
                argexp = RDB_table_ref(argv[i], ecp);
            } else if (argv[i]->var.tb.exp != NULL) {
                argexp = RDB_dup_expr(argv[i]->var.tb.exp, ecp);
            } else {
                argexp = RDB_obj_to_expr(argv[i], ecp);
            }
        } else {
            argexp = RDB_obj_to_expr(argv[i], ecp);
        }
        if (argexp == NULL) {
            RDB_drop_expr(exp, ecp);
            return RDB_ERROR;
        }
        RDB_add_arg(exp, argexp);
    }

    /*
     * Create virtual table
     */
    if (_RDB_vtexp_to_obj(exp, ecp, txp, retvalp) != RDB_OK) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_tuple(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int i;

    if (argc % 2 == 1) {
        RDB_raise_invalid_argument("Even number of arguments required by TUPLE", ecp);
        return RDB_OK;
    }

    for (i = 0; i < argc; i += 2) {
        if (RDB_obj_type(argv[i]) != &RDB_STRING) {
            RDB_raise_invalid_argument("invalid TUPLE argument", ecp);
            return RDB_ERROR;
        }

        if (RDB_tuple_set(retvalp, RDB_obj_string(argv[i]), argv[i + 1], ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
op_relation(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_type *rtyp;
    int i;

    if (argc == 0) {
        return RDB_init_table(retvalp, NULL, 0, NULL, 0, NULL, ecp);
    }

    if (argv[0]->kind != RDB_OB_TUPLE) {
        RDB_raise_not_supported("tuple required by RELATION", ecp);
        return RDB_ERROR;
    }

    rtyp = RDB_alloc(sizeof(RDB_type), ecp);
    if (rtyp == NULL) {
        return RDB_ERROR;
    }
    rtyp->kind = RDB_TP_RELATION;
    rtyp->name = NULL;
    rtyp->var.basetyp = _RDB_tuple_type(argv[0], ecp);
    if (rtyp->var.basetyp == NULL) {
        RDB_free(rtyp);
        return RDB_ERROR;
    }

    if (RDB_init_table_from_type(retvalp, NULL, rtyp, 0, NULL, ecp) != RDB_OK) {
        RDB_drop_type(rtyp, ecp, NULL);
        return RDB_ERROR;
    }

    for (i = 0; i < argc; i++) {
        if (RDB_insert(retvalp, argv[i], ecp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    return RDB_OK;
}

static int
op_array(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
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
op_vtable_wrapfn(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    return op_vtable(name, argc, argv, rtyp, ecp, txp, retvalp);
}

static int
op_project(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int i;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, rtyp, ecp, txp, retvalp);

    for (i = 1; i < argc; i++) {
        char *attrname = RDB_obj_string(argv[i]);

        if (RDB_tuple_set(retvalp, attrname,
                RDB_tuple_get(argv[0], attrname), ecp) != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_remove(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    int ret;
    char **attrv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, rtyp, ecp, txp, retvalp);

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
op_rename(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    int ret;
    int renc = (argc - 1) / 2;
    RDB_renaming *renv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, rtyp, ecp, txp, retvalp);

    renv = RDB_alloc(sizeof(RDB_renaming) * renc, ecp);
    if (renv == NULL) {
        return RDB_ERROR;
    }

    for (i = 0; i < renc; i++) {
        if (argv[1 + i]->typ != &RDB_STRING
                || argv[2 + i]->typ != &RDB_STRING) {
            RDB_free(renv);
            RDB_raise_type_mismatch("RENAME argument must be STRING", ecp);
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
op_join(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{    
    if (argc != 2) {
        RDB_raise_invalid_argument("invalid argument to JOIN", ecp);
        return RDB_ERROR;
    }

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, rtyp, ecp, txp, retvalp);

    return RDB_join_tuples(argv[0], argv[1], ecp, txp, retvalp);
}

static int
op_wrap(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i, j;
    int wrapc;
    RDB_wrapping *wrapv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, rtyp, ecp, txp, retvalp);

    if (argc < 1 || argc %2 != 1) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return RDB_ERROR;
    }

    wrapc = argc % 2;
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
op_unwrap(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i;
    char **attrv;

    if (argc < 1) {
        RDB_raise_invalid_argument("invalid argument to UNWRAP", ecp);
        return RDB_ERROR;
    }
    
    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, rtyp, ecp, txp, retvalp);

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
op_subscript(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object *objp = RDB_array_get(argv[0], RDB_obj_int(argv[1]), ecp);
    if (objp == NULL)
        return RDB_ERROR;

    return RDB_copy_obj(retvalp, objp, ecp);
}

static int
integer_float(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->var.float_val);
    return RDB_OK;
}

static int
integer_string(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_int_to_obj(retvalp, (RDB_int)
            strtol(argv[0]->var.bin.datap, &endp, 10));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to INTEGER failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
float_int(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) argv[0]->var.int_val);
    return RDB_OK;
}

static int
float_string(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_float_to_obj(retvalp, (RDB_float)
            strtod(argv[0]->var.bin.datap, &endp));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to FLOAT failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
string_obj(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_obj_to_string(retvalp, argv[0], ecp);
}

static int
length_string(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    size_t len = mbstowcs(NULL, argv[0]->var.bin.datap, 0);
    if (len == -1) {
        RDB_raise_invalid_argument("", ecp);
        return RDB_ERROR;
    }

    RDB_int_to_obj(retvalp, (RDB_int) len);
    return RDB_OK;
}

static int
substring(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int start = argv[1]->var.int_val;
    int len = argv[2]->var.int_val;
    int i;
    int cl;
    int bstart, blen;

    /* Operands must not be negative */
    if (len < 0 || start < 0) {
        RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
        return RDB_ERROR;
    }

    /* Find start of substring */
    bstart = 0;
    for (i = 0; i < start && bstart < argv[0]->var.bin.len - 1; i++) {
        cl = mblen(((char *) argv[0]->var.bin.datap) + bstart, 4);
        if (cl == -1) {
            RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
            return RDB_ERROR;
        }
        bstart += cl;
    }
    if (bstart >= argv[0]->var.bin.len - 1) {
        RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
        return RDB_ERROR;
    }

    /* Find end of substring */
    blen = 0;
    for (i = 0; i < len && bstart + blen < argv[0]->var.bin.len; i++) {
        cl = mblen(((char *) argv[0]->var.bin.datap) + bstart + blen, 4);
        if (cl == -1) {
            RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
            return RDB_ERROR;
        }
        blen += cl > 0 ? cl : 1;
    }
    if (bstart + blen >= argv[0]->var.bin.len) {
        RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
        return RDB_ERROR;
    }

    RDB_destroy_obj(retvalp, ecp);
    retvalp->typ = &RDB_STRING;
    retvalp->kind = RDB_OB_BIN;
    retvalp->var.bin.len = blen + 1;
    retvalp->var.bin.datap = RDB_alloc(retvalp->var.bin.len, ecp);
    if (retvalp->var.bin.datap == NULL) {
        return RDB_ERROR;
    }
    strncpy(retvalp->var.bin.datap, (char *) argv[0]->var.bin.datap
            + bstart, blen);
    ((char *) retvalp->var.bin.datap)[blen] = '\0';
    return RDB_OK;
}

static int
concat(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    size_t s1len = strlen(argv[0]->var.bin.datap);
    size_t dstsize = s1len + strlen(argv[1]->var.bin.datap) + 1;

    if (retvalp->kind == RDB_OB_INITIAL) {
        /* Turn *retvalp into a string */
        _RDB_set_obj_type(retvalp, &RDB_STRING);
        retvalp->var.bin.datap = RDB_alloc(dstsize, ecp);
        if (retvalp->var.bin.datap == NULL) {
            return RDB_ERROR;
        }
        retvalp->var.bin.len = dstsize;
    } else if (retvalp->typ == &RDB_STRING) {
        /* Grow string if necessary */
        if (retvalp->var.bin.len < dstsize) {
            void *datap = RDB_realloc(retvalp->var.bin.datap, dstsize, ecp);
            if (datap == NULL) {
                return RDB_ERROR;
            }
            retvalp->var.bin.datap = datap;
        }
    } else {
        RDB_raise_type_mismatch("invalid return type for || operator", ecp);
        return RDB_ERROR;
    }
    strcpy(retvalp->var.bin.datap, argv[0]->var.bin.datap);
    strcpy(((char *)retvalp->var.bin.datap) + s1len, argv[1]->var.bin.datap);
    return RDB_OK;
}

static int
matches(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    regex_t reg;
    int ret;

    ret = regcomp(&reg, argv[1]->var.bin.datap, REG_NOSUB);
    if (ret != 0) {
        RDB_raise_invalid_argument("invalid regular expression", ecp);
        return RDB_ERROR;
    }
    RDB_bool_to_obj(retvalp, (RDB_bool)
            (regexec(&reg, argv[0]->var.bin.datap, 0, NULL, 0) == 0));
    regfree(&reg);

    return RDB_OK;
}

static int
and(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->var.bool_val && argv[1]->var.bool_val);
    return RDB_OK;
}

static int
or(const char *name, int argc, RDB_object *argv[], RDB_type *rtyp,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->var.bool_val || argv[1]->var.bool_val);
    return RDB_OK;
}

static int
not(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool) !argv[0]->var.bool_val);
    return RDB_OK;
}

static int
lt(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("CMP", 2, argv, &RDB_INTEGER, NULL, 0,
            ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) < 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
let(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("CMP", 2, argv, &RDB_INTEGER, NULL, 0,
            ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return RDB_ERROR;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) <= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
gt(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("CMP", 2, argv, &RDB_INTEGER, NULL, 0,
            ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) > 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
get(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("CMP", 2, argv, &RDB_INTEGER, NULL, 0,
            ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) >= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
negate_int(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, -argv[0]->var.int_val);
    return RDB_OK;
}

static int
negate_float(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, -argv[0]->var.float_val);
    return RDB_OK;
}

static int
add_int(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val + argv[1]->var.int_val);
    return RDB_OK;
}

static int
add_float(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->var.float_val + argv[1]->var.float_val);
    return RDB_OK;
}

static int
subtract_int(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val - argv[1]->var.int_val);
    return RDB_OK;
}

static int
subtract_float(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->var.float_val - argv[1]->var.float_val);
    return RDB_OK;
}

static int
multiply_int(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val * argv[1]->var.int_val);
    return RDB_OK;
}

static int
multiply_float(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->var.float_val * argv[1]->var.float_val);
    return RDB_OK;
}

static int
divide_int(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->var.int_val == 0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_int_to_obj(retvalp, argv[0]->var.int_val / argv[1]->var.int_val);
    return RDB_OK;
}

static int
divide_float(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->var.float_val == 0.0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp,
            argv[0]->var.float_val / argv[1]->var.float_val);
    return RDB_OK;
}

static int
length_array(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int len;

    if (argc != 1) {
        RDB_raise_invalid_argument("invalid number of arguments to LENGTH",
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
op_getenv(const char *name, int argc, RDB_object *argv[], RDB_type *typ,
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    char *valp = getenv(RDB_obj_string(argv[0]));

    /* If the environment variable does not exist, raise RESOURCE_NOT_FOUND */
    if (valp == NULL) {
        RDB_raise_resource_not_found(RDB_obj_string(argv[0]), ecp);
        return RDB_ERROR;
    }
    return RDB_string_to_obj(retvalp, valp, ecp);
}

static int
put_builtin_ro_op(const char *name, int argc, RDB_type **argtv, RDB_type *rtyp,
        RDB_ro_op_func *fp, RDB_exec_context *ecp)
{
    int ret;
    RDB_op_data *datap = _RDB_new_ro_op_data(rtyp, fp, ecp);
    if (datap == NULL)
        return RDB_ERROR;

    ret =  RDB_put_op(&_RDB_builtin_ro_op_map, name, argc, argtv,
            datap, ecp);
    if (ret != RDB_OK) {
        RDB_free_op_data(datap, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
_RDB_init_builtin_ops(RDB_exec_context *ecp)
{
    static RDB_bool initialized = RDB_FALSE;
    RDB_type *argtv[3];
    int ret;

    if (initialized)
        return RDB_OK;
    initialized = RDB_TRUE;

    RDB_init_op_map(&_RDB_builtin_ro_op_map);

    argtv[0] = &RDB_FLOAT;
    ret = put_builtin_ro_op("INTEGER", 1, argtv, &RDB_INTEGER, &integer_float,
            ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    argtv[0] = &RDB_STRING;
    ret = put_builtin_ro_op("INTEGER", 1, argtv, &RDB_INTEGER, &integer_string,
            ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    argtv[0] = &RDB_INTEGER;
    ret = put_builtin_ro_op("FLOAT", 1, argtv, &RDB_FLOAT, &float_int, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    argtv[0] = &RDB_STRING;
    ret = put_builtin_ro_op("FLOAT", 1, argtv, &RDB_FLOAT, &float_string, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    argtv[0] = &RDB_INTEGER;

    ret = put_builtin_ro_op("STRING", 1, argtv, &RDB_STRING, &string_obj, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;

    ret = put_builtin_ro_op("STRING", 1, argtv, &RDB_STRING, &string_obj, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;

    ret = put_builtin_ro_op("STRING", 1, argtv, &RDB_STRING, &string_obj, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_STRING;

    ret = put_builtin_ro_op("LENGTH", 1, argtv, &RDB_INTEGER, &length_string, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_STRING;
    argtv[1] = &RDB_INTEGER;
    argtv[2] = &RDB_INTEGER;

    ret = put_builtin_ro_op("SUBSTRING", 3, argtv, &RDB_STRING, &substring, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_STRING;
    argtv[1] = &RDB_STRING;

    ret = put_builtin_ro_op("||", 2, argtv, &RDB_STRING, &concat, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_STRING;
    argtv[1] = &RDB_STRING;

    ret = put_builtin_ro_op("MATCHES", 2, argtv, &RDB_BOOLEAN, &matches, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_BOOLEAN;
    argtv[1] = &RDB_BOOLEAN;

    ret = put_builtin_ro_op("AND", 2, argtv, &RDB_BOOLEAN, &and, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_BOOLEAN;
    argtv[1] = &RDB_BOOLEAN;

    ret = put_builtin_ro_op("OR", 2, argtv, &RDB_BOOLEAN, &or, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_BOOLEAN;

    ret = put_builtin_ro_op("NOT", 1, argtv, &RDB_BOOLEAN, &not, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_INTEGER;
    argtv[1] = &RDB_INTEGER;

    ret = put_builtin_ro_op("<", 2, argtv, &RDB_BOOLEAN, &lt, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("<", 2, argtv, &RDB_BOOLEAN, &lt, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("<", 2, argtv, &RDB_BOOLEAN, &lt, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_STRING;
    argtv[1] = &RDB_STRING;

    ret = put_builtin_ro_op("<", 2, argtv, &RDB_BOOLEAN, &lt, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_INTEGER;
    argtv[1] = &RDB_INTEGER;

    ret = put_builtin_ro_op("<=", 2, argtv, &RDB_BOOLEAN, &let, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("<=", 2, argtv, &RDB_BOOLEAN, &let, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("<=", 2, argtv, &RDB_BOOLEAN, &let, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_STRING;
    argtv[1] = &RDB_STRING;

    ret = put_builtin_ro_op("<=", 2, argtv, &RDB_BOOLEAN, &let, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_INTEGER;
    argtv[1] = &RDB_INTEGER;

    ret = put_builtin_ro_op(">", 2, argtv, &RDB_BOOLEAN, &gt, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op(">", 2, argtv, &RDB_BOOLEAN, &gt, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op(">", 2, argtv, &RDB_BOOLEAN, &gt, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_STRING;
    argtv[1] = &RDB_STRING;

    ret = put_builtin_ro_op(">", 2, argtv, &RDB_BOOLEAN, &gt, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_INTEGER;
    argtv[1] = &RDB_INTEGER;

    ret = put_builtin_ro_op(">=", 2, argtv, &RDB_BOOLEAN, &get, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op(">=", 2, argtv, &RDB_BOOLEAN, &get, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op(">=", 2, argtv, &RDB_BOOLEAN, &get, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_STRING;
    argtv[1] = &RDB_STRING;

    ret = put_builtin_ro_op(">=", 2, argtv, &RDB_BOOLEAN, &get, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_BOOLEAN;
    argtv[1] = &RDB_BOOLEAN;

    ret = put_builtin_ro_op("=", 2, argtv, &RDB_BOOLEAN, &_RDB_eq_bool, ecp);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    argtv[0] = &RDB_INTEGER;
    argtv[1] = &RDB_INTEGER;

    ret = put_builtin_ro_op("=", 2, argtv, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("=", 2, argtv, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("=", 2, argtv, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_STRING;
    argtv[1] = &RDB_STRING;

    ret = put_builtin_ro_op("=", 2, argtv, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_BINARY;
    argtv[1] = &RDB_BINARY;

    ret = put_builtin_ro_op("=", 2, argtv, &RDB_BOOLEAN, &_RDB_eq_binary, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_BOOLEAN;
    argtv[1] = &RDB_BOOLEAN;

    ret = put_builtin_ro_op("<>", 2, argtv, &RDB_BOOLEAN, &neq_bool, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_INTEGER;
    argtv[1] = &RDB_INTEGER;

    ret = put_builtin_ro_op("<>", 2, argtv, &RDB_BOOLEAN,
            &_RDB_obj_not_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("<>", 2, argtv, &RDB_BOOLEAN,
            &_RDB_obj_not_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("<>", 2, argtv, &RDB_BOOLEAN,
            &_RDB_obj_not_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_STRING;
    argtv[1] = &RDB_STRING;

    ret = put_builtin_ro_op("<>", 2, argtv, &RDB_BOOLEAN,
            &_RDB_obj_not_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_BINARY;
    argtv[1] = &RDB_BINARY;

    ret = put_builtin_ro_op("<>", 2, argtv, &RDB_BOOLEAN, &neq_binary, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_INTEGER;

    ret = put_builtin_ro_op("-", 1, argtv, &RDB_INTEGER, &negate_int, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;

    ret = put_builtin_ro_op("-", 1, argtv, &RDB_FLOAT, &negate_float, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_INTEGER;
    argtv[1] = &RDB_INTEGER;

    ret = put_builtin_ro_op("+", 2, argtv, &RDB_INTEGER, &add_int, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("+", 2, argtv, &RDB_FLOAT, &add_float, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_INTEGER;
    argtv[1] = &RDB_INTEGER;

    ret = put_builtin_ro_op("-", 2, argtv, &RDB_INTEGER, &subtract_int, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("-", 2, argtv, &RDB_FLOAT, &subtract_float, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_INTEGER;
    argtv[1] = &RDB_INTEGER;

    ret = put_builtin_ro_op("*", 2, argtv, &RDB_INTEGER, &multiply_int, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("*", 2, argtv, &RDB_FLOAT, &multiply_float, ecp);
    if (ret != RDB_OK)
        return ret;

    argtv[0] = &RDB_INTEGER;
    argtv[1] = &RDB_INTEGER;

    ret = put_builtin_ro_op("/", 2, argtv, &RDB_INTEGER, &divide_int, ecp);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    argtv[0] = &RDB_FLOAT;
    argtv[1] = &RDB_FLOAT;

    ret = put_builtin_ro_op("/", 2, argtv, &RDB_FLOAT, &divide_float, ecp);
    if (ret != RDB_OK)
        return ret;

    if (put_builtin_ro_op("TUPLE", -1, NULL, NULL, &op_tuple, ecp) != RDB_OK)
        return RDB_ERROR;

    if (put_builtin_ro_op("RELATION", -1, NULL, NULL, &op_relation, ecp)
            != RDB_OK)
        return RDB_ERROR;

    if (put_builtin_ro_op("ARRAY", -1, NULL, NULL, &op_array, ecp) != RDB_OK)
        return RDB_ERROR;

    if (put_builtin_ro_op("PROJECT", -1, NULL, NULL, &op_project, ecp)
            != RDB_OK)
        return RDB_ERROR;

    ret = put_builtin_ro_op("REMOVE", -1, NULL, NULL, &op_remove, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    ret = put_builtin_ro_op("RENAME", -1, NULL, NULL, &op_rename, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    ret = put_builtin_ro_op("JOIN", -1, NULL, NULL, &op_join, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = put_builtin_ro_op("WRAP", -1, NULL, NULL, &op_wrap, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = put_builtin_ro_op("UNWRAP", -1, NULL, NULL, &op_unwrap, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = put_builtin_ro_op("UNION", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = put_builtin_ro_op("MINUS", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = put_builtin_ro_op("SEMIMINUS", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    ret = put_builtin_ro_op("INTERSECT", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = put_builtin_ro_op("SEMIJOIN", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = put_builtin_ro_op("DIVIDE", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = put_builtin_ro_op("GROUP", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = put_builtin_ro_op("UNGROUP", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    if (put_builtin_ro_op("[]", -1, NULL, NULL, &op_subscript, ecp)
            != RDB_OK) {
        return RDB_ERROR;
    }

    if (put_builtin_ro_op("LENGTH", -1, NULL, NULL, &length_array, ecp)
            != RDB_OK) {
        return RDB_ERROR;
    }

    argtv[0] = &RDB_STRING;

    ret = put_builtin_ro_op("GETENV", 1, argtv, &RDB_STRING, &op_getenv, ecp);
    if (ret != RDB_OK)
        return ret;

    return RDB_OK;
}
