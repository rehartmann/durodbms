/*
 * $Id$
 *
 * Copyright (C) 2005-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include <regex.h>
#include <string.h>

RDB_hashmap _RDB_builtin_ro_op_map;

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

OPERATOR &lt; (DOUBLE, DOUBLE) RETURNS BOOLEAN;

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

OPERATOR &lt;= (DOUBLE, DOUBLE) RETURNS BOOLEAN;

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

OPERATOR &gt; (DOUBLE, DOUBLE) RETURNS BOOLEAN;

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

OPERATOR &gt;= (DOUBLE, DOUBLE) RETURNS BOOLEAN;

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

OPERATOR + (DOUBLE, DOUBLE) RETURNS DOUBLE;

<h4>Description</h4>

The addition operator.

<h4>Return value</h4>

The sum of the two operands.

<hr>

<h3 id="op_uminus">OPERATOR - (unary)</h3>

OPERATOR - (INTEGER) RETURNS INTEGER;

OPERATOR - (FLOAT) RETURNS FLOAT;

OPERATOR - (DOUBLE) RETURNS DOUBLE;

<h4>Description</h4>

The unary minus operator.

<h4>Return value</h4>

The operand, sign inverted.

<hr>

<h3 id="op_bminus">OPERATOR - (binary)</h3>

OPERATOR - (INTEGER, INTEGER) RETURNS INTEGER;

OPERATOR - (FLOAT, FLOAT) RETURNS FLOAT;

OPERATOR - (DOUBLE, DOUBLE) RETURNS DOUBLE;

<h4>Description</h4>

The subtraction operator.

<h4>Return value</h4>

The difference of the two operands.

<hr>

<h3 id="op_times">OPERATOR *</h3>

OPERATOR * (INTEGER, INTEGER) RETURNS INTEGER;;

OPERATOR * (FLOAT, FLOAT) RETURNS FLOAT;

OPERATOR * (DOUBLE, DOUBLE) RETURNS DOUBLE;

<h4>Description</h4>

The multiplication operator.

<h4>Return value</h4>

The product of the two operands.

<hr>

<h3 id="op_div">OPERATOR /</h3>

OPERATOR / (INTEGER, INTEGER) RETURNS INTEGER;

OPERATOR / (FLOAT, FLOAT) RETURNS FLOAT;

OPERATOR / (DOUBLE, DOUBLE) RETURNS DOUBLE;

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

OPERATOR INTEGER (DOUBLE) RETURNS INTEGER;

OPERATOR INTEGER (FLOAT) RETURNS INTEGER;

OPERATOR INTEGER (STRING) RETURNS INTEGER;

<h4>Description</h4>

Converts the operand to INTEGER.

<h4>Return value</h4>

The operand, converted to INTEGER.

<hr>

<h3 id="op_float">OPERATOR FLOAT</h3>

OPERATOR FLOAT (INTEGER) RETURNS FLOAT;

OPERATOR FLOAT (DOUBLE) RETURNS FLOAT;

OPERATOR FLOAT (STRING) RETURNS FLOAT;

<h4>Description</h4>

Converts the operand to FLOAT.

<h4>Return value</h4>

The operand, converted to FLOAT.

<hr>

<h3 id="op_double">OPERATOR DOUBLE</h3>

OPERATOR DOUBLE (INTEGER) RETURNS DOUBLE;

OPERATOR DOUBLE (FLOAT) RETURNS DOUBLE;

OPERATOR DOUBLE (STRING) RETURNS DOUBLE;

<h4>Description</h4>

Converts the operand to DOUBLE.

<h4>Return value</h4>

The operand, converted to DOUBLE.

<hr>

<h3 id="op_string">OPERATOR STRING</h3>

OPERATOR STRING (INTEGER) RETURNS STRING;

OPERATOR STRING (FLOAT) RETURNS STRING;

OPERATOR STRING (DOUBLE) RETURNS STRING;

<h4>Description</h4>

Converts the operand to a string.

<h4>Return value</h4>

The operand, converted to STRING.

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

OPERATOR AVG(R <em>RELATION</em>, ATTR INTEGER) RETURNS DOUBLE;

OPERATOR AVG(R <em>RELATION</em>, ATTR FLOAT) RETURNS FLOAT;

OPERATOR AVG(R <em>RELATION</em>, ATTR DOUBLE) RETURNS DOUBLE;

<h4>Description</h4>

The AVG aggregate operator.
For the semantics, see RDB_avg().

<hr>

<h3 id="op_max">OPERATOR MAX</h3>

OPERATOR MAX(R <em>RELATION</em>, ATTR INTEGER) RETURNS INTEGER;

OPERATOR MAX(R <em>RELATION</em>, ATTR FLOAT) RETURNS FLOAT;

OPERATOR MAX(R <em>RELATION</em>, ATTR DOUBLE) RETURNS DOUBLE;

<h4>Description</h4>

The MAX aggregate operator.
For the semantics, see RDB_max().

<hr>

<h3 id="op_min">OPERATOR MIN</h3>

OPERATOR MIN(R <em>RELATION</em>, ATTR INTEGER) RETURNS INTEGER;

OPERATOR MIN(R <em>RELATION</em>, ATTR FLOAT) RETURNS FLOAT;

OPERATOR MIN(R <em>RELATION</em>, ATTR DOUBLE) RETURNS DOUBLE;

<h4>Description</h4>

The MIN aggregate operator.
For the semantics, see RDB_min().

<hr>

<h3 id="op_sum">OPERATOR SUM</h3>

OPERATOR SUM(R <em>RELATION</em>, ATTR INTEGER) RETURNS INTEGER;

OPERATOR SUM(R <em>RELATION</em>, ATTR FLOAT) RETURNS FLOAT;

OPERATOR SUM(R <em>RELATION</em>, ATTR DOUBLE) RETURNS DOUBLE;

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

@section tup-rel-ops Built-in tuple and relational operators

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

OPERATOR RENAME(R <em>RELATION</em>, SRC_ATTRNAME STRING, DST_ATTRNAME ...) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_ungroup">OPERATOR UNGROUP</h3>

OPERATOR UNGROUP(R <em>RELATION</em>, ATTRNAME STRING) RETURNS <em>RELATION</em>;

<hr>

<h3 id="op_union">OPERATOR UNION</h3>

OPERATOR UNION(R1 <em>RELATION</em>, R2 <em>RELATION</em>) RETURNS <em>RELATION</em>;

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
neq_bool(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->var.bool_val != argv[1]->var.bool_val));
    return RDB_OK;
}

static int
neq_binary(const char *name, int argc, RDB_object *argv[],
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
op_vtable(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    RDB_expression *argexp;
    RDB_expression *exp;

    /*
     * Convert arguments to expressions
     */
    exp = RDB_ro_op(name, argc, ecp);
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
op_vtable_wrapfn(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    return op_vtable(name, argc, argv, ecp, txp, retvalp);
}

static int
op_project(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int i;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    for (i = 1; i < argc; i++) {
        char *attrname = RDB_obj_string(argv[i]);

        if (RDB_tuple_set(retvalp, attrname,
                RDB_tuple_get(argv[0], attrname), ecp) != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_remove(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    int ret;
    char **attrv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    attrv = malloc(sizeof (char *) * (argc - 1));
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    ret = RDB_remove_tuple(argv[0], argc - 1, attrv, ecp, retvalp);
    free(attrv);
    return ret;
}

static int
op_rename(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    int ret;
    int renc = (argc - 1) / 2;
    RDB_renaming *renv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    renv = malloc(sizeof(RDB_renaming) * renc);
    if (renv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < renc; i++) {
        if (argv[1 + i]->typ != &RDB_STRING
                || argv[2 + i]->typ != &RDB_STRING) {
            free(renv);
            RDB_raise_type_mismatch("RENAME argument must be STRING", ecp);
            return RDB_ERROR;
        }
        renv[i].from = RDB_obj_string(argv[1 + i * 2]);
        renv[i].to = RDB_obj_string(argv[2 + i * 2]);
    }

    ret = RDB_rename_tuple(argv[0], renc, renv, ecp, retvalp);
    free(renv);
    return ret;
}

static int
op_join(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{    
    if (argc != 2) {
        RDB_raise_invalid_argument("invalid argument to JOIN", ecp);
        return RDB_ERROR;
    }

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    return RDB_join_tuples(argv[0], argv[1], ecp, txp, retvalp);
}

static int
op_wrap(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i, j;
    int wrapc;
    RDB_wrapping *wrapv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    if (argc < 1 || argc %2 != 1) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return RDB_ERROR;
    }

    wrapc = argc % 2;
    if (wrapc > 0) {
        wrapv = malloc(sizeof(RDB_wrapping) * wrapc);
        if (wrapv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }
    for (i = 0; i < wrapc; i++) {
        wrapv[i].attrv = NULL;
    }

    for (i = 0; i < wrapc; i++) {
        RDB_object *objp;

        wrapv[i].attrc =  RDB_array_length(argv[i * 2 + 1], ecp);
        wrapv[i].attrv = malloc(sizeof (char *) * wrapv[i].attrc);
        if (wrapv[i].attrv == NULL) {
            RDB_raise_no_memory(ecp);
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
        free(wrapv[i].attrv);
    }
    if (wrapc > 0)
        free(wrapv);

    return ret;
}

static int
op_unwrap(const char *name, int argc, RDB_object *argv[],
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
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    attrv = malloc(sizeof(char *) * (argc - 1));
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    ret = RDB_unwrap_tuple(argv[0], argc - 1, attrv, ecp, retvalp);
    free(attrv);
    return ret;
}

static int
integer_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->var.float_val);
    return RDB_OK;
}

static int
integer_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->var.double_val);
    return RDB_OK;
}

static int
integer_string(const char *name, int argc, RDB_object *argv[],
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
float_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) argv[0]->var.int_val);
    return RDB_OK;
}

static int
float_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) argv[0]->var.double_val);
    return RDB_OK;
}

static int
float_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_float_to_obj(retvalp, (RDB_float)
            strtod(argv[0]->var.bin.datap, &endp));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to DOUBLE failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
double_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp, (RDB_double) argv[0]->var.int_val);
    return RDB_OK;
}

static int
double_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp, (RDB_double) argv[0]->var.float_val);
    return RDB_OK;
}

static int
double_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_double_to_obj(retvalp, (RDB_double)
            strtod(argv[0]->var.bin.datap, &endp));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to DOUBLE failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
string_obj(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_obj_to_string(retvalp, argv[0], ecp);
}

static int
length_string(const char *name, int argc, RDB_object *argv[],
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
substring(const char *name, int argc, RDB_object *argv[],
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
    retvalp->var.bin.datap = malloc(retvalp->var.bin.len);
    if (retvalp->var.bin.datap == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    strncpy(retvalp->var.bin.datap, (char *) argv[0]->var.bin.datap
            + bstart, blen);
    ((char *) retvalp->var.bin.datap)[blen] = '\0';
    return RDB_OK;
}

static int
concat(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    size_t s1len = strlen(argv[0]->var.bin.datap);
    size_t dstsize = s1len + strlen(argv[1]->var.bin.datap) + 1;

    if (retvalp->kind == RDB_OB_INITIAL) {
        /* Turn *retvalp into a string */
        _RDB_set_obj_type(retvalp, &RDB_STRING);
        retvalp->var.bin.datap = malloc(dstsize);
        if (retvalp->var.bin.datap == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        retvalp->var.bin.len = dstsize;
    } else if (retvalp->typ == &RDB_STRING) {
        /* Grow string if necessary */
        if (retvalp->var.bin.len < dstsize) {
            void *datap = realloc(retvalp->var.bin.datap, dstsize);
            if (datap == NULL) {
                RDB_raise_no_memory(ecp);
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
matches(const char *name, int argc, RDB_object *argv[],
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
and(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->var.bool_val && argv[1]->var.bool_val);
    return RDB_OK;
}

static int
or(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->var.bool_val || argv[1]->var.bool_val);
    return RDB_OK;
}

static int
not(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool) !argv[0]->var.bool_val);
    return RDB_OK;
}

static int
lt(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("CMP", 2, argv, NULL, 0, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) < 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
let(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("CMP", 2, argv, NULL, 0, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return RDB_ERROR;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) <= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
gt(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("CMP", 2, argv, NULL, 0, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) > 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
get(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("CMP", 2, argv, NULL, 0, ecp, txp,
            &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) >= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
negate_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, -argv[0]->var.int_val);
    return RDB_OK;
}

static int
negate_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, -argv[0]->var.float_val);
    return RDB_OK;
}

static int
negate_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp, -argv[0]->var.double_val);
    return RDB_OK;
}

static int
add_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val + argv[1]->var.int_val);
    return RDB_OK;
}

static int
add_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp,
            argv[0]->var.double_val + argv[1]->var.double_val);
    return RDB_OK;
}

static int
subtract_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val - argv[1]->var.int_val);
    return RDB_OK;
}

static int
subtract_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->var.float_val - argv[1]->var.float_val);
    return RDB_OK;
}

static int
subtract_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp,
            argv[0]->var.double_val - argv[1]->var.double_val);
    return RDB_OK;
}

static int
multiply_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val * argv[1]->var.int_val);
    return RDB_OK;
}

static int
multiply_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->var.float_val * argv[1]->var.float_val);
    return RDB_OK;
}

static int
multiply_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp,
            argv[0]->var.double_val * argv[1]->var.double_val);
    return RDB_OK;
}

static int
divide_int(const char *name, int argc, RDB_object *argv[],
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
divide_float(const char *name, int argc, RDB_object *argv[],
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
divide_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->var.double_val == 0.0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_double_to_obj(retvalp,
            argv[0]->var.double_val / argv[1]->var.double_val);
    return RDB_OK;
}

int
_RDB_put_builtin_ro_op(RDB_ro_op_desc *op, RDB_exec_context *ecp)
{
    int ret;
    RDB_ro_op_desc *fop = RDB_hashmap_get(&_RDB_builtin_ro_op_map, op->name);

    if (fop == NULL) {
        op->nextp = NULL;
        ret = RDB_hashmap_put(&_RDB_builtin_ro_op_map, op->name, op);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, NULL);
            return RDB_ERROR;
        }
    } else {
        op->nextp = fop->nextp;
        fop->nextp = op;
    }
    return RDB_OK;
}

int
_RDB_init_builtin_ops(RDB_exec_context *ecp)
{
    static RDB_bool initialized = RDB_FALSE;
    RDB_ro_op_desc *op;
    int ret;

    if (initialized)
        return RDB_OK;
    initialized = RDB_TRUE;

    RDB_init_hashmap(&_RDB_builtin_ro_op_map, 64);

    op = _RDB_new_ro_op("INTEGER", 1, &RDB_INTEGER, &integer_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("INTEGER", 1, &RDB_INTEGER, &integer_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("INTEGER", 1, &RDB_INTEGER, &integer_string, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("FLOAT", 1, &RDB_FLOAT, &float_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("FLOAT", 1, &RDB_FLOAT, &float_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("FLOAT", 1, &RDB_FLOAT, &float_string, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("DOUBLE", 1, &RDB_DOUBLE, &double_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("DOUBLE", 1, &RDB_DOUBLE, &double_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("DOUBLE", 1, &RDB_DOUBLE, &double_string, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("STRING", 1, &RDB_STRING, &string_obj, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("STRING", 1, &RDB_STRING, &string_obj, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("STRING", 1, &RDB_STRING, &string_obj, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("LENGTH", 1, &RDB_INTEGER, &length_string, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("SUBSTRING", 3, &RDB_STRING, &substring, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_INTEGER;
    op->argtv[2] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("||", 2, &RDB_STRING, &concat, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("MATCHES", 2, &RDB_BOOLEAN, &matches, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("AND", 2, &RDB_BOOLEAN, &and, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("OR", 2, &RDB_BOOLEAN, &or, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("NOT", 1, &RDB_BOOLEAN, &not, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<", 2, &RDB_BOOLEAN, &lt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<", 2, &RDB_BOOLEAN, &lt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<", 2, &RDB_BOOLEAN, &lt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<", 2, &RDB_BOOLEAN, &lt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<=", 2, &RDB_BOOLEAN, &let, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<=", 2, &RDB_BOOLEAN, &let, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<=", 2, &RDB_BOOLEAN, &let, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<=", 2, &RDB_BOOLEAN, &let, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">", 2, &RDB_BOOLEAN, &gt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">", 2, &RDB_BOOLEAN, &gt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">", 2, &RDB_BOOLEAN, &gt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">", 2, &RDB_BOOLEAN, &gt, ecp);
    if (op == NULL) { 
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">=", 2, &RDB_BOOLEAN, &get, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">=", 2, &RDB_BOOLEAN, &get, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">=", 2, &RDB_BOOLEAN, &get, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">=", 2, &RDB_BOOLEAN, &get, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, &_RDB_eq_bool, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, &_RDB_eq_binary, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BINARY;
    op->argtv[1] = &RDB_BINARY;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &neq_bool, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &_RDB_obj_not_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &_RDB_obj_not_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &_RDB_obj_not_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &_RDB_obj_not_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &neq_binary, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BINARY;
    op->argtv[1] = &RDB_BINARY;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 1, &RDB_INTEGER, &negate_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 1, &RDB_FLOAT, &negate_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 1, &RDB_DOUBLE, &negate_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("+", 2, &RDB_INTEGER, &add_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("+", 2, &RDB_FLOAT, &add_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("+", 2, &RDB_DOUBLE, &add_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 2, &RDB_INTEGER, &subtract_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 2, &RDB_FLOAT, &subtract_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 2, &RDB_DOUBLE, &subtract_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("*", 2, &RDB_INTEGER, &multiply_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("*", 2, &RDB_FLOAT, &multiply_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("*", 2, &RDB_DOUBLE, &multiply_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("/", 2, &RDB_INTEGER, &divide_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    op = _RDB_new_ro_op("/", 2, &RDB_FLOAT, &divide_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("/", 2, &RDB_DOUBLE, &divide_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    if (_RDB_put_builtin_ro_op(op, ecp) != RDB_OK)
        return RDB_ERROR;

    op = _RDB_new_ro_op("PROJECT", -1, NULL, &op_project, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    if (_RDB_put_builtin_ro_op(op, ecp) != RDB_OK)
        return RDB_ERROR;

    op = _RDB_new_ro_op("REMOVE", -1, NULL, &op_remove, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("RENAME", -1, NULL, &op_rename, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("JOIN", -1, NULL, &op_join, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("WRAP", -1, NULL, &op_wrap, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("UNWRAP", -1, NULL, &op_unwrap, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("UNION", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("MINUS", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("SEMIMINUS", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("INTERSECT", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("SEMIJOIN", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("DIVIDE", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("GROUP", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("UNGROUP", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    ret = _RDB_put_builtin_ro_op(op, ecp);
    if (ret != RDB_OK)
        return ret;

    return RDB_OK;
}
