/*
 * $Id$
 *
 * Copyright (C) 2005-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "tostr.h"
#include "optimize.h"
#include <obj/objinternal.h>

#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <regex.h>
#ifdef _WIN32
#include "Shlwapi.h"
#else
#include <fnmatch.h>
#endif

RDB_op_map RDB_builtin_ro_op_map;

/** @page builtin-ops Built-in read-only operators
@section scalar-ops Built-in scalar operators

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

<hr>

<h3 id="op_plus">OPERATOR +</h3>

OPERATOR + (integer, integer) RETURNS integer;

OPERATOR + (float, float) RETURNS float;

<h4>Description</h4>

The addition operator.

<h4>Return value</h4>

The sum of the two operands.

<hr>

<h3 id="op_uminus">OPERATOR - (unary)</h3>

OPERATOR - (integer) RETURNS integer;

OPERATOR - (float) RETURNS float;

<h4>Description</h4>

The unary minus operator.

<h4>Return value</h4>

The operand, sign inverted.

<hr>

<h3 id="op_bminus">OPERATOR - (binary)</h3>

OPERATOR - (integer, integer) RETURNS integer;

OPERATOR - (float, float) RETURNS float;

<h4>Description</h4>

The subtraction operator.

<h4>Return value</h4>

The difference of the two operands.

<hr>

<h3 id="op_times">OPERATOR *</h3>

OPERATOR * (integer, integer) RETURNS integer;;

OPERATOR * (float, float) RETURNS float;

<h4>Description</h4>

The multiplication operator.

<h4>Return value</h4>

The product of the two operands.

<hr>

<h3 id="op_div">OPERATOR /</h3>

OPERATOR / (integer, integer) RETURNS integer;

OPERATOR / (float, float) RETURNS float;

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

<h3 id="op_and">OPERATOR and</h3>

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

<h3 id="op_concat">OPERATOR ||</h3>

OPERATOR || (string, string) RETURNS string;

<h4>Description</h4>

The string concatenation operator.

<h4>Return value</h4>

The result of the concatenation of the operands.

<hr>

<h3 id="op_strlen">OPERATOR strlen</h3>

OPERATOR strlen (string) RETURNS integer;

<h4>Description</h4>

The string length operator.

<h4>Return value</h4>

The length of the operand.

<hr>

<h3 id="substr">OPERATOR substr</h3>

OPERATOR substr(s string, start integer, length integer) RETURNS
string;

<h4>Description</h4>

The substring operator.

<h4>Return value</h4>

The substring of S with length <var>length</var> starting at position
START.

<h4>Errors</h4>

<dl>
<dt>invalid_argument_error
<dd>START is negative, or START + LENGTH is greater than LENGTH(S).
</dl>

<hr>

<h3 id="op_like">OPERATOR like</h3>

OPERATOR like (s string, pattern string) RETURNS boolean;

<h4>Description</h4>

Pattern matching operator. A period ('.') matches a single character;
an asterisk ('*') matches multiple characters.

<h4>Return value</h4>

RDB_TRUE if s matches pattern, RDB_FALSE otherwise.

<hr>

<h3 id="op_regex_like">OPERATOR regex_like</h3>

OPERATOR regex_like (s string, pattern string) RETURNS boolean;

<h4>Description</h4>

The regular expression matching operator.

<h4>Return value</h4>

RDB_TRUE if s matches pattern, RDB_FALSE otherwise.

<hr>

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

<h3 id="op_is_empty">OPERATOR is_empty</h3>

OPERATOR is_empty (RELATION { * }) RETURNS boolean;

<h4>Description</h4>

Checks if a table is empty.

<h4>Return value</h4>

RDB_TRUE if the relation-valued operand is empty, RDB_FALSE
otherwise.

<hr>

<h3 id="op_count">OPERATOR count</h3>

OPERATOR count (RELATION { * }) RETURNS integer;

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

RDB_TRUE if <var>r</var> contains <var>t</var>, RDB_FALSE otherwise.

<hr>

<h3 id="op_subset_of">OPERATOR subset_of</h3>

OPERATOR subset_of (R1 RELATION { * }, R2 RELATION { * }) RETURNS boolean;

<h4>Description</h4>

Checks if a table is a subset of another table.

<h4>Return value</h4>

RDB_TRUE if the <var>R1</var> is a subset of <var>R2</var>, RDB_FALSE otherwise.

<hr>

<h3 id="op_any">OPERATOR any</h3>

OPERATOR any(R RELATION { * }, ATTR boolean) RETURNS boolean;

<h4>Description</h4>

The <code>any</code> aggregate operator.
For the semantics, see RDB_any().

<hr>

<h3 id="op_all">OPERATOR all</h3>

OPERATOR all(R RELATION { * }, ATTR boolean) RETURNS boolean;

<h4>Description</h4>

The <code>all</code> aggregate operator.
For the semantics, see RDB_all().

<hr>

<h3 id="op_avg">OPERATOR avg</h3>

OPERATOR avg(R RELATION { * }, ATTR INTEGER) RETURNS float;

OPERATOR avg(R RELATION { * }, ATTR float) RETURNS float;

<h4>Description</h4>

The <code>avg</code> aggregate operator.
For the semantics, see RDB_avg().

<hr>

<h3 id="op_max">OPERATOR max</h3>

OPERATOR max(R RELATION { * }, attr integer) RETURNS integer;

OPERATOR max(R RELATION { * }, attr float) RETURNS float;

<h4>Description</h4>

The <code>max</code> aggregate operator.
For the semantics, see RDB_max().

<hr>

<h3 id="op_min">OPERATOR min</h3>

OPERATOR min(R RELATION { * }, attr integer) RETURNS INTEGER;

OPERATOR min(R RELATION { * }, attr float) RETURNS float;

<h4>Description</h4>

The <code>min</code> aggregate operator.
For the semantics, see RDB_min().

<hr>

<h3 id="op_sum">OPERATOR sum</h3>

OPERATOR SUM(R RELATION { * }, attr integer) RETURNS integer;

OPERATOR SUM(R RELATION { * }, attr float) RETURNS float;

<h4>Description</h4>

The SUM aggregate operator.
For the semantics, see RDB_sum().

<hr>

<h3 id="op_if">OPERATOR if</h3>

OPERATOR IF (B boolean, V1 <em>ANY</em>, V2 <em>ANY</em>) RETURNS <em>ANY</em>;

<h4>Description</h4>

The IF-THEN-ELSE operator.

<h4>Return value</h4>

<var>V1</var> if <var>B</var> is RDB_TRUE, <var>V2</var> otherwise.

<hr>

<h3 id="op_getenv">OPERATOR getenv</h3>

MODULE os;

OPERATOR getenv (name string) RETURNS string;

END MODULE;

<h4>Description</h4>

Reads the environment variable <var>name</var>.

<h4>Return value</h4>

The value of the environment variable <var>name</var>.

<hr>

@section tup-rel-ops Built-in tuple, relational, and array operators

<h3 id="op_tuple">OPERATOR TUPLE</h3>

OPERATOR TUPLE(ATTRNAME string, ATTRVAL <em>ANY</em>, ...) RETURNS TUPLE { * };

<h4>Description</h4>

The tuple selector.

<hr>

<h3 id="op_relation">OPERATOR RELATION</h3>

OPERATOR RELATION(T TUPLE { * }, ...) RETURNS RELATION { * };

<h4>Description</h4>

The relation selector.

<hr>

<h3 id="op_array">OPERATOR array</h3>

OPERATOR array(<em>ANY</em>, ...) RETURNS <em>ARRAY</em>;

<h4>Description</h4>

The array selector.

<hr>

<h3 id="op_array_length">OPERATOR length</h3>

OPERATOR length (<em>ARRAY</em>) RETURNS integer;

<h4>Description</h4>

The array length operator.

<h4>Return value</h4>

The length of the operand.

<hr>

<h3 id="op_index_of">OPERATOR index_of</h3>

OPERATOR index_of (ARR <em>ARRAY</em>, DATA <em>ANY</em>) RETURNS integer;

<h4>Description</h4>

Returns the index of the first occurrence of DATA in the array ARR.

<h4>Return value</h4>

The index, or -1 if DATA does not appear in the array.

<hr>

<h3 id="op_to_tuple">OPERATOR to_tuple</h3>

OPERATOR TO_TUPLE(R RELATION { * }) RETURNS TUPLE { * };

<h4>Description</h4>

Extracts a single tuple from a relation.

<hr>

<h3 id="op_divide">OPERATOR divide</h3>

OPERATOR divide(R1 RELATION { * }, R2 RELATION { * }, R2 RELATION { * }) RETURNS RELATION { * };

<h4>Description</h4>

The relational three-argument (small) DIVIDE operator.

<hr>

<h3 id="op_extend">OPERATOR extend</h3>

OPERATOR extend(R RELATION { * }, ATTREXP <em>ANY</em>, attrname string, ...) RETURNS RELATION { * };

<hr>

<h3 id="op_group">OPERATOR group</h3>

OPERATOR group(R RELATION { * }, attrname string ...) RETURNS RELATION { * };

<hr>

<h3 id="op_intersect">OPERATOR intersect</h3>

OPERATOR intersect(R1 RELATION { * }, R2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_join">OPERATOR join</h3>

OPERATOR join(R1 RELATION { * }, R2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_minus">OPERATOR minus</h3>

OPERATOR minus(R1 RELATION { * }, R2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_project">OPERATOR project</h3>

OPERATOR project(R1 RELATION { * }, ATTRNAME string ...) RETURNS RELATION { * };

<hr>

<h3 id="op_remove">OPERATOR remove</h3>

OPERATOR remove(R RELATION { * }, ATTRNAME string ...) RETURNS RELATION { * };

<hr>

<h3 id="op_rename">OPERATOR rename</h3>

OPERATOR rename(R RELATION { * }, SRC_ATTRNAME string, DST_ATTRNAME string ...) RETURNS RELATION { * };

<hr>

<h3 id="op_ungroup">OPERATOR ungroup</h3>

OPERATOR ungroup(R RELATION { * }, ATTRNAME string) RETURNS RELATION { * };

<hr>

<h3 id="op_union">OPERATOR union</h3>

OPERATOR union(R1 RELATION { * }, R2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_d_union">OPERATOR d_union</h3>

OPERATOR d_union(R1 RELATION { * }, R2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_update">OPERATOR update</h3>

OPERATOR update(R1 RELATION { * }, DST_ATTRNAME string, SRC_EXPR <em>ANY</em>, ...) RETURNS RELATION { * };

<hr>

<h3 id="op_unwrap">OPERATOR unwrap</h3>

OPERATOR unwrap(ATTRNAME string, ...) RETURNS RELATION { * };

<hr>

<h3 id="op_semijoin">OPERATOR semijoin</h3>

OPERATOR semijoin(R1 RELATION { * }, R2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_semiminus">OPERATOR semiminus</h3>

OPERATOR semiminus(R1 RELATION { * }, R2 RELATION { * }) RETURNS RELATION { * };

<hr>

<h3 id="op_summarize">OPERATOR summarize</h3>

OPERATOR summarize(R1 RELATION { * }, R2 RELATION { * }, EXPR <em>ANY</em>, ATTRNAME string, ...) RETURNS RELATION { * };

<hr>

<h3 id="op_tclose">OPERATOR tclose</h3>

OPERATOR tclose(R RELATION { * }) RETURNS RELATION { * };

The transitive closure operator.

<hr>

<h3 id="op_where">OPERATOR where</h3>

OPERATOR where(R RELATION { * }, B boolean) RETURNS RELATION { * };

The relational WHERE operator.

<hr>

<h3 id="op_wrap">OPERATOR wrap</h3>

OPERATOR wrap(R RELATION { * }, SRC_ATTRS ARRAY OF string, DST_ATTR string ...) RETURNS RELATION { * };

The relational WRAP operator.

<hr>

<h3 id="op_sqrt">OPERATOR sqrt</h3>

OPERATOR sqrt(x float) RETURNS float;

The square root operator.

<hr>

<h3 id="op_sin">OPERATOR sin</h3>

OPERATOR sin (x float) RETURNS float;

The sine operator.

<hr>

<h3 id="op_cos">OPERATOR cos</h3>

OPERATOR cos(x float) RETURNS float;

The cosine operator.

<hr>

<h3 id="op_atan">OPERATOR atan</h3>

OPERATOR atan(x float) RETURNS float;

The arc tangent operator.

<hr>

<h3 id="op_atan2">OPERATOR atan2</h3>

OPERATOR atan2(y float, x float) RETURNS float;

The atan2 operator.

*/

/*
 * The following functions implement the built-in operators
 */
static int
neq_bool(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->val.bool_val != argv[1]->val.bool_val));
    return RDB_OK;
}

static int
neq_binary(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[0]->val.bin.len != argv[1]->val.bin.len)
        RDB_bool_to_obj(retvalp, RDB_TRUE);
    else if (argv[0]->val.bin.len == 0)
        RDB_bool_to_obj(retvalp, RDB_FALSE);
    else
        RDB_bool_to_obj(retvalp, (RDB_bool) (memcmp(argv[0]->val.bin.datap,
            argv[1]->val.bin.datap, argv[0]->val.bin.len) != 0));
    return RDB_OK;
}

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
            } else if (argv[i]->val.tb.exp != NULL) {
                argexp = RDB_dup_expr(argv[i]->val.tb.exp, ecp);
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
op_vtable_wrapfn(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    return op_vtable(argc, argv, op, ecp, txp, retvalp);
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
op_rel_binop(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_type *typ;

    if (argc != 2) {
        RDB_raise_invalid_argument("invalid # of arguments", ecp);
        return RDB_ERROR;
    }

    typ = RDB_obj_type(argv[0]);
    if (!RDB_type_is_relation(typ)) {
        RDB_raise_type_mismatch("relation argument required", ecp);
        return RDB_ERROR;
    }

    typ = RDB_obj_type(argv[1]);
    if (!RDB_type_is_relation(typ)) {
        RDB_raise_type_mismatch("relation argument required", ecp);
        return RDB_ERROR;
    }

    return op_vtable(argc, argv, op, ecp, txp, retvalp);
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
op_d_union(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_type *typ1, *typ2;

    if (argc != 2) {
        RDB_raise_invalid_argument("invalid # of arguments", ecp);
        return RDB_ERROR;
    }

    typ1 = RDB_obj_type(argv[0]);
    typ2 = RDB_obj_type(argv[1]);

    /* Check types */
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
    RDB_type *argtyp;

    if (argc != 1) {
        RDB_raise_invalid_argument("invalid # of arguments", ecp);
        return RDB_ERROR;
    }

    argtyp = RDB_obj_type(argv[0]);

    /* Relation UNION - check types */
    if (argtyp == NULL || !RDB_type_is_relation(argtyp)) {
        RDB_raise_type_mismatch("relation argument required", ecp);
        return RDB_ERROR;
    }

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
op_subscript(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object *objp = RDB_array_get(argv[0], RDB_obj_int(argv[1]), ecp);
    if (objp == NULL)
        return RDB_ERROR;

    return RDB_copy_obj(retvalp, objp, ecp);
}

static int
cast_as_integer_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->val.float_val);
    return RDB_OK;
}

static int
cast_as_integer_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_int_to_obj(retvalp, (RDB_int)
            strtol(argv[0]->val.bin.datap, &endp, 10));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to INTEGER failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
cast_as_float_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) argv[0]->val.int_val);
    return RDB_OK;
}

static int
cast_as_float_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_float_to_obj(retvalp, (RDB_float)
            strtod(argv[0]->val.bin.datap, &endp));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to float failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
cast_as_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_obj_to_string(retvalp, argv[0], ecp);
}

static int
cast_as_binary(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_binary_set(retvalp, 0, argv[0]->val.bin.datap,
            argv[0]->val.bin.len - 1, ecp);
}

static int
length_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    size_t len = mbstowcs(NULL, argv[0]->val.bin.datap, 0);
    if (len == -1) {
        RDB_raise_invalid_argument("obtaining string length failed", ecp);
        return RDB_ERROR;
    }

    RDB_int_to_obj(retvalp, (RDB_int) len);
    return RDB_OK;
}

static int
op_substr(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int start = argv[1]->val.int_val;
    int len = argv[2]->val.int_val;
    int i;
    int cl;
    int bstart, blen;

    /* Operands must not be negative */
    if (len < 0 || start < 0) {
        RDB_raise_invalid_argument("invalid substr argument", ecp);
        return RDB_ERROR;
    }

    /* Find start of substring */
    bstart = 0;
    for (i = 0; i < start && bstart < argv[0]->val.bin.len - 1; i++) {
        cl = mblen(((char *) argv[0]->val.bin.datap) + bstart, MB_CUR_MAX);
        if (cl == -1) {
            RDB_raise_invalid_argument("invalid substr argument", ecp);
            return RDB_ERROR;
        }
        bstart += cl;
    }
    if (bstart >= argv[0]->val.bin.len - 1) {
        RDB_raise_invalid_argument("invalid substr argument", ecp);
        return RDB_ERROR;
    }

    /* Find end of substring */
    blen = 0;
    for (i = 0; i < len && bstart + blen < argv[0]->val.bin.len; i++) {
        cl = mblen(((char *) argv[0]->val.bin.datap) + bstart + blen,
                MB_CUR_MAX);
        if (cl == -1) {
            RDB_raise_invalid_argument("invalid substr argument", ecp);
            return RDB_ERROR;
        }
        blen += cl > 0 ? cl : 1;
    }
    if (bstart + blen >= argv[0]->val.bin.len) {
        RDB_raise_invalid_argument("invalid substr argument", ecp);
        return RDB_ERROR;
    }

    return RDB_string_n_to_obj(retvalp,
            (char *) argv[0]->val.bin.datap + bstart, blen, ecp);
}

static int
op_concat(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    size_t s1len = strlen(argv[0]->val.bin.datap);
    size_t dstsize = s1len + strlen(argv[1]->val.bin.datap) + 1;

    if (retvalp->kind == RDB_OB_INITIAL) {
        /* Turn *retvalp into a string */
        RDB_set_obj_type(retvalp, &RDB_STRING);
        retvalp->val.bin.datap = RDB_alloc(dstsize, ecp);
        if (retvalp->val.bin.datap == NULL) {
            return RDB_ERROR;
        }
        retvalp->val.bin.len = dstsize;
    } else if (retvalp->typ == &RDB_STRING) {
        /* Grow string if necessary */
        if (retvalp->val.bin.len < dstsize) {
            void *datap = RDB_realloc(retvalp->val.bin.datap, dstsize, ecp);
            if (datap == NULL) {
                return RDB_ERROR;
            }
            retvalp->val.bin.datap = datap;
        }
    } else {
        RDB_raise_type_mismatch("invalid return type for || operator", ecp);
        return RDB_ERROR;
    }
    strcpy(retvalp->val.bin.datap, argv[0]->val.bin.datap);
    strcpy(((char *)retvalp->val.bin.datap) + s1len, argv[1]->val.bin.datap);
    return RDB_OK;
}

static int
op_starts_with(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *str = RDB_obj_string(argv[0]);
    char *pref = RDB_obj_string(argv[1]);
    size_t preflen = strlen(pref);

    if (preflen > strlen(str)) {
        RDB_bool_to_obj(retvalp, RDB_FALSE);
        return RDB_OK;
    }
    RDB_bool_to_obj(retvalp, (RDB_bool) (strncmp(str, pref, preflen) == 0));
    return RDB_OK;
}

static int
op_like(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
#ifdef _WIN32
    BOOL res = PathMatchSpec(RDB_obj_string(argv[0]), RDB_obj_string(argv[1]));
    RDB_bool_to_obj(retvalp, (RDB_bool) res);
#else /* Use POSIX fnmatch() */
    int ret = fnmatch(RDB_obj_string(argv[1]), RDB_obj_string(argv[0]),
            FNM_NOESCAPE);
    if (ret != 0 && ret != FNM_NOMATCH) {
        RDB_raise_system("fnmatch() failed", ecp);
        return RDB_ERROR;
    }
    RDB_bool_to_obj(retvalp, (RDB_bool) (ret == 0));
#endif
    return RDB_OK;
}

static int
op_regex_like(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    regex_t reg;
    int ret;

    ret = regcomp(&reg, argv[1]->val.bin.datap, REG_EXTENDED);
    if (ret != 0) {
        RDB_raise_invalid_argument("invalid regular expression", ecp);
        return RDB_ERROR;
    }
    RDB_bool_to_obj(retvalp, (RDB_bool)
            (regexec(&reg, argv[0]->val.bin.datap, 0, NULL, 0) == 0));
    regfree(&reg);

    return RDB_OK;
}

static int
and(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->val.bool_val && argv[1]->val.bool_val);
    return RDB_OK;
}

static int
or(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->val.bool_val || argv[1]->val.bool_val);
    return RDB_OK;
}

static int
xor(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->val.bool_val != argv[1]->val.bool_val);
    return RDB_OK;
}

static int
not(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool) !argv[0]->val.bool_val);
    return RDB_OK;
}

static int
lt(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->compare_op->opfn.ro_fp) (2, argv,
            argv[0]->typ->compare_op, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) < 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
let(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->compare_op->opfn.ro_fp) (2, argv,
            argv[0]->typ->compare_op, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return RDB_ERROR;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) <= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
gt(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->compare_op->opfn.ro_fp) (2, argv,
            argv[0]->typ->compare_op, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) > 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
get(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->compare_op->opfn.ro_fp) (2, argv,
            argv[0]->typ->compare_op, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) >= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
negate_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, -argv[0]->val.int_val);
    return RDB_OK;
}

static int
negate_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, -argv[0]->val.float_val);
    return RDB_OK;
}

static int
add_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->val.int_val + argv[1]->val.int_val);
    return RDB_OK;
}

static int
add_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->val.float_val + argv[1]->val.float_val);
    return RDB_OK;
}

static int
subtract_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->val.int_val - argv[1]->val.int_val);
    return RDB_OK;
}

static int
subtract_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->val.float_val - argv[1]->val.float_val);
    return RDB_OK;
}

static int
multiply_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->val.int_val * argv[1]->val.int_val);
    return RDB_OK;
}

static int
multiply_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->val.float_val * argv[1]->val.float_val);
    return RDB_OK;
}

static int
divide_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->val.int_val == 0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_int_to_obj(retvalp, argv[0]->val.int_val / argv[1]->val.int_val);
    return RDB_OK;
}

static int
divide_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->val.float_val == 0.0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp,
            argv[0]->val.float_val / argv[1]->val.float_val);
    return RDB_OK;
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
math_sqrt(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    double d = (double) RDB_obj_float(argv[0]);
    if (d < 0.0) {
        RDB_raise_invalid_argument(
                "square root of negative number is undefined", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp, (RDB_float) sqrt(d));
    return RDB_OK;
}

static int
math_sin(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) sin(RDB_obj_float(argv[0])));
    return RDB_OK;
}

static int
math_cos(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) cos(RDB_obj_float(argv[0])));
    return RDB_OK;
}

static int
math_atan(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) atan((double) RDB_obj_float(argv[0])));
    return RDB_OK;
}

static int
math_atan2(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            (RDB_float) atan2((double) RDB_obj_float(argv[0]),
                              (double) RDB_obj_float(argv[1])));
    return RDB_OK;
}

static int
op_getenv(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *valp = getenv(RDB_obj_string(argv[0]));

    /* If the environment variable does not exist, return empty string */
    return RDB_string_to_obj(retvalp, valp != NULL ? valp : "", ecp);
}

int
RDB_put_global_ro_op(const char *name, int argc, RDB_type **argtv,
        RDB_type *rtyp, RDB_ro_op_func *fp, RDB_exec_context *ecp)
{
    int ret;
    RDB_operator *datap = RDB_new_op_data(name, argc, argtv, rtyp, ecp);
    if (datap == NULL)
        return RDB_ERROR;
    datap->opfn.ro_fp = fp;

    ret =  RDB_put_op(&RDB_builtin_ro_op_map, datap, ecp);
    if (ret != RDB_OK) {
        RDB_free_op_data(datap, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
RDB_init_builtin_ops(RDB_exec_context *ecp)
{
    RDB_type *paramtv[3];
    int ret;

    /*
     * No errors other than no_memory_error may be raised here because the other error
     * types are not initialized yet.
     */

    RDB_init_op_map(&RDB_builtin_ro_op_map);

    paramtv[0] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("cast_as_integer", 1, paramtv, &RDB_INTEGER, &cast_as_integer_float,
            ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;
    ret = RDB_put_global_ro_op("cast_as_int", 1, paramtv, &RDB_INTEGER, &cast_as_integer_float,
            ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;

    ret = RDB_put_global_ro_op("cast_as_integer", 1, paramtv, &RDB_INTEGER, &cast_as_integer_string,
            ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;
    ret = RDB_put_global_ro_op("cast_as_int", 1, paramtv, &RDB_INTEGER, &cast_as_integer_string,
            ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("cast_as_float", 1, paramtv, &RDB_FLOAT, &cast_as_float_int, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;
    ret = RDB_put_global_ro_op("cast_as_rat", 1, paramtv, &RDB_FLOAT, &cast_as_float_int, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;
    ret = RDB_put_global_ro_op("cast_as_rational", 1, paramtv, &RDB_FLOAT, &cast_as_float_int, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    ret = RDB_put_global_ro_op("cast_as_float", 1, paramtv, &RDB_FLOAT, &cast_as_float_string, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;
    ret = RDB_put_global_ro_op("cast_as_rational", 1, paramtv, &RDB_FLOAT, &cast_as_float_string, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;
    ret = RDB_put_global_ro_op("cast_as_rat", 1, paramtv, &RDB_FLOAT, &cast_as_float_string, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("cast_as_string", 1, paramtv, &RDB_STRING, &cast_as_string, ecp);
    if (ret != RDB_OK)
        return ret;
    ret = RDB_put_global_ro_op("cast_as_char", 1, paramtv, &RDB_STRING, &cast_as_string, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("cast_as_string", 1, paramtv, &RDB_STRING, &cast_as_string, ecp);
    if (ret != RDB_OK)
        return ret;
    ret = RDB_put_global_ro_op("cast_as_char", 1, paramtv, &RDB_STRING, &cast_as_string, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_BINARY;

    ret = RDB_put_global_ro_op("cast_as_string", 1, paramtv, &RDB_STRING, &cast_as_string, ecp);
    if (ret != RDB_OK)
        return ret;
    ret = RDB_put_global_ro_op("cast_as_char", 1, paramtv, &RDB_STRING, &cast_as_string, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;

    ret = RDB_put_global_ro_op("cast_as_binary", 1, paramtv, &RDB_BINARY, &cast_as_binary, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;

    ret = RDB_put_global_ro_op("strlen", 1, paramtv, &RDB_INTEGER, &length_string, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_INTEGER;
    paramtv[2] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("substr", 3, paramtv, &RDB_STRING, &op_substr, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    ret = RDB_put_global_ro_op("||", 2, paramtv, &RDB_STRING, &op_concat, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    ret = RDB_put_global_ro_op("starts_with", 2, paramtv, &RDB_BOOLEAN, &op_starts_with, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("like", 2, paramtv, &RDB_BOOLEAN, &op_like, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("regex_like", 2, paramtv, &RDB_BOOLEAN,
            &op_regex_like, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_BOOLEAN;
    paramtv[1] = &RDB_BOOLEAN;

    ret = RDB_put_global_ro_op("and", 2, paramtv, &RDB_BOOLEAN, &and, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("or", 2, paramtv, &RDB_BOOLEAN, &or, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("xor", 2, paramtv, &RDB_BOOLEAN, &xor, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_BOOLEAN;

    ret = RDB_put_global_ro_op("not", 1, paramtv, &RDB_BOOLEAN, &not, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    ret = RDB_put_global_ro_op("<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    ret = RDB_put_global_ro_op("<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op(">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op(">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op(">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    ret = RDB_put_global_ro_op(">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op(">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op(">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op(">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    ret = RDB_put_global_ro_op(">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_BOOLEAN;
    paramtv[1] = &RDB_BOOLEAN;

    ret = RDB_put_global_ro_op("=", 2, paramtv, &RDB_BOOLEAN, &RDB_eq_bool, ecp);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("=", 2, paramtv, &RDB_BOOLEAN, RDB_dfl_obj_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("=", 2, paramtv, &RDB_BOOLEAN, RDB_dfl_obj_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("=", 2, paramtv, &RDB_BOOLEAN, RDB_dfl_obj_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    ret = RDB_put_global_ro_op("=", 2, paramtv, &RDB_BOOLEAN, RDB_dfl_obj_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_BINARY;
    paramtv[1] = &RDB_BINARY;

    ret = RDB_put_global_ro_op("=", 2, paramtv, &RDB_BOOLEAN, &RDB_eq_binary, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_BOOLEAN;
    paramtv[1] = &RDB_BOOLEAN;

    ret = RDB_put_global_ro_op("<>", 2, paramtv, &RDB_BOOLEAN, &neq_bool, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("<>", 2, paramtv, &RDB_BOOLEAN,
            &RDB_obj_not_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("<>", 2, paramtv, &RDB_BOOLEAN,
            &RDB_obj_not_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("<>", 2, paramtv, &RDB_BOOLEAN,
            &RDB_obj_not_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    ret = RDB_put_global_ro_op("<>", 2, paramtv, &RDB_BOOLEAN,
            &RDB_obj_not_equals, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_BINARY;
    paramtv[1] = &RDB_BINARY;

    ret = RDB_put_global_ro_op("<>", 2, paramtv, &RDB_BOOLEAN, &neq_binary, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("-", 1, paramtv, &RDB_INTEGER, &negate_int, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("-", 1, paramtv, &RDB_FLOAT, &negate_float, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("+", 2, paramtv, &RDB_INTEGER, &add_int, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("+", 2, paramtv, &RDB_FLOAT, &add_float, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("-", 2, paramtv, &RDB_INTEGER, &subtract_int, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("-", 2, paramtv, &RDB_FLOAT, &subtract_float, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("*", 2, paramtv, &RDB_INTEGER, &multiply_int, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("*", 2, paramtv, &RDB_FLOAT, &multiply_float, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    ret = RDB_put_global_ro_op("/", 2, paramtv, &RDB_INTEGER, &divide_int, ecp);
    if (ret != RDB_OK) {
        return RDB_ERROR;
    }

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("/", 2, paramtv, &RDB_FLOAT, &divide_float, ecp);
    if (ret != RDB_OK)
        return ret;

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

    ret = RDB_put_global_ro_op("join", -1, NULL, NULL, &op_rel_binop, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("minus", -1, NULL, NULL, &op_rel_binop, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("semiminus", -1, NULL, NULL, &op_rel_binop, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("intersect", -1, NULL, NULL, &op_rel_binop, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("semijoin", -1, NULL, NULL, &op_rel_binop, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("union", -1, NULL, NULL, &op_union, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("d_union", -1, NULL, NULL, &op_d_union, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("wrap", -1, NULL, NULL, &op_wrap, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("unwrap", -1, NULL, NULL, &op_unwrap, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("union", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("minus", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("semiminus", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    ret = RDB_put_global_ro_op("intersect", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("semijoin", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("divide", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("tclose", -1, NULL, NULL, &op_tclose, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("group", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("ungroup", -1, NULL, NULL, &op_vtable_wrapfn, ecp);
    if (ret != RDB_OK)
        return ret;

    if (RDB_put_global_ro_op("[]", -1, NULL, NULL, &op_subscript, ecp)
            != RDB_OK) {
        return RDB_ERROR;
    }

    if (RDB_put_global_ro_op("length", -1, NULL, NULL, &length_array, ecp)
            != RDB_OK) {
        return RDB_ERROR;
    }

    paramtv[0] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("sqrt", 1, paramtv, &RDB_FLOAT, &math_sqrt, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("sin", 1, paramtv, &RDB_FLOAT, &math_sin, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("cos", 1, paramtv, &RDB_FLOAT, &math_cos, ecp);
    if (ret != RDB_OK)
        return ret;

    ret = RDB_put_global_ro_op("atan", 1, paramtv, &RDB_FLOAT, &math_atan, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[1] = &RDB_FLOAT;

    ret = RDB_put_global_ro_op("atan2", 2, paramtv, &RDB_FLOAT, &math_atan2, ecp);
    if (ret != RDB_OK)
        return ret;

    paramtv[0] = &RDB_STRING;

    ret = RDB_put_global_ro_op("os.getenv", 1, paramtv, &RDB_STRING, &op_getenv, ecp);
    if (ret != RDB_OK)
        return ret;

    return RDB_OK;
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
