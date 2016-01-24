/*
 * Scalar built-in read-only operators.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "builtinscops.h"
#include "builtintypes.h"
#include "objinternal.h"

#include <string.h>
#include <math.h>
#include <regex.h>
#include <ctype.h>
#ifdef _WIN32
#include "Shlwapi.h"
#else
#include <fnmatch.h>
#endif

#include <stdio.h>
#include <errno.h>

#ifdef _WIN32
#define isfinite _finite
#endif

/**

@page stringops Built-in string operators

<h3 id="op_concat">OPERATOR ||</h3>

OPERATOR || (s1 string, s2 string) RETURNS string;

<h4>Description</h4>

The string concatenation operator.

<h4>Return value</h4>

The result of the concatenation of the operands.

<hr>

<h3 id="op_strlen">OPERATOR strlen</h3>

OPERATOR strlen (s string) RETURNS integer;

<h4>Description</h4>

The string length operator.

<h4>Return value</h4>

The length of @a s, in code points.

<hr>

<h3 id="op_strlen_b">OPERATOR strlen_b</h3>

OPERATOR strlen_b (s string) RETURNS integer;

<h4>Description</h4>

The string length operator, returning the number of bytes.

<h4>Return value</h4>

The length of @a s, in bytes.

<hr>

<h3 id="substr">OPERATOR substr</h3>

OPERATOR substr(s string, start integer, length integer) RETURNS
string;

<h4>Description</h4>

Extracts a substring.

<h4>Return value</h4>

The substring of @a s with length @a length starting at position
@a start. @a length and @a start are measured
in code points, according to the current encoding.

<h4>Errors</h4>

<dl>
<dt>invalid_argument_error
<dd>@a start is negative, or @a start + @a length
is greater than strlen(@a s).
</dl>

<hr>

<h3 id="substr_b">OPERATOR substr_b</h3>

OPERATOR substr_b(s string, start integer, length integer) RETURNS
string;

OPERATOR substr_b(s string, start integer) RETURNS
string;

<h4>Description</h4>

Extracts a substring.

<h4>Return value</h4>

The substring of @a s with length @a length starting at position
@a start. @a length and @a start are measured
in bytes. If called with 2 arguments, the substring extends to the end of
@a s.

<h4>Errors</h4>

<dl>
<dt>invalid_argument_error
<dd>@a start or @a length are negative, or @a start + @a length
is greater than strlen(@a s).
</dl>

<hr>

<h3 id="strfind_b">OPERATOR strfind_b</h3>

OPERATOR strfind_b (haystack string, needle string) RETURNS
integer;

OPERATOR strfind_b (haystack string, needle string, int pos) RETURNS
integer;

<h4>Description</h4>

Finds the first occurrence of the string @a needle in the string @a haystack.
If called with 3 arguments, it finds the first occurrence after @a pos,
where @a pos is a byte offset.

<h4>Return value</h4>

The position of the substring, in bytes, or -1 if the substring has not been found.

<hr>

<h3 id="op_starts_with">OPERATOR starts_with</h3>

OPERATOR starts_with (s string, prefix string) RETURNS boolean;

<h4>Description</h4>

Tests if string @a s starts with string @a prefix.

<h4>Return value</h4>

TRUE if @a s starts with @a prefix, FALSE otherwise

<hr>

<h3 id="op_like">OPERATOR like</h3>

OPERATOR like (s string, pattern string) RETURNS boolean;

<h4>Description</h4>

Pattern matching operator. A period ('.') matches a single character;
an asterisk ('*') matches zero or more characters.

<h4>Return value</h4>

TRUE if @a s matches @a pattern, RDB_FALSE otherwise.

<hr>

<h3 id="op_regex_like">OPERATOR regex_like</h3>

OPERATOR regex_like (s string, pattern string) RETURNS boolean;

<h4>Description</h4>

The regular expression matching operator.

<h4>Return value</h4>

TRUE if @a s matches @a pattern, RDB_FALSE otherwise.

<hr>

<h3 id="op_format">OPERATOR format</h3>

OPERATOR format (format string, ...) RETURNS string;

<h4>Description</h4>

Generates a formatted string in the style of sprintf.
The arguments passed after format must be of type string, integer, or float
and must match the format argument.

<h4>Return value</h4>

The formatted string.

@page arithmetic Built-in arithmetic operators

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

<h3 id="op_sqrt">OPERATOR sqrt</h3>

OPERATOR sqrt(x float) RETURNS float;

The square root operator.

<hr>

<h3 id="op_abs">OPERATOR abs</h3>

OPERATOR abs(x integer) RETURNS integer;

OPERATOR abs(x float) RETURNS float;

The abs(absolute value) operator.

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

<hr>

<h3 id="op_power">OPERATOR power</h3>

OPERATOR power(b float, x float) RETURNS float;

The power operator.

<hr>

<h3 id="op_exp">OPERATOR exp</h3>

OPERATOR exp(x float) RETURNS float;

The exponential function operator.

<hr>

<h3 id="op_ln">OPERATOR </h3>

OPERATOR ln(x float) RETURNS float;

The natural logarithm operator.

<hr>

<h3 id="op_log">OPERATOR </h3>

OPERATOR log(x float) RETURNS float;

The base 10 logarithm operator.

*/

int
RDB_eq_bool(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->val.bool_val == argv[1]->val.bool_val));
    return RDB_OK;
}

int
RDB_eq_binary(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[0]->val.bin.len != argv[1]->val.bin.len)
        RDB_bool_to_obj(retvalp, RDB_FALSE);
    else if (argv[0]->val.bin.len == 0)
        RDB_bool_to_obj(retvalp, RDB_TRUE);
    else
        RDB_bool_to_obj(retvalp, (RDB_bool) (memcmp(argv[0]->val.bin.datap,
            argv[1]->val.bin.datap, argv[0]->val.bin.len) == 0));
    return RDB_OK;
}

static int
cast_as_integer_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[0]->val.float_val > RDB_INT_MAX
            || argv[0]->val.float_val < RDB_INT_MIN) {
        RDB_raise_type_constraint_violation("floating point overflow", ecp);
        return RDB_ERROR;
    }
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->val.float_val);
    return RDB_OK;
}

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
cast_as_integer_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;
    long lv = strtol(RDB_obj_string(argv[0]), &endp, 10);
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to integer failed", ecp);
        return RDB_ERROR;
    }
    if (lv > RDB_INT_MAX || (errno == ERANGE && lv == LONG_MAX)) {
        RDB_raise_type_constraint_violation("integer number too large", ecp);
        return RDB_ERROR;
    }

    RDB_int_to_obj(retvalp, (RDB_int) lv);
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
    RDB_float fv = (RDB_float) strtod(argv[0]->val.bin.datap, &endp);

    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to float failed", ecp);
        return RDB_ERROR;
    }
    if (!isfinite(fv)) {
        RDB_raise_type_constraint_violation("floating point overflow", ecp);
        return RDB_ERROR;
    }

    RDB_float_to_obj(retvalp, fv);
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
op_strlen(int argc, RDB_object *argv[], RDB_operator *op,
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
op_strlen_b(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) strlen(argv[0]->val.bin.datap));
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
op_substr_b_remaining(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    const char *strp = RDB_obj_string(argv[0]);
    int pos = (int) RDB_obj_int(argv[1]);
    if (pos < 0 || pos > strlen(strp)) {
        RDB_raise_invalid_argument("invalid substr_b argument", ecp);
        return RDB_ERROR;
    }

    return RDB_string_to_obj(retvalp, strp + pos, ecp);
}

static int
op_substr_b(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    const char *strp = RDB_obj_string(argv[0]);
    int pos = (int) RDB_obj_int(argv[1]);
    RDB_int len = RDB_obj_int(argv[2]);

    if (pos < 0 || len < 0 || pos + len > strlen(strp) ) {
        RDB_raise_invalid_argument("invalid substr_b argument", ecp);
        return RDB_ERROR;
    }

    return RDB_string_n_to_obj(retvalp, strp + pos, (size_t) len, ecp);
}

static int
op_strfind_b(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    const char *haystack = RDB_obj_string(argv[0]);

    char *substrp = strstr(haystack, RDB_obj_string(argv[1]));

    if (substrp == NULL) {
        RDB_int_to_obj(retvalp, (RDB_int) -1);
    } else {
        RDB_int_to_obj(retvalp, (RDB_int) (substrp - haystack));
    }
    return RDB_OK;
}

static int
op_strfind_b_pos(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *substrp;
    const char *haystack = RDB_obj_string(argv[0]);
    int pos = (int) RDB_obj_int(argv[2]);

    if (pos < 0 || pos > strlen(haystack)) {
        RDB_raise_invalid_argument("Invalid strfind_b argument", ecp);
        return RDB_ERROR;
    }

    substrp = strstr(haystack + pos, RDB_obj_string(argv[1]));

    if (substrp == NULL) {
        RDB_int_to_obj(retvalp, (RDB_int) -1);
    } else {
        RDB_int_to_obj(retvalp, (RDB_int) (substrp - haystack));
    }
    return RDB_OK;
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
sprintf_obj(char **buf, int *bufsizep, const char *format,
        const RDB_object *srcobjp, RDB_exec_context *ecp)
{
    int reqsize;
    RDB_type *typ = RDB_obj_type(srcobjp);
    const char *convp = format + 1;

    /* Skip flags etc. */
    while (*convp == '#' || *convp == '0' || *convp == '-'
            || *convp == ' ' || *convp == '+') {
        ++convp;
    }
    while (isdigit(*convp)) ++convp;
    if (*convp == '.') {
        ++convp;
        while (isdigit(*convp)) ++convp;
    }
    switch (*convp) {
    case 'd':
    case 'o':
    case 'u':
    case 'x':
    case 'X':
        if (typ != &RDB_INTEGER) {
            RDB_raise_type_mismatch("integer expected", ecp);
            return RDB_ERROR;
        }
        if (*convp == 'd') {
#ifdef _WIN32
            reqsize = _scprintf(format, (int) RDB_obj_int(srcobjp));
#else
            reqsize = snprintf(NULL, 0, format, (int) RDB_obj_int(srcobjp));
#endif
        } else {
#ifdef _WIN32
            reqsize = _scprintf(format, (unsigned int) RDB_obj_int(srcobjp));
#else
            reqsize = snprintf(NULL, 0, format, (unsigned int) RDB_obj_int(srcobjp));
#endif
        }
        if (reqsize < 0) {
            RDB_raise_invalid_argument("", ecp);
            return RDB_ERROR;
        }
        ++reqsize; /* For the terminating null character */
        if (*bufsizep < reqsize) {
            *buf = RDB_realloc(*buf, reqsize , ecp);
            if (*buf == NULL)
                return RDB_ERROR;
            *bufsizep = reqsize;
        }
        if (*convp == 'd') {
            sprintf(*buf, format, (int) RDB_obj_int(srcobjp));
        } else {
            sprintf(*buf, format, (unsigned int) RDB_obj_int(srcobjp));
        }
        break;
    case 's':
        if (typ != &RDB_STRING) {
            RDB_raise_type_mismatch("string expected", ecp);
            return RDB_ERROR;
        }
#ifdef _WIN32
        reqsize = _scprintf(format, RDB_obj_string(srcobjp));
#else
        reqsize = snprintf(NULL, 0, format, RDB_obj_string(srcobjp));
#endif
        if (reqsize < 0) {
            RDB_raise_invalid_argument("", ecp);
            return RDB_ERROR;
        }
        ++reqsize;
        if (*bufsizep < reqsize) {
            *buf = RDB_realloc(*buf, reqsize, ecp);
            if (*buf == NULL)
                return RDB_ERROR;
            *bufsizep = reqsize;
        }
        sprintf(*buf, format, RDB_obj_string(srcobjp));
        break;
    case 'e':
    case 'f':
    case 'g':
        if (typ != &RDB_FLOAT) {
            RDB_raise_type_mismatch("float expected", ecp);
            return RDB_ERROR;
        }
#ifdef _WIN32
        reqsize = _scprintf(format, (double) RDB_obj_float(srcobjp));
#else
        reqsize = snprintf(NULL, 0, format, (double) RDB_obj_float(srcobjp));
#endif
        if (reqsize < 0) {
            RDB_raise_invalid_argument("", ecp);
            return RDB_ERROR;
        }
        ++reqsize;
        if (*bufsizep < reqsize) {
            *buf = RDB_realloc(*buf, reqsize, ecp);
            if (*buf == NULL)
                return RDB_ERROR;
            *bufsizep = reqsize;
        }
        sprintf(*buf, format, (double) RDB_obj_float(srcobjp));
        break;
    default:
        RDB_raise_type_mismatch("Unsupported conversion", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_format(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *format;
    char *pos, *npos;
    int argi;
    int bufsize = 160;
    char *buf = RDB_alloc(bufsize, ecp);
    if (buf == NULL)
        return RDB_ERROR;

    if (argc < 1) {
        RDB_raise_invalid_argument("format argument missing", ecp);
        goto error;
    }

    if (RDB_obj_type(argv[0]) != &RDB_STRING) {
        RDB_raise_type_mismatch("format must be of type string", ecp);
        goto error;
    }
    format = RDB_obj_string(argv[0]);

    pos = strchr(format, '%');
    if (pos == NULL) {
        RDB_free(buf);
        return RDB_string_to_obj(retvalp, format, ecp);
    }
    if (RDB_string_n_to_obj(retvalp, format, pos - format, ecp) != RDB_OK)
        goto error;

    argi = 1;
    for (;;) {
        char *tmpformat;
        int ret;

        while (pos[1] == '%') {
            npos = strchr(pos + 2, '%');
            if (npos == NULL) {
                RDB_free(buf);
                return RDB_append_string(retvalp, pos + 1, ecp);
            }
            for (pos += 1; pos < npos; pos++) {
                if (RDB_append_char(retvalp, *pos, ecp) != RDB_OK)
                    goto error;
            }
            pos = npos;
        }
        if (argi >= argc) {
            RDB_raise_invalid_argument("missing argument", ecp);
            goto error;
        }
        npos = strchr(pos + 1, '%');
        if (npos == NULL) {
            if (sprintf_obj(&buf, &bufsize, pos, argv[argi], ecp) != RDB_OK)
                goto error;

            if (RDB_append_string(retvalp, buf, ecp) != RDB_OK)
                goto error;
            RDB_free(buf);
            return RDB_OK;
        }
        tmpformat = RDB_alloc(npos - pos + 1, ecp);
        if (tmpformat == NULL)
            goto error;
        strncpy(tmpformat, pos, npos - pos);
        tmpformat[npos - pos] = '\0';
        ret = sprintf_obj(&buf, &bufsize, tmpformat, argv[argi], ecp);
        RDB_free(tmpformat);
        if (ret != RDB_OK)
            goto error;
        if (RDB_append_string(retvalp, buf, ecp) != RDB_OK)
            goto error;

        pos = npos;
        argi++;
    }

error:
    RDB_free(buf);
    return RDB_ERROR;
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
    if (argv[0]->val.int_val == RDB_INT_MIN) {
        RDB_raise_type_constraint_violation("integer overflow", ecp);
        return RDB_ERROR;
    }
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
    if (argv[1]->val.int_val > 0) {
        if (argv[0]->val.int_val > RDB_INT_MAX - argv[1]->val.int_val) {
            RDB_raise_type_constraint_violation("integer overflow", ecp);
            return RDB_ERROR;
        }
    } else {
        if (argv[0]->val.int_val < RDB_INT_MIN - argv[1]->val.int_val) {
            RDB_raise_type_constraint_violation("integer overflow", ecp);
            return RDB_ERROR;
        }
    }

    RDB_int_to_obj(retvalp, argv[0]->val.int_val + argv[1]->val.int_val);
    return RDB_OK;
}

static int
add_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float fv = argv[0]->val.float_val + argv[1]->val.float_val;
    if (!isfinite(fv)) {
        RDB_raise_type_constraint_violation("floating point overflow", ecp);
        return RDB_ERROR;
    }

    RDB_float_to_obj(retvalp, fv);
    return RDB_OK;
}

static int
subtract_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->val.int_val > 0) {
        if (argv[0]->val.int_val < RDB_INT_MIN + argv[1]->val.int_val) {
            RDB_raise_type_constraint_violation("integer overflow", ecp);
            return RDB_ERROR;
        }
    } else {
        if (argv[0]->val.int_val > RDB_INT_MAX + argv[1]->val.int_val) {
            RDB_raise_type_constraint_violation("integer overflow", ecp);
            return RDB_ERROR;
        }
    }

    RDB_int_to_obj(retvalp, argv[0]->val.int_val - argv[1]->val.int_val);
    return RDB_OK;
}

static int
subtract_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float fv = argv[0]->val.float_val - argv[1]->val.float_val;
    if (!isfinite(fv)) {
        RDB_raise_type_constraint_violation("floating point overflow", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp, fv);
    return RDB_OK;
}

static int
multiply_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    long long prod = (long long) argv[0]->val.int_val * argv[1]->val.int_val;
    if (prod > RDB_INT_MAX || prod < RDB_INT_MIN) {
        RDB_raise_type_constraint_violation("integer overflow", ecp);
        return RDB_ERROR;
    }
    RDB_int_to_obj(retvalp, (RDB_int) prod);
    return RDB_OK;
}

static int
multiply_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float fv = argv[0]->val.float_val * argv[1]->val.float_val;
    if (!isfinite(fv)) {
        RDB_raise_type_constraint_violation("floating point overflow", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp, fv);
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
    double q = argv[0]->val.float_val / argv[1]->val.float_val;
    if (!isfinite(q)) {
        RDB_raise_type_constraint_violation("floating point overflow", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp, (RDB_float) q);
    return RDB_OK;
}

static int
abs_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp,
            (RDB_int) abs((int) RDB_obj_int(argv[0])));
    return RDB_OK;
}

static int
abs_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            (RDB_float) fabs((double) RDB_obj_float(argv[0])));
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
math_power(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    double p = pow((double) RDB_obj_float(argv[0]),
            (double) RDB_obj_float(argv[1]));
    if (!isfinite(p)) {
        RDB_raise_invalid_argument("pow()", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp, (RDB_float) p);
    return RDB_OK;
}

static int
math_exp(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) exp((double) RDB_obj_float(argv[0])));
    return RDB_OK;
}

static int
math_ln(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    double p = log((double) RDB_obj_float(argv[0]));
    if (!isfinite(p)) {
        RDB_raise_invalid_argument("ln()", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp, (RDB_float) p);
    return RDB_OK;
}

static int
math_log(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    double p = log10((double) RDB_obj_float(argv[0]));
    if (!isfinite(p)) {
        RDB_raise_invalid_argument("log()", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp, (RDB_float) p);
    return RDB_OK;
}

int
RDB_add_builtin_scalar_ro_ops(RDB_op_map *opmap, RDB_exec_context *ecp)
{
    RDB_type *paramtv[6];

    paramtv[0] = &RDB_FLOAT;
    if (RDB_put_ro_op(opmap, "cast_as_integer", 1, paramtv, &RDB_INTEGER,
            &cast_as_integer_float, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_int", 1, paramtv, &RDB_INTEGER,
            &cast_as_integer_float, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "cast_as_integer", 1, paramtv, &RDB_INTEGER, &cast_as_integer_string,
            ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_int", 1, paramtv, &RDB_INTEGER, &cast_as_integer_string,
            ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "cast_as_float", 1, paramtv, &RDB_FLOAT,
            &cast_as_float_int, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_rat", 1, paramtv, &RDB_FLOAT,
            &cast_as_float_int, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_rational", 1, paramtv, &RDB_FLOAT,
            &cast_as_float_int, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "cast_as_float", 1, paramtv, &RDB_FLOAT, &cast_as_float_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_rational", 1, paramtv, &RDB_FLOAT, &cast_as_float_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_rat", 1, paramtv, &RDB_FLOAT, &cast_as_float_string, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "cast_as_string", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_char", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    if (RDB_put_ro_op(opmap, "cast_as_string", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_char", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_BINARY;
    if (RDB_put_ro_op(opmap, "cast_as_string", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_char", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "cast_as_binary", 1, paramtv, &RDB_BINARY, &cast_as_binary, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "strlen", 1, paramtv, &RDB_INTEGER, &op_strlen,
            ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "strlen_b", 1, paramtv, &RDB_INTEGER, &op_strlen_b,
            ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_INTEGER;
    paramtv[2] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "substr", 3, paramtv, &RDB_STRING, &op_substr, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_INTEGER;
    paramtv[2] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "substr_b", 3, paramtv, &RDB_STRING, &op_substr_b, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "substr_b", 2, paramtv, &RDB_STRING,
            &op_substr_b_remaining, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "strfind_b", 2, paramtv, &RDB_INTEGER, &op_strfind_b, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;
    paramtv[2] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "strfind_b", 3, paramtv, &RDB_INTEGER, &op_strfind_b_pos,
            ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "||", 2, paramtv, &RDB_STRING, &op_concat, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_ro_op(opmap, "starts_with", 2, paramtv, &RDB_BOOLEAN, &op_starts_with, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "like", 2, paramtv, &RDB_BOOLEAN, &op_like, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "regex_like", 2, paramtv, &RDB_BOOLEAN,
            &op_regex_like, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "format", -1, NULL, &RDB_STRING,
            &op_format, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_BOOLEAN;
    paramtv[1] = &RDB_BOOLEAN;

    if (RDB_put_ro_op(opmap, "and", 2, paramtv, &RDB_BOOLEAN, &and, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "or", 2, paramtv, &RDB_BOOLEAN, &or, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "xor", 2, paramtv, &RDB_BOOLEAN, &xor, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_BOOLEAN;

    if (RDB_put_ro_op(opmap, "not", 1, paramtv, &RDB_BOOLEAN, &not, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_ro_op(opmap, "<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_DATETIME;
    paramtv[1] = &RDB_DATETIME;

    if (RDB_put_ro_op(opmap, "<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_ro_op(opmap, "<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_DATETIME;
    paramtv[1] = &RDB_DATETIME;

    if (RDB_put_ro_op(opmap, "<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, ">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, ">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, ">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_ro_op(opmap, ">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_DATETIME;
    paramtv[1] = &RDB_DATETIME;

    if (RDB_put_ro_op(opmap, ">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, ">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, ">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, ">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_ro_op(opmap, ">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_DATETIME;
    paramtv[1] = &RDB_DATETIME;

    if (RDB_put_ro_op(opmap, ">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_BOOLEAN;
    paramtv[1] = &RDB_BOOLEAN;

    if (RDB_put_ro_op(opmap, "=", 2, paramtv, &RDB_BOOLEAN, &RDB_eq_bool, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    paramtv[0] = &RDB_BINARY;
    paramtv[1] = &RDB_BINARY;

    if (RDB_put_ro_op(opmap, "=", 2, paramtv, &RDB_BOOLEAN, &RDB_eq_binary, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_BOOLEAN;
    paramtv[1] = &RDB_BOOLEAN;

    if (RDB_put_ro_op(opmap, "<>", 2, paramtv, &RDB_BOOLEAN, &neq_bool, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;


    paramtv[0] = &RDB_BINARY;
    paramtv[1] = &RDB_BINARY;

    if (RDB_put_ro_op(opmap, "<>", 2, paramtv, &RDB_BOOLEAN, &neq_binary, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "-", 1, paramtv, &RDB_INTEGER, &negate_int, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "-", 1, paramtv, &RDB_FLOAT, &negate_float, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "+", 2, paramtv, &RDB_INTEGER, &add_int, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "+", 2, paramtv, &RDB_FLOAT, &add_float, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "-", 2, paramtv, &RDB_INTEGER, &subtract_int, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "-", 2, paramtv, &RDB_FLOAT, &subtract_float, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "*", 2, paramtv, &RDB_INTEGER, &multiply_int, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "*", 2, paramtv, &RDB_FLOAT, &multiply_float, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "/", 2, paramtv, &RDB_INTEGER, &divide_int, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    if (RDB_put_ro_op(opmap, "abs", 1, paramtv, &RDB_INTEGER, &abs_int, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "/", 2, paramtv, &RDB_FLOAT, &divide_float, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "abs", 1, paramtv, &RDB_FLOAT, &abs_float, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "sqrt", 1, paramtv, &RDB_FLOAT, &math_sqrt, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "sin", 1, paramtv, &RDB_FLOAT, &math_sin, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "cos", 1, paramtv, &RDB_FLOAT, &math_cos, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "atan", 1, paramtv, &RDB_FLOAT, &math_atan, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "atan2", 2, paramtv, &RDB_FLOAT, &math_atan2, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "power", 2, paramtv, &RDB_FLOAT, &math_power, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "exp", 1, paramtv, &RDB_FLOAT, &math_exp, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "ln", 1, paramtv, &RDB_FLOAT, &math_ln, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "log", 1, paramtv, &RDB_FLOAT, &math_log, ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}
