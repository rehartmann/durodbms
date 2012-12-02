/*
 * $Id$
 *
 * Copyright (C) 2004-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "tostr.h"
#include "internal.h"
#include "stable.h"
#include <gen/hashtabit.h>
#include <string.h>
#include <errno.h>

static int
append_obj(RDB_object *objp, const RDB_object *srcp, RDB_environment *,
        RDB_exec_context *, RDB_transaction *);

static int
append_table_def(RDB_object *, const RDB_object *, RDB_environment *,
        RDB_exec_context *, RDB_transaction *, int options);

static int
append_tuple(RDB_object *objp, const RDB_object *tplp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_hashtable_iter hiter;
    tuple_entry *entryp;
    RDB_bool start = RDB_TRUE;

    if (RDB_append_string(objp, "TUPLE { ", ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    /* Tuple can be empty */
    if (tplp->kind != RDB_OB_INITIAL) {
        RDB_init_hashtable_iter(&hiter, (RDB_hashtable *) &tplp->val.tpl_tab);
        for (;;) {
            /* Get next attribute */
            entryp = RDB_hashtable_next(&hiter);
            if (entryp == NULL)
                break;
    
            if (start) {
                start = RDB_FALSE;
            } else {
                ret = RDB_append_string(objp, ", ", ecp);
                if (ret != RDB_OK) {
                    goto error;
                }
            }
    
            ret = RDB_append_string(objp, entryp->key, ecp);
            if (ret != RDB_OK) {
                goto error;
            }
    
            ret = RDB_append_string(objp, " ", ecp);
            if (ret != RDB_OK) {
                goto error;
            }

            ret = append_obj(objp, &entryp->obj, envp, ecp, txp);
            if (ret != RDB_OK)
                goto error;
        }
    }
    ret = RDB_append_string(objp, "}", ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    RDB_destroy_hashtable_iter(&hiter);
    return RDB_OK;

error:
    if (objp->kind != RDB_OB_INITIAL)
        RDB_destroy_hashtable_iter(&hiter);
    return RDB_ERROR;
}

static int
append_quoted_string(RDB_object *objp, const RDB_object *strp,
        RDB_exec_context *ecp)
{
    int ret;
    int i;
    size_t qlen;
    char *qstr = RDB_alloc((strp->val.bin.len + 2) * 2, ecp);

    if (qstr == NULL)
        return RDB_ERROR;

    qstr[0] = '\'';
    qlen = 1;
    for (i = 0; i < strp->val.bin.len - 1; i++) {
        switch (((char *)strp->val.bin.datap)[i]) {
            case '\"':
                qstr[qlen++] = '\\';
                qstr[qlen++] = '\"';
                break;
            case '\\':
                qstr[qlen++] = '\\';
                qstr[qlen++] = '\\';
                break;
            case '\n':
                qstr[qlen++] = '\\';
                qstr[qlen++] = 'n';
                break;
            case '\r':
                qstr[qlen++] = '\\';
                qstr[qlen++] = 'r';
                break;
            case '\t':
                qstr[qlen++] = '\\';
                qstr[qlen++] = 't';
                break;
            default:
                qstr[qlen++] = ((char *)strp->val.bin.datap)[i];
        }
    }
    qstr[qlen++] = '\'';
    qstr[qlen] = '\0';

    ret = RDB_append_string(objp, qstr, ecp);
    RDB_free(qstr);
    return ret;
}

/*
 * Generate selector invocation and append it to *objp.
 * *txp is passed to RDB_obj_comp (may be needed to read the getter operator
 * from the catalog)
 */
static int
append_utype_obj(RDB_object *objp, const RDB_object *srcp,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int compc;
    int i;
    int ret;
    RDB_possrep *possrep = &srcp->typ->def.scalar.repv[0]; /* Take 1st possrep */

    ret = RDB_append_string(objp, possrep->name, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;
    ret = RDB_append_string(objp, "(", ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    compc = possrep->compc;
    for (i = 0; i < compc; i++) {
        RDB_object compobj;

        if (i > 0) {
            ret = RDB_append_string(objp, ", ", ecp);
            if (ret != RDB_OK)
                return RDB_ERROR;
        }

        RDB_init_obj(&compobj);
        ret = RDB_obj_comp(srcp, possrep->compv[i].name, &compobj, envp,
                ecp, txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&compobj, ecp);
            return ret;
        }
        ret = append_obj(objp, &compobj, envp, ecp, txp);
        RDB_destroy_obj(&compobj, ecp);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_append_string(objp, ")", ecp);
}

static int
append_table_val(RDB_object *objp, const RDB_object *tbp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_object arr;
    RDB_object *tplp;

    if (RDB_append_string(objp, "RELATION { ", ecp) != RDB_OK)
        return RDB_ERROR;

    RDB_init_obj(&arr);
    if (RDB_table_to_array(&arr, (RDB_object *) tbp, 0, NULL, 0, ecp, txp)
            != RDB_OK) {
        return RDB_ERROR;
    }

    for (i = 0;
            (tplp = RDB_array_get(&arr, (RDB_int) i, ecp)) != NULL;
            i++) {
        if (i > 0) {
            if (RDB_append_string(objp, ", ", ecp) != RDB_OK) {
                RDB_destroy_obj(&arr, ecp);
                return RDB_ERROR;
            }
        }

        if (append_tuple(objp, tplp, envp, ecp, txp) != RDB_OK) {
            RDB_destroy_obj(&arr, ecp);
            return RDB_ERROR;
        }
    }
    RDB_destroy_obj(&arr, ecp);
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        return RDB_ERROR;
    RDB_clear_err(ecp);
    return RDB_append_string(objp, "}", ecp);
}

static int
append_array(RDB_object *objp, const RDB_object *arrp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_object *elemp;

    if (RDB_append_string(objp, "ARRAY (", ecp) != RDB_OK)
        return RDB_ERROR;

    for (i = 0;
            (elemp = RDB_array_get((RDB_object *) arrp, (RDB_int) i, ecp)) != NULL;
            i++) {
        if (i > 0) {
            if (RDB_append_string(objp, ", ", ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }

        if (append_obj(objp, elemp, envp, ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        return RDB_ERROR;
    RDB_clear_err(ecp);
    return RDB_append_string(objp, ")", ecp);
}

static int
append_obj(RDB_object *objp, const RDB_object *srcp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_type *typ = RDB_obj_type(srcp);

    if (typ != NULL) {
        if (RDB_type_is_scalar(typ)) {
            if (srcp->typ->def.scalar.repc > 0) {
                 /* Type with possreps - generate selector */
                 ret = append_utype_obj(objp, srcp, envp, ecp, txp);
             } else if (srcp->typ == &RDB_STRING) {
                 /* String */
                 ret = append_quoted_string(objp, srcp, ecp);
             } else {
                 /* Other type - convert to string */
                 RDB_object dst;

                 RDB_init_obj(&dst);
                 ret = RDB_obj_to_string(&dst, srcp, ecp);
                 if (ret != RDB_OK) {
                     RDB_destroy_obj(&dst, ecp);
                     return ret;
                 }
                 ret = RDB_append_string(objp, dst.val.bin.datap, ecp);
                 RDB_destroy_obj(&dst, ecp);
             }
             return ret;
        } else if (RDB_type_is_relation(typ)) {
            return append_table_val(objp, srcp, envp, ecp, txp);
        } else if (RDB_type_is_array(typ)
                || srcp->kind == RDB_OB_ARRAY) {
            return append_array(objp, srcp, envp, ecp, txp);
        }
    }
    if (srcp->kind == RDB_OB_TUPLE) {
        return append_tuple(objp, srcp, envp, ecp, txp);
    }
    if (srcp->kind == RDB_OB_INITIAL) {
        /* Treat as empty tuple */
        return RDB_append_string(objp, "TUPLE { }", ecp);
    }
    RDB_raise_invalid_argument("unable to convert RDB_object to string", ecp);
    return RDB_ERROR;
} /* append_obj */

static int
append_ex(RDB_object *, const RDB_expression *, RDB_environment *,
        RDB_exec_context *, RDB_transaction *, int);

static int
append_infix_binary_ex(RDB_object *objp, const RDB_expression *exp,
        RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp,
        int options)
{
    /* Argument #1 */
    if (exp->def.op.args.firstp->kind != RDB_EX_VAR
            && exp->def.op.args.firstp->kind != RDB_EX_OBJ) {
        if (RDB_append_string(objp, "(", ecp) != RDB_OK)
            return RDB_ERROR;
    }
    if (append_ex(objp, exp->def.op.args.firstp, envp, ecp, txp,
            options) != RDB_OK)
        return RDB_ERROR;
    if (exp->def.op.args.firstp->kind != RDB_EX_VAR
            && exp->def.op.args.firstp->kind != RDB_EX_OBJ) {
        if (RDB_append_string(objp, ")", ecp) != RDB_OK)
            return RDB_ERROR;
    }

    if (RDB_append_string(objp, " ", ecp) != RDB_OK)
        return RDB_ERROR;

    /* Operator name */
    if (RDB_append_string(objp, exp->def.op.name, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_append_string(objp, " ", ecp) != RDB_OK)
        return RDB_ERROR;

    /* Argument #2 */
    if (exp->def.op.args.firstp->nextp->kind != RDB_EX_VAR
            && exp->def.op.args.firstp->nextp->kind != RDB_EX_OBJ) {
        if (RDB_append_string(objp, "(", ecp) != RDB_OK)
            return RDB_ERROR;
    }
    if (append_ex(objp, exp->def.op.args.firstp->nextp, envp, ecp,
            txp, options) != RDB_OK)
        return RDB_ERROR;
    if (exp->def.op.args.firstp->nextp->kind != RDB_EX_VAR
            && exp->def.op.args.firstp->nextp->kind != RDB_EX_OBJ) {
        if (RDB_append_string(objp, ")", ecp) != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

static int
append_ro_op_ex(RDB_object *objp, const RDB_expression *exp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp, int options)
{
    RDB_expression *argp;

    if (strcmp(exp->def.op.name, "=") == 0
            || strcmp(exp->def.op.name, "<>") == 0
            || strcmp(exp->def.op.name, "<") == 0
            || strcmp(exp->def.op.name, ">") == 0
            || strcmp(exp->def.op.name, "<=") == 0
            || strcmp(exp->def.op.name, ">=") == 0
            || strcmp(exp->def.op.name, "+") == 0
            || strcmp(exp->def.op.name, "-") == 0
            || strcmp(exp->def.op.name, "||") == 0
            || strcmp(exp->def.op.name, "like") == 0
            || strcmp(exp->def.op.name, "regex_like") == 0
            || strcmp(exp->def.op.name, "and") == 0
            || strcmp(exp->def.op.name, "or") == 0
            || strcmp(exp->def.op.name, "where") == 0
            || strcmp(exp->def.op.name, "join") == 0
            || strcmp(exp->def.op.name, "semijoin") == 0
            || strcmp(exp->def.op.name, "union") == 0
            || strcmp(exp->def.op.name, "intersect") == 0
            || strcmp(exp->def.op.name, "minus") == 0
            || strcmp(exp->def.op.name, "semiminus") == 0
            || strcmp(exp->def.op.name, "in") == 0
            || strcmp(exp->def.op.name, "subset_of") == 0) {
        if (append_infix_binary_ex(objp, exp, envp, ecp, txp, options)
                != RDB_OK) {
            return RDB_ERROR;
        }
    } else if (strcmp(exp->def.op.name, "project") == 0) {
        argp = exp->def.op.args.firstp;
        if (argp->kind != RDB_EX_VAR
                && argp->kind != RDB_EX_OBJ
                && argp->kind != RDB_EX_TBP) {
            if (RDB_append_string(objp, "(", ecp) != RDB_OK)
                return RDB_ERROR;
        }
        if (append_ex(objp, argp, envp, ecp, txp, options) != RDB_OK)
            return RDB_ERROR;
        if (argp->kind != RDB_EX_VAR
                && argp->kind != RDB_EX_OBJ
                && argp->kind != RDB_EX_TBP) {
            if (RDB_append_string(objp, ")", ecp) != RDB_OK)
                return RDB_ERROR;
        }
        if (RDB_append_string(objp, " { ", ecp) != RDB_OK)
            return RDB_ERROR;
        argp = argp->nextp;
        while (argp != NULL) {
            if (RDB_append_string(objp, RDB_obj_string(&argp->def.obj),
                    ecp) != RDB_OK) {
                return RDB_ERROR;
            }
            argp = argp->nextp;
            if (argp != NULL) {
                if (RDB_append_string(objp, ", ", ecp) != RDB_OK)
                    return RDB_ERROR;
            }
        }
        if (RDB_append_string(objp, " }", ecp) != RDB_OK)
            return RDB_ERROR;
    } else {
        if (RDB_append_string(objp, exp->def.op.name, ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_append_string(objp, "(", ecp) != RDB_OK)
            return RDB_ERROR;
        argp = exp->def.op.args.firstp;
        while (argp != NULL) {
            if (append_ex(objp, argp, envp, ecp, txp, options) != RDB_OK)
                return RDB_ERROR;
            argp = argp->nextp;
            if (argp != NULL) {
                if (RDB_append_string(objp, ", ", ecp) != RDB_OK)
                    return RDB_ERROR;
            }
        }
        if (RDB_append_string(objp, ")", ecp) != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

static int
append_ex(RDB_object *objp, const RDB_expression *exp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp, int options)
{
    int ret;

    switch (exp->kind) {
        case RDB_EX_OBJ:
            ret = append_obj(objp, &exp->def.obj, envp, ecp, txp);
            if (ret != RDB_OK)
                 return ret;
            break;
        case RDB_EX_TBP:
            ret = append_table_def(objp, exp->def.tbref.tbp, envp,
                    ecp, txp, options);
            if (ret != RDB_OK)
                 return ret;
            if (exp->def.tbref.indexp != NULL && (RDB_SHOW_INDEX & options)) {
                ret = RDB_append_string(objp, " INDEX ", ecp);
                if (ret != RDB_OK)
                    return RDB_ERROR;
                ret = RDB_append_string(objp, exp->def.tbref.indexp->name, ecp);
                if (ret != RDB_OK)
                    return RDB_ERROR;
            }            
            break;            
        case RDB_EX_VAR:
             ret = RDB_append_string(objp, exp->def.varname, ecp);
             if (ret != RDB_OK)
                 return ret;
            break;
        case RDB_EX_TUPLE_ATTR:
            ret = RDB_append_string(objp, "(", ecp);
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->def.op.args.firstp, envp,
                    ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, ").", ecp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, exp->def.op.name, ecp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, ")", ecp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_GET_COMP:
            ret = RDB_append_string(objp, "the_", ecp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, exp->def.op.name, ecp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, "(", ecp);
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->def.op.args.firstp, envp,
                    ecp, txp, options);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_append_string(objp, ")", ecp);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_RO_OP:
            if (append_ro_op_ex(objp, exp, envp, ecp, txp, options) != RDB_OK)
                return RDB_ERROR;
            break;
    }
    return RDB_OK;
}

/*
 * Append the table definition to *objp.
 * If the table has a name, append the name.
 * Otherwise, if it's not a virtual table, append the value.
 * Otherwise, append the defining expression.
 */
static int
append_table_def(RDB_object *objp, const RDB_object *tbp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp, int options)
{
    RDB_expression *exp;
    const char *tbname = RDB_table_name(tbp);
    if (tbname != NULL) {
        return RDB_append_string(objp, tbname, ecp);
    }
    exp = RDB_vtable_expr(tbp);
    if (exp == NULL) {
        return append_table_val(objp, tbp, envp, ecp, txp);
    }
    if (RDB_append_string(objp, "(", ecp) != RDB_OK)
        return RDB_ERROR;

    if (append_ex(objp, exp, envp, ecp, txp, options) != RDB_OK)
        return RDB_ERROR;
    return RDB_append_string(objp, ")", ecp);
}

int
RDB_obj_to_str(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (RDB_string_to_obj(dstp, "", ecp) != RDB_OK)
        return RDB_ERROR;

    return append_obj(dstp, srcp, NULL, ecp, txp);
}

int
RDB_table_def_to_str(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *ecp, RDB_transaction *txp, int options)
{
    if (RDB_string_to_obj(dstp, "", ecp) != RDB_OK)
        return RDB_ERROR;

    return append_table_def(dstp, srcp, NULL, ecp, txp, options);
}

/*
 * Convert the expresssion *exp to its string representation and store it in *dstp.
 */
int
RDB_expr_to_str(RDB_object *dstp, const RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp, int options)
{
    if (RDB_string_to_obj(dstp, "", ecp) != RDB_OK)
        return RDB_ERROR;

    return append_ex(dstp, exp, NULL, ecp, txp, options);
}

int
RDB_print_expr(const RDB_expression *exp, FILE *fp, RDB_exec_context *ecp)
{
	RDB_object dst;
	int ret;

	RDB_init_obj(&dst);
    if (RDB_expr_to_str(&dst, exp, ecp, NULL, 0) != RDB_OK) {
        RDB_destroy_obj(&dst, ecp);
        return RDB_ERROR;
    }
    ret = fputs(RDB_obj_string(&dst), fp);
    RDB_destroy_obj(&dst, ecp);
    if (ret == EOF) {
        RDB_raise_system(strerror(errno), ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}
