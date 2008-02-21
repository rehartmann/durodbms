/*
 * $Id$
 *
 * Copyright (C) 2007-2008 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "stmtser.h"
#include <rel/serialize.h>
#include <rel/internal.h>

static int
serialize_var_def(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    if (_RDB_serialize_str(objp, posp,
            RDB_obj_string(&stmtp->var.vardef.varname), ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_serialize_byte(objp, posp,
            (RDB_byte) (stmtp->var.vardef.type.exp != NULL), ecp) != RDB_OK) {
         return RDB_ERROR;
    }
    if (stmtp->var.vardef.type.exp != NULL) {
        if (_RDB_serialize_expr(objp, posp, stmtp->var.vardef.type.exp, ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
    }
    if (_RDB_serialize_byte(objp, posp,
            (RDB_byte) (stmtp->var.vardef.exp != NULL), ecp) != RDB_OK) {
         return RDB_ERROR;
    }
    if (stmtp->var.vardef.exp != NULL) {
        if (_RDB_serialize_expr(objp, posp, stmtp->var.vardef.exp, ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
serialize_key_def(RDB_object *objp, int *posp, const RDB_parse_keydef *firstkeyp,
        RDB_exec_context *ecp)
{
    RDB_int keyc = 0;
    const RDB_parse_keydef *keyp = firstkeyp;
    do {
        keyc++;
        keyp = keyp->nextp;
    } while (keyp != NULL);

    if (_RDB_serialize_int(objp, posp, keyc, ecp) != RDB_OK) {
         return RDB_ERROR;
    }
    keyp = firstkeyp;
    do {
        RDB_expression *attrexp;
        RDB_int attrc = RDB_expr_list_length(&keyp->attrlist);

        if (_RDB_serialize_int(objp, posp, attrc, ecp) != RDB_OK) {
            return RDB_ERROR;
        }

        attrexp = keyp->attrlist.firstp;
        while (attrexp != NULL) {
            if (_RDB_serialize_expr(objp, posp, attrexp, ecp) != RDB_OK) {
                return RDB_ERROR;
            }
            attrexp = attrexp->nextp;
        }

        keyp = keyp->nextp;
    } while (keyp != NULL);

    return RDB_OK;
}

static int
serialize_var_def_real(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    if (_RDB_serialize_str(objp, posp,
            RDB_obj_string(&stmtp->var.vardef.varname), ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_serialize_byte(objp, posp,
            (RDB_byte) (stmtp->var.vardef.type.exp != NULL), ecp) != RDB_OK) {
         return RDB_ERROR;
    }
    if (stmtp->var.vardef.type.exp != NULL) {
        if (_RDB_serialize_expr(objp, posp, stmtp->var.vardef.type.exp, ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
    }
    if (_RDB_serialize_byte(objp, posp,
            (RDB_byte) (stmtp->var.vardef.exp != NULL), ecp) != RDB_OK) {
         return RDB_ERROR;
    }
    if (stmtp->var.vardef.exp != NULL) {
        if (_RDB_serialize_expr(objp, posp, stmtp->var.vardef.exp, ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
    }

    if (serialize_key_def(objp, posp, stmtp->var.vardef.firstkeyp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }    

    return RDB_OK;
}

static int
serialize_var_def_virtual(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    if (_RDB_serialize_str(objp, posp,
            RDB_obj_string(&stmtp->var.vardef.varname), ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_serialize_expr(objp, posp, stmtp->var.vardef.exp, ecp)
            != RDB_OK) {
        return RDB_ERROR;
    }

    /* No keys currently supported - store 0 for future compatibility */
    if (_RDB_serialize_int(objp, posp, (RDB_int) 0, ecp) != RDB_OK) {
        return RDB_ERROR;
    }    

    return RDB_OK;
}

static int
serialize_var_drop(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    if (_RDB_serialize_str(objp, posp,
            RDB_obj_string(&stmtp->var.vardrop.varname), ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
serialize_stmt_list(RDB_object *, int *, const RDB_parse_statement *, RDB_exec_context *);

static int
serialize_if(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    if (_RDB_serialize_expr(objp, posp, stmtp->var.ifthen.condp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (serialize_stmt_list(objp, posp, stmtp->var.ifthen.ifp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    /* Works for elsep == NULL */
    if (serialize_stmt_list(objp, posp, stmtp->var.ifthen.elsep, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
serialize_for(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    if (_RDB_serialize_expr(objp, posp, stmtp->var.forloop.varexp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_serialize_expr(objp, posp, stmtp->var.forloop.fromp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_serialize_expr(objp, posp, stmtp->var.forloop.top, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (serialize_stmt_list(objp, posp, stmtp->var.forloop.bodyp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
serialize_while(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    if (_RDB_serialize_expr(objp, posp, stmtp->var.whileloop.condp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (serialize_stmt_list(objp, posp, stmtp->var.whileloop.bodyp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
serialize_call(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    RDB_expression *exp;

    if (_RDB_serialize_str(objp, posp,
            RDB_obj_string(&stmtp->var.call.opname), ecp) != RDB_OK)
        return RDB_ERROR;

    if (_RDB_serialize_int(objp, posp,
            RDB_expr_list_length(&stmtp->var.call.arglist), ecp) != RDB_OK)
        return RDB_ERROR;

    exp = stmtp->var.call.arglist.firstp;
    while (exp != NULL) {
        if (_RDB_serialize_expr(objp, posp, exp, ecp) != RDB_OK)
            return RDB_ERROR;
        exp = exp->nextp;
    }
    return RDB_OK;
}

static RDB_int
assign_list_len(RDB_parse_assign *assignp)
{
    RDB_int len = 0;
    while (assignp != NULL) {
        len++;
        assignp = assignp->nextp;
    }
    return len;
}

static int
serialize_assignlist(RDB_object *objp, int *posp,
        const RDB_parse_assign *assignp, RDB_exec_context *ecp);

static int
serialize_assign(RDB_object *objp, int *posp,
        const RDB_parse_assign *assignp, RDB_exec_context *ecp)
{
    RDB_int len;
    RDB_bool hascond;

    if (_RDB_serialize_int(objp, posp, (RDB_int) assignp->kind, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    switch (assignp->kind) {
        case RDB_STMT_COPY:
            if (_RDB_serialize_expr(objp, posp,
                    assignp->var.copy.dstp, ecp) != RDB_OK)
                return RDB_ERROR;
            if (_RDB_serialize_expr(objp, posp,
                    assignp->var.copy.srcp, ecp) != RDB_OK)
                return RDB_ERROR;
            break;
        case RDB_STMT_INSERT:
            if (_RDB_serialize_expr(objp, posp,
                    assignp->var.ins.dstp, ecp) != RDB_OK)
                return RDB_ERROR;
            if (_RDB_serialize_expr(objp, posp,
                    assignp->var.ins.srcp, ecp) != RDB_OK)
                return RDB_ERROR;
            break;
        case RDB_STMT_UPDATE:
            if (_RDB_serialize_expr(objp, posp,
                    assignp->var.upd.dstp, ecp) != RDB_OK)
                return RDB_ERROR;

            hascond = (RDB_bool) (assignp->var.upd.condp != NULL);
            if (_RDB_serialize_byte(objp, posp, (RDB_byte) hascond, ecp) != RDB_OK)
                return RDB_ERROR;

            if (serialize_assignlist(objp, posp, assignp->var.upd.assignlp, ecp)
                    != RDB_OK)
                return RDB_ERROR;

            len = assign_list_len(assignp->var.upd.assignlp);
            if (_RDB_serialize_int(objp, posp, len, ecp) != RDB_OK)
                return RDB_ERROR;
            assignp = assignp->var.upd.assignlp;
            while (assignp != NULL) {
                if (_RDB_serialize_expr(objp, posp, assignp->var.copy.dstp, ecp)
                        != RDB_OK)
                    return RDB_ERROR;
                if (_RDB_serialize_expr(objp, posp, assignp->var.copy.srcp, ecp)
                        != RDB_OK)
                    return RDB_ERROR;
                assignp = assignp->nextp;
            }
            break;
        case RDB_STMT_DELETE:
            if (_RDB_serialize_expr(objp, posp,
                    assignp->var.del.dstp, ecp) != RDB_OK)
                return RDB_ERROR;

            hascond = (RDB_bool) (assignp->var.del.condp != NULL);
            if (_RDB_serialize_byte(objp, posp, (RDB_byte) hascond, ecp) != RDB_OK)
                return RDB_ERROR;                
            if (hascond) {
                if (_RDB_serialize_expr(objp, posp,
                        assignp->var.del.condp, ecp) != RDB_OK)
                    return RDB_ERROR;
            }
            break;
    }
    return RDB_OK;
}

static int
serialize_assignlist(RDB_object *objp, int *posp,
        const RDB_parse_assign *assignp, RDB_exec_context *ecp)
{
    RDB_int ac = RDB_parse_assignlist_length(assignp);

    if (_RDB_serialize_int(objp, posp, ac, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    while (assignp != NULL) {
        if (serialize_assign(objp, posp, assignp, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
        assignp = assignp->nextp;
    }
    return RDB_OK;
}

static int
serialize_return(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    RDB_byte hasexp = (RDB_byte) (stmtp->var.retexp != NULL);

    if (_RDB_serialize_byte(objp, posp, hasexp, ecp) != RDB_OK) {
         return RDB_ERROR;
    }
    if (hasexp) {
        if (_RDB_serialize_expr(objp, posp, stmtp->var.retexp, ecp)
                != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

static int
serialize_possrep(RDB_object *objp, int *posp, const RDB_parse_possrep *rep,
        RDB_exec_context *ecp)
{
    RDB_expression *exp;

    if (_RDB_serialize_str(objp, posp,
            rep->namexp != NULL ? RDB_obj_string(RDB_expr_obj(rep->namexp)) : "",
            ecp) != RDB_OK)
        return RDB_ERROR;

    if (_RDB_serialize_int(objp, posp,
            RDB_expr_list_length(&rep->attrlist), ecp) != RDB_OK)
        return RDB_ERROR;

    exp = rep->attrlist.firstp;
    while (exp != NULL) {
        if (_RDB_serialize_expr(objp, posp, exp, ecp) != RDB_OK)
            return RDB_ERROR;
        exp = exp->nextp;
    }
    return RDB_OK;
}

static int
serialize_type_def(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    RDB_parse_possrep *rep;
    RDB_int repc;

    if (_RDB_serialize_str(objp, posp,
            RDB_obj_string(&stmtp->var.deftype.typename), ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    repc = 0;
    rep = stmtp->var.deftype.replistp;
    while (rep != NULL) {
        repc++;
        rep = rep->nextp;
    }
    if (_RDB_serialize_int(objp, posp, repc, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    rep = stmtp->var.deftype.replistp;
    while (rep != NULL) {
        if (serialize_possrep(objp, posp, rep, ecp) != RDB_OK)
            return RDB_ERROR;
        rep = rep->nextp;
    }

    if (_RDB_serialize_byte(objp, posp,
            (RDB_byte) (stmtp->var.deftype.constraintp != NULL), ecp)
            != RDB_OK) {
         return RDB_ERROR;
    }
    if (stmtp->var.deftype.constraintp != NULL) {
        if (_RDB_serialize_expr(objp, posp, stmtp->var.deftype.constraintp, ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
serialize_type_drop(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    return _RDB_serialize_str(objp, posp,
            RDB_obj_string(&stmtp->var.typedrop.typename), ecp);
}

static int
serialize_ro_op_def(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    int i;

    if (_RDB_serialize_str(objp, posp,
            RDB_obj_string(&stmtp->var.opdef.opname), ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_serialize_int(objp, posp, (RDB_int) stmtp->var.opdef.argc, ecp) != RDB_OK)
        return RDB_ERROR;
    for (i = 0; i < stmtp->var.opdef.argc; i++) {
        if (_RDB_serialize_str(objp, posp,
                RDB_obj_string(&stmtp->var.opdef.argv[i].name), ecp) != RDB_OK) {
            return RDB_ERROR;
        }
        if (_RDB_serialize_expr(objp, posp, stmtp->var.opdef.argv[i].type.exp, ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
    }
    if (_RDB_serialize_expr(objp, posp, stmtp->var.opdef.rtype.exp, ecp)
            != RDB_OK) {
        return RDB_ERROR;
    }
    return serialize_stmt_list(objp, posp, stmtp->var.opdef.bodyp, ecp);
} 

static int
serialize_upd_op_def(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    int i;

    if (_RDB_serialize_str(objp, posp,
            RDB_obj_string(&stmtp->var.opdef.opname), ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_serialize_int(objp, posp, (RDB_int) stmtp->var.opdef.argc, ecp) != RDB_OK)
        return RDB_ERROR;
    for (i = 0; i < stmtp->var.opdef.argc; i++) {
        if (_RDB_serialize_str(objp, posp,
                RDB_obj_string(&stmtp->var.opdef.argv[i].name), ecp) != RDB_OK) {
            return RDB_ERROR;
        }
        if (_RDB_serialize_expr(objp, posp, stmtp->var.opdef.argv[i].type.exp, ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
        if (_RDB_serialize_byte(objp, posp, (RDB_byte) stmtp->var.opdef.argv[i].upd, ecp)
                != RDB_OK) {
            return RDB_ERROR;
        }
    }
    return serialize_stmt_list(objp, posp, stmtp->var.opdef.bodyp, ecp);
} 

static int
serialize_op_drop(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    return _RDB_serialize_str(objp, posp,
            RDB_obj_string(&stmtp->var.opdrop.opname), ecp);
}

static int
serialize_stmt(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    if (_RDB_serialize_byte(objp, posp, (RDB_byte) stmtp->kind, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_serialize_int(objp, posp, stmtp->lineno, ecp) != RDB_OK) {
         return RDB_ERROR;
    }

    switch (stmtp->kind) {
        case RDB_STMT_NOOP:
            return RDB_OK;
        case RDB_STMT_CALL:
            return serialize_call(objp, posp, stmtp, ecp);
        case RDB_STMT_VAR_DEF:
            return serialize_var_def(objp, posp, stmtp, ecp);
        case RDB_STMT_VAR_DEF_REAL:
        case RDB_STMT_VAR_DEF_PRIVATE:
            return serialize_var_def_real(objp, posp, stmtp, ecp);
        case RDB_STMT_VAR_DEF_VIRTUAL:
            return serialize_var_def_virtual(objp, posp, stmtp, ecp);
        case RDB_STMT_VAR_DROP:
            return serialize_var_drop(objp, posp, stmtp, ecp);
        case RDB_STMT_IF:
            return serialize_if(objp, posp, stmtp, ecp);
        case RDB_STMT_FOR:
            return serialize_for(objp, posp, stmtp, ecp);
        case RDB_STMT_WHILE:
            return serialize_while(objp, posp, stmtp, ecp);
        case RDB_STMT_ASSIGN:
            return serialize_assignlist(objp, posp, stmtp->var.assignment.assignp, ecp);
        case RDB_STMT_BEGIN_TX:
        case RDB_STMT_COMMIT:
        case RDB_STMT_ROLLBACK:
            return RDB_OK;
        case RDB_STMT_TYPE_DEF:
            return serialize_type_def(objp, posp, stmtp, ecp);
        case RDB_STMT_TYPE_DROP:
            return serialize_type_drop(objp, posp, stmtp, ecp);
        case RDB_STMT_RO_OP_DEF:
            return serialize_ro_op_def(objp, posp, stmtp, ecp);
        case RDB_STMT_UPD_OP_DEF:
            return serialize_upd_op_def(objp, posp, stmtp, ecp);
        case RDB_STMT_OP_DROP:
            return serialize_op_drop(objp, posp, stmtp, ecp);
        case RDB_STMT_RETURN:
            return serialize_return(objp, posp, stmtp, ecp);
    }
    abort();
}

static RDB_int
stmt_list_length(const RDB_parse_statement *stmtp)
{
    RDB_int len = 0;
    while (stmtp != NULL) {
        len++;
        stmtp = stmtp->nextp;
    }
    return len;
}

static int
serialize_stmt_list(RDB_object *objp, int *posp, const RDB_parse_statement *stmtp,
        RDB_exec_context *ecp)
{
    if (_RDB_serialize_int(objp, posp, stmt_list_length(stmtp), ecp) != RDB_OK)
        return RDB_ERROR;

    while (stmtp != NULL) {
        if (serialize_stmt(objp, posp, stmtp, ecp) != RDB_OK)
            return RDB_ERROR;
        stmtp = stmtp->nextp;
    }
    return RDB_OK;
}

static int
deserialize_var_def(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    int hastype, hasexp;

    RDB_init_obj(&stmtp->var.vardef.varname);

    if (_RDB_deserialize_strobj(objp, posp, ecp, &stmtp->var.vardef.varname)
            != RDB_OK)
        return RDB_ERROR;
    hastype = _RDB_deserialize_byte(objp, posp, ecp);
    if (hastype == RDB_ERROR)
        return RDB_ERROR;
    stmtp->var.vardef.type.typ = NULL;
    if (hastype) {
        if (_RDB_deserialize_expr(objp, posp, ecp, txp, &stmtp->var.vardef.type.exp) != RDB_OK)
            return RDB_ERROR;
    } else {
        stmtp->var.vardef.type.exp = NULL;
    }
    hasexp = _RDB_deserialize_byte(objp, posp, ecp);
    if (hasexp == RDB_ERROR)
        return RDB_OK;
    if (hasexp) {
        if (_RDB_deserialize_expr(objp, posp, ecp, txp, &stmtp->var.vardef.exp) != RDB_OK)
            return RDB_ERROR;
    } else {
        stmtp->var.vardef.exp = NULL;
    }
    return RDB_OK;
}

static int
deserialize_expr_list(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp, RDB_expr_list *explistp)
{
    int i;
    RDB_int len;

    if (_RDB_deserialize_int(objp, posp, ecp, &len) != RDB_OK) {
        return RDB_ERROR;
    }

    explistp->firstp = explistp->lastp = NULL;

    for (i = 0; i < len; i++) {
        RDB_expression *exp;

        if (_RDB_deserialize_expr(objp, posp, ecp, txp, &exp) != RDB_OK)
            return RDB_ERROR;
        if (explistp->firstp == NULL) {
            explistp->firstp = explistp->lastp = exp;
        } else {
            explistp->lastp->nextp = exp;
            explistp->lastp = exp;
        }
    }
    if (explistp->lastp != NULL)
        explistp->lastp->nextp = NULL;
    return RDB_OK;
}    

static RDB_parse_keydef *
deserialize_key_def(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    RDB_int len;
    int i;
    RDB_parse_keydef *firstp = NULL;
    RDB_parse_keydef *lastp = NULL;

    if (_RDB_deserialize_int(objp, posp, ecp, &len) != RDB_OK) {
        return NULL;
    }

    /*
     * At least one key is required, so len > 1 and NULL is returned
     * only in case of error
     */

    for (i = 0; i < len; i++) {
        RDB_parse_keydef *keyp = RDB_alloc(sizeof(RDB_parse_keydef), ecp);
        if (keyp == NULL)
            return NULL;

        if (deserialize_expr_list(objp, posp, ecp, txp, &keyp->attrlist) != RDB_OK)
            return NULL;

        if (firstp == NULL) {
            firstp = lastp = keyp;
        } else {
            lastp->nextp = keyp;
            lastp = keyp;
        }
    }
    lastp->nextp = NULL;
    return firstp;
}

static int
deserialize_var_def_real(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    int hastype, hasexp;

    RDB_init_obj(&stmtp->var.vardef.varname);

    if (_RDB_deserialize_strobj(objp, posp, ecp, &stmtp->var.vardef.varname)
            != RDB_OK)
        return RDB_ERROR;
    hastype = _RDB_deserialize_byte(objp, posp, ecp);
    if (hastype == RDB_ERROR)
        return RDB_ERROR;
    if (hastype) {
        if (_RDB_deserialize_expr(objp, posp, ecp, txp,
                &stmtp->var.vardef.type.exp) != RDB_OK)
            return RDB_ERROR;

        stmtp->var.vardef.type.typ = NULL;
        if (stmtp->var.vardef.type.exp == NULL)
            return RDB_ERROR;
    } else {
        stmtp->var.vardef.type.exp = NULL;
    }
    hasexp = _RDB_deserialize_byte(objp, posp, ecp);
    if (hasexp == RDB_ERROR)
        return RDB_OK;
    if (hasexp) {
        if (_RDB_deserialize_expr(objp, posp, ecp, txp, &stmtp->var.vardef.exp) != RDB_OK)
            return RDB_ERROR;
    } else {
        stmtp->var.vardef.exp = NULL;
    }
    stmtp->var.vardef.firstkeyp = deserialize_key_def(objp, posp, ecp, txp, stmtp);
    if (stmtp->var.vardef.firstkeyp == NULL)
        return RDB_ERROR;

    return RDB_OK;
}

static int
deserialize_var_def_virtual(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    RDB_int keyc;

    RDB_init_obj(&stmtp->var.vardef.varname);

    if (_RDB_deserialize_strobj(objp, posp, ecp, &stmtp->var.vardef.varname)
            != RDB_OK)
        goto error;
    if (_RDB_deserialize_expr(objp, posp, ecp, txp, &stmtp->var.vardef.exp) != RDB_OK)
        goto error;

    if (_RDB_deserialize_int(objp, posp, ecp, &keyc) != RDB_OK)
        goto error;

    return RDB_OK;

error:
    RDB_destroy_obj(&stmtp->var.vardef.varname, ecp);
    return RDB_ERROR;
}

static int
deserialize_var_drop(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    RDB_init_obj(&stmtp->var.vardrop.varname);

    if (_RDB_deserialize_strobj(objp, posp, ecp, &stmtp->var.vardrop.varname)
            != RDB_OK)
        return RDB_ERROR;
    return RDB_OK;
}

static int
deserialize_stmt_list(RDB_object *, int *, RDB_exec_context *,
    RDB_transaction *, RDB_parse_statement **);

static int
deserialize_if(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    if (_RDB_deserialize_expr(objp, posp, ecp, txp, &stmtp->var.ifthen.condp) != RDB_OK)
        return RDB_ERROR;
    if (deserialize_stmt_list(objp, posp, ecp, txp, &stmtp->var.ifthen.ifp) != RDB_OK)
        return RDB_ERROR;
    if (deserialize_stmt_list(objp, posp, ecp, txp, &stmtp->var.ifthen.elsep) != RDB_OK)
        return RDB_ERROR;
    return RDB_OK;
}

static int
deserialize_for(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    if (_RDB_deserialize_expr(objp, posp, ecp, txp, &stmtp->var.forloop.varexp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_deserialize_expr(objp, posp, ecp, txp, &stmtp->var.forloop.fromp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_deserialize_expr(objp, posp, ecp, txp, &stmtp->var.forloop.top) != RDB_OK)
        return RDB_ERROR;
    if (deserialize_stmt_list(objp, posp, ecp, txp, &stmtp->var.forloop.bodyp) != RDB_OK)
        return RDB_ERROR;
    return RDB_OK;
}

static int
deserialize_while(RDB_object *objp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    if (_RDB_deserialize_expr(objp, posp, ecp, txp, &stmtp->var.whileloop.condp) != RDB_OK)
        return RDB_ERROR;
    if (deserialize_stmt_list(objp, posp, ecp, txp, &stmtp->var.whileloop.bodyp) != RDB_OK)
        return RDB_ERROR;
    return RDB_OK;
}

static int
deserialize_call(RDB_object *objp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    RDB_init_obj(&stmtp->var.call.opname);
    if (_RDB_deserialize_strobj(objp, posp, ecp, &stmtp->var.call.opname) != RDB_OK) {
        return RDB_ERROR;
    }
    if (deserialize_expr_list(objp, posp, ecp, txp, &stmtp->var.call.arglist)
            != RDB_OK) {
        RDB_destroy_obj(&stmtp->var.call.opname, ecp);
        return RDB_ERROR;        
    }
    return RDB_OK;
}

static RDB_parse_possrep *
deserialize_possrep(RDB_object *objp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object nameobj;
    RDB_parse_possrep *rep = RDB_alloc(sizeof(RDB_parse_possrep), ecp);
    if (rep == NULL)
        return NULL;

    RDB_init_obj(&nameobj);

    if (_RDB_deserialize_strobj(objp, posp, ecp, &nameobj) != RDB_OK) {
        goto error;
    }
    if (RDB_obj_string(&nameobj)[0] == '\0') {
        rep->namexp = NULL;
    } else {
        rep->namexp = RDB_obj_to_expr(&nameobj, ecp);
        if (rep->namexp == NULL)
            goto error;
    }

    if (deserialize_expr_list(objp, posp, ecp, txp, &rep->attrlist) != RDB_OK)
        goto error;

    RDB_destroy_obj(&nameobj, ecp);
    return rep;

error:
    RDB_destroy_obj(&nameobj, ecp);
    RDB_free(rep);
    return NULL;
}

static int
deserialize_type_def(RDB_object *objp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    int i;
    RDB_int repc;
    int hasconstr;
    RDB_parse_possrep *rep, *lastrep;

    RDB_init_obj(&stmtp->var.deftype.typename);
    if (_RDB_deserialize_strobj(objp, posp, ecp, &stmtp->var.deftype.typename)
            != RDB_OK) {
        return RDB_ERROR;
    }
    if (_RDB_deserialize_int(objp, posp, ecp, &repc) != RDB_OK) {
        return RDB_ERROR;
    }
    lastrep = NULL;
    for (i = 0; i < repc; i++) {
        rep = deserialize_possrep(objp, posp, ecp, txp);
        if (rep == NULL)
            return RDB_ERROR;
        if (lastrep != NULL) {
            lastrep->nextp = rep;
        } else {
            stmtp->var.deftype.replistp = rep;
        }
        lastrep = rep;
        lastrep->nextp = NULL;
    }          

    hasconstr = _RDB_deserialize_byte(objp, posp, ecp);
    if (hasconstr == RDB_ERROR) {
         return RDB_ERROR;
    }
    if (hasconstr) {
        if (_RDB_deserialize_expr(objp, posp, ecp, txp, &stmtp->var.deftype.constraintp)
                != RDB_OK) {
            return RDB_ERROR;
        }
    } else {
        stmtp->var.deftype.constraintp = NULL;
    }
    return RDB_OK;    
}

static int
deserialize_type_drop(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    RDB_init_obj(&stmtp->var.typedrop.typename);

    if (_RDB_deserialize_strobj(objp, posp, ecp, &stmtp->var.typedrop.typename)
            != RDB_OK)
        return RDB_ERROR;
    return RDB_OK;
}

static int
deserialize_ro_op_def(RDB_object *objp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    int i;
    RDB_int argc;

    RDB_init_obj(&stmtp->var.opdef.opname);
    if (_RDB_deserialize_strobj(objp, posp, ecp, &stmtp->var.opdef.opname) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_deserialize_int(objp, posp, ecp, &argc) != RDB_OK)
        return RDB_ERROR;
    stmtp->var.opdef.argc = (int) argc;
    stmtp->var.opdef.argv = RDB_alloc(argc * sizeof(RDB_parse_arg), ecp);
    if (stmtp->var.opdef.argv == NULL)
        return RDB_ERROR;
    for (i = 0; i < argc; i++) {
        RDB_init_obj(&stmtp->var.opdef.argv[i].name);
        if (_RDB_deserialize_strobj(objp, posp, ecp,
                &stmtp->var.opdef.argv[i].name) != RDB_OK) {
            return RDB_ERROR;
        }
        if (_RDB_deserialize_expr(objp, posp, ecp, txp,
                &stmtp->var.opdef.argv[i].type.exp) != RDB_OK) {
            return RDB_ERROR;
        }
        stmtp->var.opdef.argv[i].type.typ = NULL;
    }
    if (_RDB_deserialize_expr(objp, posp, ecp, txp,
            &stmtp->var.opdef.rtype.exp) != RDB_OK) {
        return RDB_ERROR;
    }
    stmtp->var.opdef.rtype.typ = NULL;
    return deserialize_stmt_list(objp, posp, ecp, txp, &stmtp->var.opdef.bodyp);
} 

static int
deserialize_op_drop(RDB_object *objp, int *posp, RDB_exec_context *ecp,
        RDB_parse_statement *stmtp)
{
    RDB_init_obj(&stmtp->var.opdrop.opname);
    return _RDB_deserialize_strobj(objp, posp, ecp, &stmtp->var.opdrop.opname);
}

static int
deserialize_upd_op_def(RDB_object *objp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    int i;
    RDB_int argc;

    RDB_init_obj(&stmtp->var.opdef.opname);
    if (_RDB_deserialize_strobj(objp, posp, ecp, &stmtp->var.opdef.opname) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_deserialize_int(objp, posp, ecp, &argc) != RDB_OK)
        return RDB_ERROR;
    stmtp->var.opdef.argc = (int) argc;
    stmtp->var.opdef.argv = RDB_alloc(argc * sizeof(RDB_parse_arg), ecp);
    if (stmtp->var.opdef.argv == NULL)
        return RDB_ERROR;
    for (i = 0; i < argc; i++) {
        int b;

        RDB_init_obj(&stmtp->var.opdef.argv[i].name);
        if (_RDB_deserialize_strobj(objp, posp, ecp,
                &stmtp->var.opdef.argv[i].name) != RDB_OK) {
            return RDB_ERROR;
        }
        if (_RDB_deserialize_expr(objp, posp, ecp, txp,
                &stmtp->var.opdef.argv[i].type.exp) != RDB_OK) {
            return RDB_ERROR;
        }
        stmtp->var.opdef.argv[i].type.typ = NULL;

        b = _RDB_deserialize_byte(objp, posp, ecp);
        if (b == RDB_ERROR)
            return RDB_ERROR;
        stmtp->var.opdef.argv[i].upd = (RDB_byte) b;
    }
    return deserialize_stmt_list(objp, posp, ecp, txp, &stmtp->var.opdef.bodyp);
} 

static int
deserialize_assignlist(RDB_object *, int *, RDB_exec_context *,
    RDB_transaction *, RDB_parse_assign **);

static RDB_parse_assign *
deserialize_assign(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp)
{
    RDB_int len;
    RDB_int iv;
    int hascond;
    RDB_parse_assign *assignp = RDB_alloc(sizeof(RDB_parse_assign), ecp);
    if (assignp == NULL)
        return NULL;

    if (_RDB_deserialize_int(objp, posp, ecp, &iv) != RDB_OK)
        goto error;
    assignp->kind = iv;
    switch (assignp->kind) {
        case RDB_STMT_COPY:
            if (_RDB_deserialize_expr(objp, posp, ecp, txp,
                    &assignp->var.copy.dstp) != RDB_OK)
                goto error;
            if (_RDB_deserialize_expr(objp, posp, ecp, txp,
                    &assignp->var.copy.srcp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_INSERT:
            if (_RDB_deserialize_expr(objp, posp, ecp, txp,
                    &assignp->var.ins.dstp) != RDB_OK)
                goto error;
            if (_RDB_deserialize_expr(objp, posp, ecp, txp,
                    &assignp->var.ins.srcp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_UPDATE:
            if (_RDB_deserialize_expr(objp, posp, ecp, txp,
                    &assignp->var.upd.dstp) != RDB_OK)
                goto error;
            hascond = _RDB_deserialize_byte(objp, posp, ecp);
            if (hascond == RDB_ERROR)
                goto error;
            if (hascond) {
                if (_RDB_deserialize_expr(objp, posp, ecp, txp,
                        &assignp->var.upd.condp) != RDB_OK)
                    goto error;
            } else {
                assignp->var.upd.condp = NULL;
            }
            if (_RDB_deserialize_int(objp, posp, ecp, &len) != RDB_OK)
                goto error;
            if (deserialize_assignlist(objp, posp, ecp, txp,
                    &assignp->var.upd.assignlp) != RDB_OK) {
                goto error;
            }
            break;
        case RDB_STMT_DELETE:
            if (_RDB_deserialize_expr(objp, posp, ecp, txp,
                    &assignp->var.del.dstp) != RDB_OK)
                goto error;
            hascond = _RDB_deserialize_byte(objp, posp, ecp);
            if (hascond == RDB_ERROR)
                goto error;
            if (hascond) {
                if (_RDB_deserialize_expr(objp, posp, ecp, txp,
                        &assignp->var.del.condp) != RDB_OK)
                    goto error;
            } else {
                assignp->var.del.condp = NULL;
            }
            break;
    }
    return assignp;

error:
    RDB_free(assignp);
    return NULL;
}

static int
deserialize_assignlist(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp, RDB_parse_assign **assignpp)
{
    int i;
    RDB_int ac;
    RDB_parse_assign *assignp;
    RDB_parse_assign *assignlistp = NULL;

    if (_RDB_deserialize_int(objp, posp, ecp, &ac) != RDB_OK)
        return RDB_ERROR;
    for (i = 0; i < ac; i++) {
        assignp = deserialize_assign(objp, posp, ecp, txp);
        if (assignp == NULL)
            goto error;
        if (assignlistp == NULL) {
            assignp->nextp = NULL;
        } else {
            assignp->nextp = assignlistp;
        }
        assignlistp = assignp;
    }
    *assignpp = assignlistp;
    return RDB_OK;

error:
    if (assignlistp != NULL)
        RDB_parse_del_assignlist(assignlistp, ecp);
    return RDB_ERROR;
}

static int
deserialize_return(RDB_object *objp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_parse_statement *stmtp)
{
    int hasexp = _RDB_deserialize_byte(objp, posp, ecp);
    if (hasexp == RDB_ERROR)
        return RDB_ERROR;

    if (hasexp) {
        if (_RDB_deserialize_expr(objp, posp, ecp, txp, &stmtp->var.retexp)
                != RDB_OK) {
            return RDB_ERROR;
        }
    } else {
        stmtp->var.retexp = NULL;
    }
    return RDB_OK;
}

static RDB_parse_statement *
deserialize_stmt(RDB_object *objp, int *posp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_parse_statement *stmtp;
    int ret = _RDB_deserialize_byte(objp, posp, ecp);
    if (ret == RDB_ERROR)
        return NULL;

    stmtp = RDB_alloc(sizeof(RDB_parse_statement), ecp);
    if (stmtp == NULL) {
        return NULL;
    }
    if (_RDB_deserialize_int(objp, posp, ecp, &stmtp->lineno) != RDB_OK) {
        RDB_free(stmtp);
        return NULL;
    }
    stmtp->kind = (RDB_parse_stmt_kind) ret;
    switch (stmtp->kind) {
        case RDB_STMT_NOOP:
            break;
        case RDB_STMT_CALL:
            if (deserialize_call(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_VAR_DEF:
            if (deserialize_var_def(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_VAR_DEF_REAL:
        case RDB_STMT_VAR_DEF_PRIVATE:
            if (deserialize_var_def_real(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_VAR_DEF_VIRTUAL:
            if (deserialize_var_def_virtual(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_VAR_DROP:
            if (deserialize_var_drop(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_IF:
            if (deserialize_if(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_FOR:
            if (deserialize_for(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_WHILE:
            if (deserialize_while(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_ASSIGN:
            if (deserialize_assignlist(objp, posp, ecp, txp,
                    &stmtp->var.assignment.assignp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_BEGIN_TX:
        case RDB_STMT_COMMIT:
        case RDB_STMT_ROLLBACK:
            break;
        case RDB_STMT_TYPE_DEF:
            if (deserialize_type_def(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_TYPE_DROP:
            if (deserialize_type_drop(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_RO_OP_DEF:
            if (deserialize_ro_op_def(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_UPD_OP_DEF:            
            if (deserialize_upd_op_def(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_OP_DROP:
            if (deserialize_op_drop(objp, posp, ecp, stmtp) != RDB_OK)
                goto error;
            break;
        case RDB_STMT_RETURN:
            if (deserialize_return(objp, posp, ecp, txp, stmtp) != RDB_OK)
                goto error;
            break;
    }
    return stmtp;

error:
    RDB_free(stmtp);
    return NULL;
}

static int
deserialize_stmt_list(RDB_object *objp, int *posp, RDB_exec_context *ecp,
    RDB_transaction *txp, RDB_parse_statement **stmtpp)
{
    int i;
    RDB_int len;
    RDB_parse_statement *stmtp;
    RDB_parse_statement *firststmtp = NULL;
    RDB_parse_statement *laststmtp = NULL;

    if (_RDB_deserialize_int(objp, posp, ecp, &len) != RDB_OK)
        return RDB_ERROR;

    for (i = 0; i < len; i++) {
        stmtp = deserialize_stmt(objp, posp, ecp, txp);
        if (stmtp == NULL) {
            return RDB_ERROR;
        }
        if (firststmtp == NULL) {
            firststmtp = laststmtp = stmtp;
        } else {
            laststmtp->nextp = stmtp;
            laststmtp = stmtp;
        }
    }
    if (laststmtp != NULL)
        laststmtp->nextp = NULL;

    *stmtpp = firststmtp;
    return RDB_OK;
}

int
Duro_op_to_binobj(RDB_object *objp, const RDB_parse_statement *opstmtp,
        RDB_exec_context *ecp)
{
    int pos;
    int i;
    RDB_parse_statement *stmtp;

    RDB_destroy_obj(objp, ecp);
    objp->typ = &RDB_BINARY;
    objp->kind = RDB_OB_BIN;
    objp->var.bin.len = RDB_BUF_INITLEN;
    objp->var.bin.datap = RDB_alloc(RDB_BUF_INITLEN, ecp);
    if (objp->var.bin.datap == NULL) {
        return RDB_ERROR;
    }
    pos = 0;

    /* Serialize argument names */
    for (i = 0; i < opstmtp->var.opdef.argc; i++) {
        if (_RDB_serialize_str(objp, &pos,
                RDB_obj_string(&opstmtp->var.opdef.argv[i].name), ecp) != RDB_OK)
            return RDB_ERROR;
    }   

    /* Serialize body */
    stmtp = opstmtp->var.opdef.bodyp;
    do {
        if (serialize_stmt(objp, &pos, stmtp, ecp) != RDB_OK)
            return RDB_ERROR;
        stmtp = stmtp->nextp;
    } while (stmtp != NULL);
    objp->var.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

RDB_parse_statement *
Duro_bin_to_stmts(const void *p, size_t len, int argc, RDB_exec_context *ecp,
        RDB_transaction *txp, char **argnamev)
{
    int i;
    RDB_object binobj;
    RDB_parse_statement *stmtp;
    int pos = 0;
    RDB_parse_statement *firststmtp = NULL;
    RDB_parse_statement *laststmtp = NULL;

    RDB_init_obj(&binobj);
    if (RDB_binary_set(&binobj, 0, p, len, ecp) != RDB_OK)
        return NULL;

    for (i = 0; i < argc; i++) {
        if (_RDB_deserialize_str(&binobj, &pos, ecp, &argnamev[i]) != RDB_OK)
            return NULL;
    }

    while (pos < len) {
        stmtp = deserialize_stmt(&binobj, &pos, ecp, txp);
        if (stmtp == NULL) {
            RDB_destroy_obj(&binobj, ecp);
            return NULL;
        }
        stmtp->nextp = NULL;
        if (firststmtp == NULL) {
            firststmtp = laststmtp = stmtp;
        } else {
            laststmtp->nextp = stmtp;
            laststmtp = stmtp;
        }
    }
    if (laststmtp != NULL)
        laststmtp->nextp = NULL;

    RDB_destroy_obj(&binobj, ecp);
    return firststmtp;
}
