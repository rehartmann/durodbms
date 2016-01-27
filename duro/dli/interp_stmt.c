/*
 * The interpreter main loop
 *
 * Copyright (C) 2016 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "interp_core.h"
#include "interp_vardef.h"
#include "interp_assign.h"
#include "exparse.h"
#include <gen/strfns.h>
#include <rel/optimize.h>
#include <rel/typeimpl.h>
#include <rel/tostr.h>

#include <stdlib.h>
#include <string.h>
#include <errno.h>

extern int yylineno;
extern YY_BUFFER_STATE RDB_parse_buffer;
extern int RDB_parse_buffer_valid;

typedef struct yy_buffer_state *YY_BUFFER_STATE;

YY_BUFFER_STATE yy_scan_string(const char *txt);
void yy_delete_buffer(YY_BUFFER_STATE);
void yy_switch_to_buffer(YY_BUFFER_STATE);

/*
 * Check if *stmt is a COMMIT, ROLLBACK, or BEGIN TX
 */
static RDB_bool
is_tx_stmt(RDB_parse_node *stmtp) {
    RDB_parse_node *firstchildp = stmtp->val.children.firstp;

    return firstchildp->kind == RDB_NODE_TOK
            && (firstchildp->val.token == TOK_COMMIT
                || firstchildp->val.token == TOK_ROLLBACK
                || (firstchildp->val.token == TOK_BEGIN
                    && firstchildp->nextp->kind == RDB_NODE_TOK));
}

static int
Duro_exec_stmt(RDB_parse_node *, Duro_interp *interp, RDB_exec_context *,
        Duro_return_info *);

static int
exec_stmts(RDB_parse_node *stmtp, Duro_interp *interp, RDB_exec_context *ecp,
        Duro_return_info *retinfop)
{
    int ret;
    while (stmtp != NULL) {
        ret = Duro_exec_stmt(stmtp, interp, ecp, retinfop);
        if (ret != RDB_OK)
            return ret;
        stmtp = stmtp->nextp;
    }
    return RDB_OK;
}

static int
exec_if(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp,
        Duro_return_info *retinfop)
{
    RDB_bool b;
    int ret;
    RDB_expression *condp = RDB_parse_node_expr(nodep, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (condp == NULL)
        return RDB_ERROR;

    if (RDB_evaluate_bool(condp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL, &b) != RDB_OK) {
        return RDB_ERROR;
    }
    if (Duro_add_varmap(interp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (b) {
        ret = exec_stmts(nodep->nextp->nextp->val.children.firstp,
                interp, ecp, retinfop);
    } else if (nodep->nextp->nextp->nextp->val.token == TOK_ELSE) {
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->val.children.firstp,
                interp, ecp, retinfop);
    } else {
        ret = RDB_OK;
    }

    Duro_remove_varmap(interp);
    return ret;
}

static int
exec_case(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp,
        Duro_return_info *retinfop)
{
    RDB_parse_node *whenp;
    int ret;

    if (nodep->kind == RDB_NODE_TOK) {
        /* Skip semicolon */
        nodep = nodep->nextp;
    }

    whenp = nodep->val.children.firstp;
    while (whenp != NULL) {
        RDB_bool b;
        RDB_expression *condp = RDB_parse_node_expr(whenp->val.children.firstp->nextp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (condp == NULL)
            return RDB_ERROR;

        if (RDB_evaluate_bool(condp, &Duro_get_var, interp, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL, &b)
                != RDB_OK) {
            return RDB_ERROR;
        }
        if (b) {
            if (Duro_add_varmap(interp, ecp) != RDB_OK)
                return RDB_ERROR;
            ret = exec_stmts(whenp->val.children.firstp->nextp->nextp->nextp->val.children.firstp,
                    interp, ecp, retinfop);
            Duro_remove_varmap(interp);
            return ret;
        }
        whenp = whenp->nextp;
    }
    if (nodep->nextp->nextp->kind == RDB_NODE_INNER) {
        /* ELSE branch */
        if (Duro_add_varmap(interp, ecp) != RDB_OK)
            return RDB_ERROR;
        ret = exec_stmts(nodep->nextp->nextp->val.children.firstp,
                interp, ecp, retinfop);
        Duro_remove_varmap(interp);
        return ret;
    }
    return RDB_OK;
}

static int
exec_while(RDB_parse_node *nodep, RDB_parse_node *labelp, Duro_interp *interp,
        RDB_exec_context *ecp, Duro_return_info *retinfop)
{
    int ret;
    RDB_bool b;
    RDB_expression *condp = RDB_parse_node_expr(nodep, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (condp == NULL)
        return RDB_ERROR;

    for(;;) {
        if (RDB_evaluate_bool(condp, &Duro_get_var, interp, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL, &b) != RDB_OK)
            return RDB_ERROR;
        if (!b)
            return RDB_OK;
        if (Duro_add_varmap(interp, ecp) != RDB_OK)
            return RDB_ERROR;
        ret = exec_stmts(nodep->nextp->nextp->val.children.firstp, interp, ecp, retinfop);
        if (ret != RDB_OK) {
            if (ret == DURO_LEAVE) {
                /*
                 * If the statement name matches the LEAVE target,
                 * exit loop with RDB_OK
                 */
                if (labelp != NULL
                        && strcmp(RDB_expr_var_name(labelp->exp),
                                interp->leave_targetname) == 0) {
                    /* Target name matches label */
                    ret = RDB_OK;
                } else if (interp->leave_targetname == NULL) {
                    /* No label and target is NULL */
                    ret = RDB_OK;
                }
            }
            Duro_remove_varmap(interp);
            break;
        }
        Duro_remove_varmap(interp);

        /*
         * Check interrupt flag - this allows the user to break out of loops
         * with Control-C
         */
        if (interp->interrupted) {
            interp->interrupted = 0;
            RDB_raise_system("interrupted", ecp);
            return RDB_ERROR;
        }
    }
    return ret;
}

static int
exec_for(const RDB_parse_node *nodep, const RDB_parse_node *labelp,
        Duro_interp *interp, RDB_exec_context *ecp, Duro_return_info *retinfop)
{
    int ret;
    RDB_object endval;
    RDB_expression *fromexp, *toexp;
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_object *varp = Duro_lookup_transient_var(interp, varname);
    if (varp == NULL) {
        RDB_raise_name(RDB_expr_var_name(nodep->exp), ecp);
        return RDB_ERROR;
    }

    if (RDB_obj_type(varp) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("loop variable must be of type integer", ecp);
        return RDB_ERROR;
    }

    fromexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (fromexp == NULL)
        return RDB_ERROR;
    if (RDB_evaluate(fromexp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL, varp) != RDB_OK) {
        return RDB_ERROR;
    }

    toexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (toexp == NULL)
        return RDB_ERROR;
    RDB_init_obj(&endval);
    if (RDB_evaluate(toexp, &Duro_get_var, interp, interp->envp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL, &endval) != RDB_OK) {
        goto error;
    }
    if (RDB_obj_type(&endval) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("expression must be of type integer", ecp);
        goto error;
    }

    while (RDB_obj_int(varp) <= RDB_obj_int(&endval)) {
        if (Duro_add_varmap(interp, ecp) != RDB_OK)
            goto error;

        /* Execute statements */
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->nextp->nextp->val.children.firstp,
                interp, ecp, retinfop);
        if (ret != RDB_OK) {
            if (ret == DURO_LEAVE) {
                /*
                 * If the statement name matches the LEAVE target,
                 * exit loop with RDB_OK
                 */
                if (labelp != NULL
                        && strcmp(RDB_expr_var_name(labelp->exp),
                                interp->leave_targetname) == 0) {
                    ret = RDB_OK;
                } else if (interp->leave_targetname == NULL) {
                    /* No label and target is NULL */
                    ret = RDB_OK;
                }
            }
            Duro_remove_varmap(interp);
            RDB_destroy_obj(&endval, ecp);
            return ret;
        }
        Duro_remove_varmap(interp);

        /* Check if the variable has been dropped */
        varp = Duro_lookup_transient_var(interp, varname);
        if (varp == NULL) {
            RDB_raise_name(varname, ecp);
            goto error;
        }

        /*
         * Check for user interrupt
         */
        if (interp->interrupted) {
            interp->interrupted = 0;
            RDB_raise_system("interrupted", ecp);
            goto error;
        }
        varp->val.int_val++;
    }
    RDB_destroy_obj(&endval, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&endval, ecp);
    return RDB_ERROR;
}

static int
exec_foreach(const RDB_parse_node *nodep, const RDB_parse_node *labelp,
        Duro_interp *interp, RDB_exec_context *ecp, Duro_return_info *retinfop)
{
    int ret;
    RDB_expression *tbexp;
    RDB_type *tbtyp;
    int seqitc;
    foreach_iter it;
    RDB_parse_node *seqitnodep;
    RDB_seq_item *seqitv = NULL;
    const char *srcvarname;
    RDB_object tb;
    RDB_transaction *txp = interp->txnp != NULL ? &interp->txnp->tx : NULL;
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_object *varp = Duro_lookup_transient_var(interp, varname);
    if (varp == NULL) {
        RDB_raise_name(varname, ecp);
        return RDB_ERROR;
    }

    it.qrp = NULL;

    tbexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, txp);
    if (tbexp == NULL)
        return RDB_ERROR;

    tbtyp = RDB_expr_type(tbexp, &Duro_get_var_type, interp,
                interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (tbtyp == NULL)
        return RDB_ERROR;
    if (!RDB_type_is_relation(tbtyp)) {
        RDB_raise_type_mismatch("table required", ecp);
        return RDB_ERROR;
    }
    if (!RDB_type_equals(RDB_obj_type(varp), RDB_base_type(tbtyp))) {
        RDB_raise_type_mismatch("type of loop variable does not match table type",
                ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tb);
    /*
     * If the expression is a variable reference, look up the variable,
     * otherwise evaluate the expression
     */
    srcvarname = RDB_expr_var_name(tbexp);
    if (srcvarname != NULL) {
        it.tbp = Duro_lookup_var(srcvarname, interp, ecp);
        if (it.tbp == NULL)
            goto error;
    } else {
        if (RDB_evaluate(tbexp, &Duro_get_var, interp, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL, &tb) != RDB_OK) {
            goto error;
        }
        it.tbp = &tb;
    }

    seqitnodep = nodep->nextp->nextp->nextp->nextp->nextp;
    seqitc = (RDB_parse_nodelist_length(seqitnodep) + 1) / 2;
    if (seqitc > 0) {
        seqitv = RDB_alloc(sizeof(RDB_seq_item) * seqitc, ecp);
        if (seqitv == NULL)
            goto error;
    }
    seqitnodep = nodep->nextp->nextp->nextp->nextp->nextp;
    if (Duro_nodes_to_seqitv(seqitv, seqitnodep->val.children.firstp, interp, ecp)
                    != RDB_OK) {
        goto error;
    }
    it.qrp = RDB_table_iterator(it.tbp, seqitc, seqitv, ecp, txp);
    if (it.qrp == NULL)
        goto error;

    it.prevp = interp->current_foreachp;
    interp->current_foreachp = &it;

    while (RDB_next_tuple(it.qrp, varp, ecp, txp) == RDB_OK) {
        if (Duro_add_varmap(interp, ecp) != RDB_OK)
            goto error;

        /* Execute statements */
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->nextp
                ->nextp->nextp->nextp->val.children.firstp,
                interp, ecp, retinfop);
        Duro_remove_varmap(interp);
        if (ret == DURO_LEAVE) {
            /*
             * If the statement name matches the LEAVE target,
             * exit loop with RDB_OK
             */
            if (labelp != NULL
                    && strcmp(RDB_expr_var_name(labelp->exp),
                            interp->leave_targetname) == 0) {
                ret = RDB_OK;
            } else if (interp->leave_targetname == NULL) {
                /* No label and target is NULL, exit loop */
                ret = RDB_OK;
            }
            break;
        }
        if (ret != RDB_OK)
            goto error;

        /* Check if the variable has been dropped */
        varp = Duro_lookup_transient_var(interp, varname);
        if (varp == NULL) {
            RDB_raise_name(varname, ecp);
            goto error;
        }

        /*
         * Check for user interrupt
         */
        if (interp->interrupted) {
            interp->interrupted = 0;
            RDB_raise_system("interrupted", ecp);
            goto error;
        }
    }

    /* Set to previous FOREACH iterator (or NULL) */
    interp->current_foreachp = it.prevp;

    RDB_free(seqitv);
    if (RDB_del_table_iterator(it.qrp, ecp, txp) != RDB_OK) {
        RDB_destroy_obj(&tb, ecp);
        return RDB_ERROR;
    }

    RDB_destroy_obj(&tb, ecp);

    /* Ignore not_found */
    if (ret == RDB_ERROR) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
            return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    return RDB_OK;

error:
    if (it.qrp != NULL) {
        RDB_del_table_iterator(it.qrp, ecp, txp);
        interp->current_foreachp = it.prevp;
    }
    RDB_destroy_obj(&tb, ecp);
    RDB_free(seqitv);
    return RDB_ERROR;
}

static int
do_begin_tx(Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_database *dbp = Duro_get_db(interp, ecp);
    if (dbp == NULL)
        return RDB_ERROR;

    if (interp->txnp != NULL) {
        /* Start subtransaction */
        tx_node *ntxnp = RDB_alloc(sizeof(tx_node), ecp);
        if (ntxnp == NULL) {
            return RDB_ERROR;
        }

        if (RDB_begin_tx(ecp, &ntxnp->tx, dbp, &interp->txnp->tx) != RDB_OK) {
            RDB_free(ntxnp);
            return RDB_ERROR;
        }
        ntxnp->parentp = interp->txnp;
        interp->txnp = ntxnp;

        return RDB_OK;
    }

    interp->txnp = RDB_alloc(sizeof(tx_node), ecp);
    if (interp->txnp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_begin_tx(ecp, &interp->txnp->tx, dbp, NULL) != RDB_OK) {
        RDB_free(interp->txnp);
        interp->txnp = NULL;
        return RDB_ERROR;
    }
    interp->txnp->parentp = NULL;

    return RDB_OK;
}

static int
exec_begin_tx(Duro_interp *interp, RDB_exec_context *ecp)
{
    int subtx = (interp->txnp != NULL);

    if (do_begin_tx(interp, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf(subtx ? "Subtransaction started.\n"
                : "Transaction started.\n");

    return RDB_OK;
}

static int
do_commit(Duro_interp *interp, RDB_exec_context *ecp)
{
    tx_node *ptxnp;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (interp->current_foreachp != NULL) {
        RDB_raise_in_use("COMMIT not allowed in FOREACH", ecp);
        return RDB_ERROR;
    }

    if (RDB_commit(ecp, &interp->txnp->tx) != RDB_OK)
        return RDB_ERROR;

    ptxnp = interp->txnp->parentp;
    RDB_free(interp->txnp);
    interp->txnp = ptxnp;

    return RDB_OK;
}

static int
exec_commit(Duro_interp *interp, RDB_exec_context *ecp)
{
    if (do_commit(interp, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf("Transaction committed.\n");

    return RDB_OK;
}

static int
do_rollback(Duro_interp *interp, RDB_exec_context *ecp)
{
    tx_node *ptxnp;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (interp->current_foreachp != NULL) {
        RDB_raise_in_use("ROLLBACK not allowed in FOREACH", ecp);
        return RDB_ERROR;
    }

    if (RDB_rollback(ecp, &interp->txnp->tx) != RDB_OK)
        return RDB_ERROR;

    ptxnp = interp->txnp->parentp;
    RDB_free(interp->txnp);
    interp->txnp = ptxnp;

    return RDB_OK;
}

static int
exec_rollback(Duro_interp *interp, RDB_exec_context *ecp)
{
    if (do_rollback(interp, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf("Transaction rolled back.\n");

    return RDB_OK;
}

static int
parserep_to_rep(const RDB_parse_node *nodep, Duro_interp *interp, RDB_possrep *rep,
        RDB_exec_context *ecp)
{
    int i;
    RDB_parse_node *np;

    if (nodep->val.children.firstp->nextp->kind == RDB_NODE_EXPR) {
        rep->name = (char *) RDB_expr_var_name(nodep->val.children.firstp->nextp->exp);
        nodep = nodep->val.children.firstp->nextp->nextp->nextp;
    } else {
        rep->name = NULL;
        nodep = nodep->val.children.firstp->nextp->nextp;
    }
    rep->compc = (RDB_parse_nodelist_length(nodep) + 1) / 3;

    rep->compv = RDB_alloc(rep->compc * sizeof(RDB_attr), ecp);
    if (rep->compv == NULL)
        return RDB_ERROR;
    for (i = 0; i < rep->compc; i++) {
        rep->compv[i].name = NULL;
        rep->compv[i].typ = NULL;
    }

    np = nodep->val.children.firstp;
    for (i = 0; i < rep->compc; i++) {
        rep->compv[i].name = (char *) RDB_expr_var_name(np->exp);
        np = np->nextp;
        rep->compv[i].typ = (RDB_type *) RDB_parse_node_to_type(np,
                &Duro_get_var_type, interp, ecp, &interp->txnp->tx);
        if (rep->compv[i].typ == NULL)
            return RDB_ERROR;
        if (RDB_type_is_generic(rep->compv[i].typ)) {
            RDB_raise_syntax("generic type not permitted", ecp);
            for (i = 0; i < rep->compc; i++) {
                if (!RDB_type_is_scalar(rep->compv[i].typ)) {
                    RDB_del_nonscalar_type(rep->compv[i].typ, ecp);
                }
            }
            RDB_free(rep->compv);
            rep->compv = NULL;
            return RDB_ERROR;
        }
        np = np->nextp;
        if (np != NULL) /* Skip comma */
            np = np->nextp;
    }
    return RDB_OK;
}

static int
exec_typedef(const RDB_parse_node *stmtp, Duro_interp *interp, RDB_exec_context *ecp)
{
    int i, j;
    int flags;
    int llen;
    int repc;
    RDB_possrep *repv = NULL;
    RDB_parse_node *nodep, *prnodep;
    RDB_expression *initexp;
    RDB_expression *constraintp = NULL;
    RDB_object nameobj;
    const char *namp;
    RDB_type **suptypev = NULL;
    int suptypec = 0;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    llen = RDB_parse_nodelist_length(stmtp->nextp);
    if (llen > 0) {
        llen = RDB_parse_nodelist_length(stmtp->nextp->val.children.firstp->nextp);
        suptypec = (llen + 1) / 2;
        suptypev = RDB_alloc (sizeof(RDB_type *) * suptypec, ecp);
        if (suptypev == NULL)
            return RDB_ERROR;
        nodep = stmtp->nextp->val.children.firstp->nextp->val.children.firstp;
        for (i = 0; i < suptypec; i++) {
            RDB_expression *exp = RDB_parse_node_expr(nodep, ecp, NULL);
            if (exp == NULL)
                goto error;
            suptypev[i] = RDB_get_type(RDB_expr_var_name(exp), ecp, &interp->txnp->tx);
            if (suptypev[i] == NULL)
                goto error;
            if (nodep->nextp != NULL)
                nodep = nodep->nextp->nextp;
        }
    }

    RDB_init_obj(&nameobj);

    if (stmtp->nextp->nextp->kind == RDB_NODE_TOK) {
        if (stmtp->nextp->nextp->val.token == TOK_UNION) {
            flags = RDB_TYPE_UNION;
            prnodep = stmtp->nextp->nextp->nextp;
        } else {
            flags = 0;
            prnodep = stmtp->nextp->nextp;
        }
        if (prnodep->kind == RDB_NODE_TOK && prnodep->val.token == TOK_ORDERED) {
            flags |= RDB_TYPE_ORDERED;
            prnodep = prnodep->nextp;
        }
    } else {
        flags = 0;
        prnodep = stmtp->nextp->nextp;
    }

    if (RDB_TYPE_UNION & flags) {
        repc = 0;
        repv = NULL;
        constraintp = NULL;
        initexp = NULL;
    } else {
        repc = RDB_parse_nodelist_length(prnodep);
        repv = RDB_alloc(repc * sizeof(RDB_possrep), ecp);
        if (repv == NULL)
            goto error;
        for (i = 0; i < repc; i++) {
            repv[i].compv = NULL;
        }

        nodep = prnodep->val.children.firstp;
        for (i = 0; i < repc; i++) {
            if (parserep_to_rep(nodep, interp, &repv[i], ecp) != RDB_OK)
                goto error;
            nodep = nodep->nextp;
        }

        if (prnodep->nextp->val.token == TOK_CONSTRAINT) {
            constraintp = RDB_parse_node_expr(prnodep->nextp->nextp, ecp,
                    &interp->txnp->tx);
            if (constraintp == NULL)
                goto error;
            initexp = RDB_parse_node_expr(prnodep->nextp->nextp->nextp->nextp, ecp,
                    &interp->txnp->tx);
            if (initexp == NULL)
                goto error;
        } else {
            initexp = RDB_parse_node_expr(prnodep->nextp->nextp, ecp,
                    &interp->txnp->tx);
            if (initexp == NULL)
                goto error;
        }
    }

    /* If we're within PACKAGE, prepend package name */
    if (*RDB_obj_string(&interp->pkg_name) != '\0') {
        if (Duro_package_q_id(&nameobj, RDB_expr_var_name(stmtp->exp), interp, ecp) != RDB_OK)
            goto error;
        namp = RDB_obj_string(&nameobj);
    } else {
        namp = RDB_expr_var_name(stmtp->exp);
    }

    if (RDB_define_subtype(namp, suptypec, suptypev, repc, repv, constraintp,
            initexp, flags, ecp, &interp->txnp->tx) != RDB_OK)
        goto error;

    for (i = 0; i < repc; i++) {
        for (j = 0; j < repv[i].compc; j++) {
            if (!RDB_type_is_scalar(repv[i].compv[j].typ))
                RDB_del_nonscalar_type(repv[i].compv[j].typ, ecp);
        }
        RDB_free(repv[i].compv);
    }
    RDB_free(suptypev);
    RDB_free(repv);
    if (RDB_parse_get_interactive())
        printf("Type %s defined.\n", RDB_expr_var_name(stmtp->exp));
    RDB_destroy_obj(&nameobj, ecp);
    return RDB_OK;

error:
    RDB_free(suptypev);
    if (repv != NULL) {
        for (i = 0; i < repc; i++) {
            if (repv[i].compv != NULL) {
                for (j = 0; j < repv[i].compc; j++) {
                    if (repv[i].compv[j].typ != NULL
                            && !RDB_type_is_scalar(repv[i].compv[j].typ))
                        RDB_del_nonscalar_type(repv[i].compv[j].typ, ecp);
                }
                RDB_free(repv[i].compv);
            }
        }
        RDB_free(repv);
    }
    RDB_destroy_obj(&nameobj, ecp);
    return RDB_ERROR;
}

static int
exec_typedrop(const RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_type *typ;
    RDB_object nameobj;
    const char *namp;

    /*
     * DROP TYPE is not allowed in user-defined operators,
     * to prevent a type used by an operator from being dropped
     * while the operator is being executed
     */
    if (interp->inner_op != NULL) {
        RDB_raise_syntax("DROP TYPE not permitted in user-defined operators",
                ecp);
        return RDB_ERROR;
    }

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&nameobj);

    /* If we're within PACKAGE, prepend package name */
    if (*RDB_obj_string(&interp->pkg_name) != '\0') {
        if (Duro_package_q_id(&nameobj, RDB_expr_var_name(nodep->exp), interp, ecp) != RDB_OK)
            goto error;
        namp = RDB_obj_string(&nameobj);
    } else {
        namp = RDB_expr_var_name(nodep->exp);
    }

    typ = RDB_get_type(namp, ecp, &interp->txnp->tx);
    if (typ == NULL)
        goto error;

    /*
     * Check if a transient variable of that type exists
     */
    if (Duro_type_in_use(interp, typ)) {
        RDB_raise_in_use("unable to drop type because a variable depends on it", ecp);
        goto error;
    }

    if (RDB_drop_typeimpl_ops(typ, ecp, &interp->txnp->tx) != RDB_OK)
        goto error;

    if (RDB_drop_type(RDB_type_name(typ), ecp, &interp->txnp->tx) != RDB_OK)
        goto error;

    if (RDB_parse_get_interactive())
        printf("Type %s dropped.\n", RDB_expr_var_name(nodep->exp));
    return RDB_destroy_obj(&nameobj, ecp);

error:
    RDB_destroy_obj(&nameobj, ecp);
    return RDB_ERROR;
}

static int
exec_typeimpl(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    RDB_type *ityp = NULL;
    RDB_object nameobj;
    const char *namp;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (nodep->nextp->val.token == TOK_AS) {
        Duro_return_info retinfo;

        ityp = Duro_parse_node_to_type_retry(nodep->nextp->nextp, interp, ecp);
        if (ityp == NULL)
            return RDB_ERROR;
        interp->impl_typename = RDB_expr_var_name(nodep->exp);
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->val.children.firstp,
                interp, ecp, &retinfo);
        interp->impl_typename = NULL;
        if (ret != RDB_OK)
            return ret;
    }

    RDB_init_obj(&nameobj);

    if (*RDB_obj_string(&interp->pkg_name) != '\0') {
        if (Duro_package_q_id(&nameobj, RDB_expr_var_name(nodep->exp), interp, ecp) != RDB_OK)
            goto error;
        namp = RDB_obj_string(&nameobj);
    } else {
        namp = RDB_expr_var_name(nodep->exp);
    }

    ret = RDB_implement_type(namp, ityp, RDB_SYS_REP, ecp, &interp->txnp->tx);
    if (ret != RDB_OK) {
        RDB_exec_context ec;
        RDB_type *typ;

        RDB_init_exec_context(&ec);
        typ = RDB_get_type(RDB_expr_var_name(nodep->exp), &ec, &interp->txnp->tx);
        if (typ != NULL) {
            RDB_drop_typeimpl_ops(typ, &ec, &interp->txnp->tx);
        }
        RDB_destroy_exec_context(&ec);
        goto error;
    }
    if (RDB_parse_get_interactive())
        printf("Type %s implemented.\n", RDB_expr_var_name(nodep->exp));
    return RDB_destroy_obj(&nameobj, ecp);

error:
    RDB_destroy_obj(&nameobj, ecp);
    return RDB_ERROR;
}

/* Define read-only operator, prepend package name */
static int
create_ro_op(const char *name, int paramc, RDB_parameter paramv[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const char *sourcep, Duro_interp *interp,
                 RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object opnameobj;

    if (*RDB_obj_string(&interp->pkg_name) == '\0') {
        return RDB_create_ro_op(name, paramc, paramv, rtyp,
                symname != NULL ? libname : NULL,
                symname != NULL ? symname : NULL,
                sourcep, ecp, txp);
    }
    RDB_init_obj(&opnameobj);
    if (Duro_package_q_id(&opnameobj, name, interp, ecp) != RDB_OK)
        goto error;

    if (RDB_create_ro_op(RDB_obj_string(&opnameobj), paramc, paramv, rtyp,
            symname[0] != '\0' ? libname : NULL, symname[0] != '\0' ? symname : NULL,
            sourcep, ecp, txp) != RDB_OK)
        goto error;
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

static int
ro_opdef_extern(const char *opname, RDB_type *rtyp,
        int paramc, RDB_parameter *paramv,
        const char *lang,
        const char *extname, Duro_interp *interp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    Duro_uop_info *creop_infop = Duro_dt_get_creop_info(interp, lang);
    if (creop_infop == NULL) {
        RDB_raise_not_supported("language not supported", ecp);
        return RDB_ERROR;
    }

    return create_ro_op(opname, paramc, paramv, rtyp,
            creop_infop->libname,
            creop_infop->ro_op_symname,
            extname, interp, ecp, txp);
}

/* Define  operator, prepend package name */
static int
create_update_op(const char *name, int paramc, RDB_parameter paramv[],
                 const char *libname, const char *symname,
                 const char *sourcep, Duro_interp *interp,
                 RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object opnameobj;

    if (*RDB_obj_string(&interp->pkg_name) == '\0') {
        return RDB_create_update_op(name, paramc, paramv,
                libname, symname, sourcep, ecp, txp);
    }
    RDB_init_obj(&opnameobj);
    if (RDB_string_to_obj(&opnameobj, RDB_obj_string(&interp->pkg_name), ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&opnameobj, ".", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&opnameobj, name, ecp) != RDB_OK)
        goto error;

    if (RDB_create_update_op(RDB_obj_string(&opnameobj), paramc, paramv,
            libname, symname, sourcep, ecp, txp) != RDB_OK)
        goto error;
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

static int
update_opdef_extern(const char *opname,
        int paramc, RDB_parameter *paramv,
        const char *lang,
        const char *extname, Duro_interp *interp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    Duro_uop_info *creop_infop = Duro_dt_get_creop_info(interp, lang);
    if (creop_infop == NULL) {
        RDB_raise_not_supported("extern: language not supported", ecp);
        return RDB_ERROR;
    }

    return create_update_op(opname, paramc, paramv,
            creop_infop->libname,
            creop_infop->update_op_symname,
            extname, interp, ecp, txp);
}

static int
exec_opdef(RDB_parse_node *parentp, Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    int i;
    RDB_bool is_ro;
    RDB_object code;
    RDB_parse_node *attrnodep;
    RDB_transaction tmp_tx;
    RDB_type *rtyp;
    const char *opname;
    RDB_object opnameobj; /* Only used when the name is modified */
    RDB_object *lwsp;
    RDB_bool is_spec;
    RDB_bool is_extern;
    RDB_bool subtx = RDB_FALSE;
    RDB_parse_node *stmtp = parentp->val.children.firstp->nextp;
    RDB_parameter *paramv = NULL;
    int paramc = ((int) RDB_parse_nodelist_length(stmtp->nextp->nextp) + 1) / 3;

    if (paramc > DURO_MAX_LLEN) {
        RDB_raise_not_supported("too many parameters", ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&code);
    RDB_init_obj(&opnameobj);

    opname = RDB_expr_var_name(stmtp->exp);

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (interp->txnp == NULL) {
        RDB_database *dbp;

        if (interp->envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            goto error;
        }

        dbp = Duro_get_db(interp, ecp);
        if (dbp == NULL)
            goto error;

        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            goto error;
        }
        subtx = RDB_TRUE;
    }

    paramv = RDB_alloc(paramc * sizeof(RDB_parameter), ecp);
    if (paramv == NULL) {
        goto error;
    }
    for (i = 0; i < paramc; i++)
        paramv[i].typ = NULL;

    attrnodep = stmtp->nextp->nextp->val.children.firstp;
    for (i = 0; i < paramc; i++) {
        /* Skip comma */
        if (i > 0)
            attrnodep = attrnodep->nextp;

        paramv[i].typ = RDB_parse_node_to_type(attrnodep->nextp,
                &Duro_get_var_type, interp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        if (paramv[i].typ == NULL)
            goto error;
        attrnodep = attrnodep->nextp->nextp;
    }

    is_ro = (RDB_bool)
            (stmtp->nextp->nextp->nextp->nextp->val.token == TOK_RETURNS);

    /* Strip off leading whitespace and comments, restore it later */
    lwsp = parentp->val.children.firstp->whitecommp;
    parentp->val.children.firstp->whitecommp = NULL;

    if (is_ro) {
        /* Check for EXTERN */
        is_extern = (RDB_bool) (stmtp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->val.token == TOK_EXTERN);
        if (!is_extern) {
            /* Check for emtpy body */
            is_spec = (RDB_bool) (RDB_parse_nodelist_length(stmtp->nextp->nextp->nextp->nextp->nextp->nextp
                    ->nextp->nextp) == 0);
        }
    } else {
        is_extern = (RDB_bool) (stmtp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->val.token
                == TOK_EXTERN);
        if (!is_extern) {
            is_spec = (RDB_bool) (RDB_parse_nodelist_length(stmtp->nextp->nextp->nextp->nextp->nextp->nextp
                    ->nextp->nextp->nextp->nextp) == 0);
        }
    }

    if (!is_extern) {
        if (is_spec) {
            if (RDB_string_to_obj(&code, "", ecp) != RDB_OK)
                goto error;
        } else {
            /*
             * 'Un-parse' the defining code
             */
            if (Duro_parse_node_to_obj_string(&code, parentp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx) != RDB_OK) {
                parentp->val.children.firstp->whitecommp = lwsp;
                goto error;
            }
        }
    }
    parentp->val.children.firstp->whitecommp = lwsp;

    if (is_ro) {
        rtyp = RDB_parse_node_to_type(stmtp->nextp->nextp->nextp->nextp->nextp,
                &Duro_get_var_type, interp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        if (rtyp == NULL)
            goto error;
        if (RDB_type_is_generic(rtyp)) {
            RDB_raise_syntax("generic type not permitted", ecp);
            if (!RDB_type_is_scalar(rtyp))
                RDB_del_nonscalar_type(rtyp, ecp);
            goto error;
        }

        if (interp->impl_typename != NULL) {
            /*
             * We're inside a IMPLEMENT TYPE; ... END IMPLEMENT block.
             * Only selector and getters allowed
             */
            if (strstr(opname, "get_") == opname) {
                /* Prepend operator name with <typename>_ */
                if (RDB_string_to_obj(&opnameobj, interp->impl_typename, ecp) != RDB_OK)
                    goto error;
                if (RDB_append_string(&opnameobj, "_", ecp) != RDB_OK)
                    goto error;
                if (RDB_append_string(&opnameobj, opname, ecp) != RDB_OK)
                    goto error;
                opname = RDB_obj_string(&opnameobj);
            } else if (!RDB_type_is_scalar(rtyp)
                    || strcmp(RDB_type_name(rtyp), interp->impl_typename) != 0) {
                RDB_raise_syntax("invalid operator", ecp);
                goto error;
            }
        }

        if (is_extern) {
            RDB_expression *langexp, *extnamexp;
            langexp = RDB_parse_node_expr(
                    stmtp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp,
                    ecp, interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
            if (langexp == NULL)
                goto error;
            extnamexp = RDB_parse_node_expr(
                    stmtp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp,
                    ecp, interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
            if (extnamexp == NULL)
                goto error;

            ret = ro_opdef_extern(opname, rtyp, paramc, paramv,
                    RDB_obj_string(RDB_expr_obj(langexp)),
                    RDB_obj_string(RDB_expr_obj(extnamexp)),
                    interp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        } else {
            ret = create_ro_op(opname, paramc, paramv, rtyp,
#ifdef _WIN32
                    is_spec ? NULL : "duro",
#else
                    is_spec ? NULL : "libduro",
#endif
                    is_spec ? NULL : "Duro_dt_invoke_ro_op",
                    RDB_obj_string(&code), interp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        }
        if (ret != RDB_OK)
            goto error;
    } else {
        if (interp->impl_typename != NULL) {
            /* Only setters allowed */
            if (strstr(opname, "set_") == opname) {
                /* Prepend operator name with <typename>_ */
                if (RDB_string_to_obj(&opnameobj, interp->impl_typename, ecp) != RDB_OK)
                    goto error;
                if (RDB_append_string(&opnameobj, "_", ecp) != RDB_OK)
                    goto error;
                if (RDB_append_string(&opnameobj, opname, ecp) != RDB_OK)
                    goto error;
                opname = RDB_obj_string(&opnameobj);
            } else {
                RDB_raise_syntax("invalid operator", ecp);
                goto error;
            }
        }

        for (i = 0; i < paramc; i++)
            paramv[i].update = RDB_FALSE;

        /*
         * Set paramv[].update
         */
        attrnodep = stmtp->nextp->nextp->nextp->nextp->nextp->nextp->val.children.firstp;
        while (attrnodep != NULL) {
            const char *updname = RDB_expr_var_name(attrnodep->exp);
            RDB_parse_node *paramnodep = stmtp->nextp->nextp->val.children.firstp;

            for (i = 0; i < paramc; i++) {
                if (strcmp(RDB_expr_var_name(paramnodep->exp), updname) == 0) {
                    paramv[i].update = RDB_TRUE;
                    break;
                }
                paramnodep = paramnodep->nextp->nextp;
                if (paramnodep != NULL)
                    paramnodep = paramnodep->nextp;
            }
            if (i == paramc) {
                RDB_raise_invalid_argument("invalid update parameter", ecp);
                goto error;
            }
            attrnodep = attrnodep->nextp;
            if (attrnodep != NULL)
                attrnodep = attrnodep->nextp;
        }

        /* Check for EXTERN ... */
        if (is_extern) {
            RDB_expression *langexp, *extnamexp;
            langexp = RDB_parse_node_expr(
                    stmtp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp,
                    ecp, interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
            if (langexp == NULL)
                goto error;
            extnamexp = RDB_parse_node_expr(
                    stmtp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp,
                    ecp, interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
            if (extnamexp == NULL)
                goto error;

            ret = update_opdef_extern(opname, paramc, paramv,
                    RDB_obj_string(RDB_expr_obj(langexp)),
                    RDB_obj_string(RDB_expr_obj(extnamexp)),
                    interp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        } else {
            ret = create_update_op(opname,
                    paramc, paramv,
#ifdef _WIN32
                    is_spec ? NULL : "duro",
#else
                    is_spec ? NULL : "libduro",
#endif
                    is_spec ? NULL : "Duro_dt_invoke_update_op",
                    RDB_obj_string(&code), interp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        }
        if (ret != RDB_OK)
            goto error;
    }

    for (i = 0; i < paramc; i++) {
        if (!RDB_type_is_scalar(paramv[i].typ))
            RDB_del_nonscalar_type(paramv[i].typ, ecp);
    }
    RDB_free(paramv);
    RDB_destroy_obj(&code, ecp);
    if (subtx) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf(is_ro ? "Read-only operator %s created.\n" : "Update operator %s created.\n", opname);
    RDB_destroy_obj(&opnameobj, ecp);
    return ret;

error:
    if (subtx)
        RDB_rollback(ecp, &tmp_tx);
    if (paramv != NULL) {
        for (i = 0; i < paramc; i++) {
            if (paramv[i].typ != NULL && !RDB_type_is_scalar(paramv[i].typ))
                RDB_del_nonscalar_type(paramv[i].typ, ecp);
        }
        RDB_free(paramv);
    }
    RDB_destroy_obj(&code, ecp);
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

static int
exec_opdrop(const RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object nameobj;
    const char *opname;
    RDB_transaction tmp_tx;

    /*
     * DROP OPERATOR is not allowed in user-defined operators,
     * to prevent an operator from being dropped while it is executed
     */
    if (interp->inner_op != NULL) {
        RDB_raise_syntax("DROP OPERATOR not permitted in user-defined operators",
                ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&nameobj);

    /* If we're within PACKAGE, prepend package name */
    if (*RDB_obj_string(&interp->pkg_name) != '\0') {
        if (Duro_package_q_id(&nameobj, RDB_expr_var_name(nodep->exp), interp, ecp) != RDB_OK)
            goto error;
        opname = RDB_obj_string(&nameobj);
    } else {
        opname = RDB_expr_var_name(nodep->exp);
    }

    /*
     * If a transaction is not active, start transaction if a database environment
     * is available
     */
    if (interp->txnp == NULL) {
        RDB_database *dbp;

        if (interp->envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            goto error;
        }

        dbp = Duro_get_db(interp, ecp);
        if (dbp == NULL)
            goto error;

        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            goto error;
        }
    }

    if (RDB_drop_op(opname, ecp,
            interp->txnp == NULL ? &tmp_tx : &interp->txnp->tx) != RDB_OK) {
        if (interp->txnp == NULL)
            RDB_rollback(ecp, &tmp_tx);
        goto error;
    }

    if (interp->txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Operator %s dropped.\n", opname);

    RDB_destroy_obj(&nameobj, ecp);
    return ret;

error:
    RDB_destroy_obj(&nameobj, ecp);
    return RDB_ERROR;
}

static int
exec_return(RDB_parse_node *stmtp, Duro_interp *interp, RDB_exec_context *ecp,
        Duro_return_info *retinfop)
{
    if (stmtp->kind != RDB_NODE_TOK) {
        RDB_expression *retexp;
        RDB_type *rtyp;

        if (retinfop == NULL) {
            RDB_raise_syntax("invalid RETURN", ecp);
            return RDB_ERROR;
        }

        retexp = RDB_parse_node_expr(stmtp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (retexp == NULL)
            return RDB_ERROR;

        /*
         * Typecheck
         */
        rtyp = RDB_expr_type(retexp, &Duro_get_var_type, interp,
                    interp->envp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);

        if (rtyp == NULL)
            return RDB_ERROR;
        if (!RDB_type_equals(rtyp, retinfop->typ)) {
            RDB_raise_type_mismatch("invalid return type", ecp);
            return RDB_ERROR;
        }

        /*
         * Evaluate expression
         */
        if (RDB_evaluate(retexp, &Duro_get_var, interp, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL,
                retinfop->objp) != RDB_OK)
            return RDB_ERROR;
    } else {
        if (retinfop != NULL) {
            RDB_raise_invalid_argument("return argument expected", ecp);
            return RDB_ERROR;
        }
    }
    return DURO_RETURN;
}

static void
free_opdata(RDB_operator *op)
{
    RDB_exec_context ec;
    Duro_op_data *opdatap = RDB_operator_u_data(op);

    /* Initialize temporary execution context */
    RDB_init_exec_context(&ec);

    /* Delete code */
    RDB_parse_del_node(opdatap->rootp, &ec);

    RDB_destroy_exec_context(&ec);

    RDB_free(opdatap->argnamev);
    RDB_free(opdatap);
}

int
Duro_dt_invoke_ro_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int i;
    int ret;
    varmap_node vars;
    RDB_parse_node *codestmtp, *attrnodep;
    Duro_op_data *opdatap;
    Duro_return_info retinfo;
    varmap_node *ovarmapp;
    int isselector;
    RDB_type *getter_utyp = NULL;
    RDB_operator *parent_op;
    RDB_type *stored_argtypv[DURO_MAX_LLEN];
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");

    if (interp->interrupted) {
        interp->interrupted = 0;
        RDB_raise_system("interrupted", ecp);
        return RDB_ERROR;
    }

    /* Try to get cached statements */
    opdatap = RDB_operator_u_data(op);
    if (opdatap == NULL) {
        /*
         * Not available - parse code
         */
        char **argnamev = RDB_alloc(argc * sizeof(char *), ecp);
        if (argnamev == NULL)
            return RDB_ERROR;

        codestmtp = RDB_parse_stmt_string(RDB_operator_source(op), ecp);
        if (codestmtp == NULL) {
            RDB_free(argnamev);
            return RDB_ERROR;
        }

        attrnodep = codestmtp->val.children.firstp->nextp->nextp->nextp->val.children.firstp;
        for (i = 0; i < argc; i++) {
            /* Skip comma */
            if (i > 0)
                attrnodep = attrnodep->nextp;
            argnamev[i] = (char *) RDB_expr_var_name(RDB_parse_node_expr(attrnodep, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : NULL));
            attrnodep = attrnodep->nextp->nextp;
        }

        opdatap = RDB_alloc(sizeof(Duro_op_data), ecp);
        if (opdatap == NULL) {
            RDB_free(argnamev);
            return RDB_ERROR;
        }
        opdatap->rootp = codestmtp;
        opdatap->stmtlistp = codestmtp->val.children.firstp->nextp->nextp->nextp->nextp
                ->nextp->nextp->nextp->nextp->nextp->val.children.firstp;
        opdatap->argnamec = argc;
        opdatap->argnamev = argnamev;

        RDB_set_operator_u_data(op, opdatap);
        RDB_set_op_cleanup_fn(op, &free_opdata);
    }

    Duro_init_varmap(&vars.map, 256);
    vars.parentp = NULL;
    ovarmapp = Duro_set_current_varmap(interp, &vars);

    for (i = 0; i < argc; i++) {
        if (Duro_varmap_put(&vars.map, opdatap->argnamev[i], argv[i], DURO_VAR_CONST,
                ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    retinfo.objp = retvalp;
    retinfo.typ = RDB_return_type(op);

    /*
     * Selectors and getters of user-defined types
     * require special treatment, because they are accessing
     * the actual rep
     */

    /*
     * If the operator is a selector, the type of the return expression
     * is the type that is used as the actual representation
     */
    isselector = RDB_is_selector(op)
            && (retinfo.typ->def.scalar.arep != NULL);
    if (isselector) {
        retinfo.typ = retinfo.typ->def.scalar.arep;
    }

    /*
     * If the operator is a getter, set the argument type
     * to the type that is used as the actual representation
     */
    if (RDB_is_getter(op)
            && (RDB_obj_type(argv[0])->def.scalar.arep != NULL)) {
        getter_utyp = RDB_obj_type(argv[0]);
        RDB_obj_set_typeinfo(argv[0], getter_utyp->def.scalar.arep);
    }

    parent_op = interp->inner_op;
    interp->inner_op = op;
    interp->err_line = -1;

    /*
     * If the argument type is scalar and differs from the parameter type,
     * set argument type to the parameter type and restore it afterwards
     */
    for (i = 0; i < argc; i++) {
        RDB_type *argtyp = RDB_obj_type(argv[i]);
        RDB_type *paramtyp = RDB_get_parameter(op, i)->typ;
        if (argtyp != NULL && RDB_type_is_scalar(argtyp)
                && argtyp != paramtyp && (i > 0 || getter_utyp == NULL)) {
            stored_argtypv[i] = argtyp;
            RDB_obj_set_typeinfo(argv[i], paramtyp);
        } else {
            stored_argtypv[i] = NULL;
        }
    }

    ret = exec_stmts(opdatap->stmtlistp, interp, ecp, &retinfo);
    for (i = 0; i < argc; i++) {
        if (stored_argtypv[i] != NULL) {
            RDB_obj_set_typeinfo(argv[i], stored_argtypv[i]);
        }
    }
    interp->inner_op = parent_op;

    /* Set type of return value to the user-defined type */
    if (isselector) {
        RDB_obj_set_typeinfo(retinfo.objp, RDB_operator_type(op));
    }

    if (getter_utyp != NULL) {
        RDB_obj_set_typeinfo(argv[0], getter_utyp);
    }

    Duro_set_current_varmap(interp, ovarmapp);
    Duro_destroy_varmap(&vars.map);

    switch (ret) {
    case RDB_OK:
        RDB_raise_syntax("end of operator reached without RETURN", ecp);
        ret = RDB_ERROR;
        break;
    case DURO_LEAVE:
        RDB_raise_syntax("unmatched LEAVE", ecp);
        ret = RDB_ERROR;
        break;
    case DURO_RETURN:
        ret = RDB_OK;
        break;
    }
    if (ret == RDB_ERROR) {
        if (interp->err_opname == NULL) {
            interp->err_opname = RDB_dup_str(RDB_operator_name(op));
            if (interp->err_opname == NULL) {
                RDB_raise_no_memory(ecp);
            }
        }
    }

    return ret;
}

int
Duro_dt_invoke_update_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    varmap_node vars;
    RDB_parse_node *codestmtp;
    RDB_parse_node *attrnodep;
    varmap_node *ovarmapp;
    Duro_op_data *opdatap;
    RDB_operator *parent_op;
    RDB_type *stored_argtypv[DURO_MAX_LLEN];
    RDB_type *setter_utyp = NULL;
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");

    if (interp->interrupted) {
        interp->interrupted = 0;
        RDB_raise_system("interrupted", ecp);
        return RDB_ERROR;
    }

    /* Try to get cached statements */
    opdatap = RDB_operator_u_data(op);
    if (opdatap == NULL) {
        char **argnamev = RDB_alloc(argc * sizeof(char *), ecp);
        if (argnamev == NULL)
            return RDB_ERROR;

        /*
         * Not available - parse code
         */
        codestmtp = RDB_parse_stmt_string(RDB_operator_source(op), ecp);
        if (codestmtp == NULL) {
            RDB_free(argnamev);
            return RDB_ERROR;
        }

        attrnodep = codestmtp->val.children.firstp->nextp->nextp->nextp->val.children.firstp;
        for (i = 0; i < argc; i++) {
            /* Skip comma */
            if (i > 0)
                attrnodep = attrnodep->nextp;
            argnamev[i] = (char *) RDB_expr_var_name(RDB_parse_node_expr(attrnodep, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : NULL));
            attrnodep = attrnodep->nextp->nextp;
        }

        opdatap = RDB_alloc(sizeof(Duro_op_data), ecp);
        if (opdatap == NULL) {
            RDB_free(argnamev);
            return RDB_ERROR;
        }
        opdatap->rootp = codestmtp;
        opdatap->stmtlistp = codestmtp->val.children.firstp->nextp->nextp->nextp->nextp
                    ->nextp->nextp->nextp->nextp->nextp->nextp->nextp->val.children.firstp;
        opdatap->argnamec = argc;
        opdatap->argnamev = argnamev;

        RDB_set_operator_u_data(op, opdatap);
        RDB_set_op_cleanup_fn(op, &free_opdata);
    }

    Duro_init_varmap(&vars.map, 256);
    vars.parentp = NULL;
    ovarmapp = Duro_set_current_varmap(interp, &vars);

    for (i = 0; i < argc; i++) {
        if (Duro_varmap_put(&vars.map, opdatap->argnamev[i],
                argv[i], RDB_get_parameter(op, i)->update ? 0 : DURO_VAR_CONST,
                        ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    /*
     * If the operator is a setter, set the type of the first argument
     * to the type that is used as the actual representation
     */
    if (RDB_is_setter(op)
            && (RDB_obj_type(argv[0])->def.scalar.arep != NULL)) {
        setter_utyp = RDB_obj_type(argv[0]);
        RDB_obj_set_typeinfo(argv[0], setter_utyp->def.scalar.arep);
    }

    parent_op = interp->inner_op;
    interp->inner_op = op;

    interp->err_line = -1;

    /*
     * If the argument type is scalar and differs from parameter type,
     * set argument type to parameter type
     */
    for (i = 0; i < argc; i++) {
        RDB_type *argtyp = RDB_obj_type(argv[i]);
        RDB_type *paramtyp = RDB_get_parameter(op, i)->typ;
        if (argtyp != NULL && RDB_type_is_scalar(argtyp)
                && argtyp != paramtyp && (i > 0 || setter_utyp == NULL)) {
            stored_argtypv[i] = argtyp;
            RDB_obj_set_typeinfo(argv[i], paramtyp);
        } else {
            stored_argtypv[i] = NULL;
        }
    }

    ret = exec_stmts(opdatap->stmtlistp, interp, ecp, NULL);

    /* Restore argument types */
    for (i = 0; i < argc; i++) {
        if (stored_argtypv[i] != NULL) {
            RDB_obj_set_typeinfo(argv[i], stored_argtypv[i]);
        }
    }
    interp->inner_op = parent_op;

    if (setter_utyp != NULL) {
        RDB_obj_set_typeinfo(argv[0], setter_utyp);
    }

    Duro_set_current_varmap(interp, ovarmapp);
    Duro_destroy_varmap(&vars.map);

    /* Catch LEAVE */
    if (ret == DURO_LEAVE) {
        RDB_raise_syntax("unmatched LEAVE", ecp);
        ret = RDB_ERROR;
    }
    if (ret == RDB_ERROR) {
        if (interp->err_opname == NULL) {
            interp->err_opname = RDB_dup_str(RDB_operator_name(op));
            if (interp->err_opname == NULL) {
                RDB_raise_no_memory(ecp);
            }
        }
    }

    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

static int
exec_raise(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_object errobj;
    RDB_object *errp;

    exp = RDB_parse_node_expr(nodep, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (exp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&errobj);
    if (Duro_evaluate(exp, interp, ecp, &errobj) != RDB_OK) {
        goto error;
    }
    errp = RDB_raise_err(ecp);
    if (errp == NULL) {
        RDB_raise_internal("RDB_raise_err() failed", ecp);
        goto error;
    }
    RDB_copy_obj(errp, &errobj, ecp);

error:
    RDB_destroy_obj(&errobj, ecp);
    if (RDB_get_err(ecp) == NULL) {
        RDB_raise_internal("missing error", ecp);
    }
    return RDB_ERROR;
}

static int
exec_catch(const RDB_parse_node *catchp, const RDB_type *errtyp,
        Duro_interp *interp, RDB_exec_context *ecp, Duro_return_info *retinfop)
{
    int ret;
    RDB_object *objp;

    if (Duro_add_varmap(interp, ecp) != RDB_OK)
        return RDB_ERROR;

    objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        goto error;
    }
    RDB_init_obj(objp);

    /*
     * Create and initialize local variable
     */
    if (Duro_put_var(RDB_expr_var_name(catchp->exp), objp, interp, ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_copy_obj(objp, RDB_get_err(ecp), ecp) != RDB_OK) {
        goto error;
    }

    RDB_clear_err(ecp);
    interp->err_line = -1;

    ret = exec_stmts(catchp->nextp->nextp->kind == RDB_NODE_TOK ?
            catchp->nextp->nextp->nextp->val.children.firstp :
            catchp->nextp->nextp->val.children.firstp,
            interp, ecp, retinfop);
    Duro_remove_varmap(interp);
    return ret;

error:
    Duro_remove_varmap(interp);
    return RDB_ERROR;
}

static int
exec_try(const RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp, Duro_return_info *retinfop)
{
    int ret;
    RDB_parse_node *catchp;

    /*
     * Execute try body
     */
    if (Duro_add_varmap(interp, ecp) != RDB_OK)
        return RDB_ERROR;
    ret = exec_stmts(nodep->val.children.firstp, interp, ecp, retinfop);
    Duro_remove_varmap(interp);

    if (ret == RDB_ERROR) {
        /*
         * Try to find a catch clause to handle the error
         */
        RDB_object *errp = RDB_get_err(ecp);
        RDB_type *errtyp = RDB_obj_type(errp);

        if (errtyp != NULL) {
            RDB_type *typ;

            catchp = nodep->nextp->val.children.firstp;

            while (catchp != NULL) {
                if (catchp->val.children.firstp->nextp->nextp->kind
                        != RDB_NODE_TOK) {
                    /* Catch clause with type */
                    typ = Duro_parse_node_to_type_retry(
                            catchp->val.children.firstp->nextp->nextp,
                            interp, ecp);
                    if (typ == NULL)
                        return RDB_ERROR;
                    if (RDB_type_equals(errtyp, typ)) {
                        return exec_catch(catchp->val.children.firstp->nextp,
                                typ, interp, ecp, retinfop);
                    }
                } else {
                    /* Catch clause without type */
                    return exec_catch(catchp->val.children.firstp->nextp,
                            errtyp, interp, ecp, retinfop);
                }
                catchp = catchp->nextp;
            }
        }
    }
    return ret;
}

/* Implements both PACKAGE .. END PACKAGE and IMPLEMENT PACKAGE .. END IMPLEMENT. */
static int
exec_packagedef(RDB_parse_node *stmtp, Duro_interp *interp, RDB_exec_context *ecp,
        Duro_return_info *retinfop)
{
    int ret;
    RDB_object oldpkgname;
    size_t olen = strlen(RDB_obj_string(&interp->pkg_name));

    RDB_init_obj(&oldpkgname);

    if (olen > 0) {
        RDB_append_string(&interp->pkg_name, ".", ecp);
    }
    RDB_append_string(&interp->pkg_name, RDB_expr_var_name(stmtp->exp), ecp);

    ret = exec_stmts(stmtp->nextp->nextp->val.children.firstp,
            interp, ecp, retinfop);

    if (RDB_string_n_to_obj(&oldpkgname, RDB_obj_string(&interp->pkg_name),
            olen, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (RDB_copy_obj(&interp->pkg_name, &oldpkgname, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    RDB_destroy_obj(&oldpkgname, ecp);
    return ret;
}

static int
exec_constrdef(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;
    const char *constrname = RDB_expr_var_name(nodep->exp);
    RDB_expression *constrexp;

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (interp->txnp == NULL) {
        RDB_database *dbp = Duro_get_db(interp, ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    constrexp = RDB_parse_node_expr(nodep->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (constrexp == NULL)
        goto error;
    constrexp = RDB_dup_expr(constrexp, ecp);
    if (constrexp == NULL)
        goto error;
    ret = RDB_create_constraint(constrname, constrexp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (interp->txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Constraint %s created.\n", constrname);
    return ret;

error:
    if (constrexp != NULL)
        RDB_del_expr(constrexp, ecp);
    if (interp->txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_indexdef(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int ret;
    int i;
    const char *indexname = RDB_expr_var_name(nodep->exp);
    RDB_object *tbp;
    int idxcompc = (RDB_parse_nodelist_length(nodep->nextp->nextp->nextp) + 1) / 2;
    RDB_seq_item *idxcompv;
    RDB_parse_node *attrnodep;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    tbp = RDB_get_table(RDB_expr_var_name(nodep->nextp->exp),
            ecp, &interp->txnp->tx);
    if (tbp == NULL) {
        return RDB_ERROR;
    }

    idxcompv = RDB_alloc(sizeof(RDB_seq_item) * idxcompc, ecp);
    if (idxcompv == NULL)
        return RDB_ERROR;

    attrnodep = nodep->nextp->nextp->nextp->val.children.firstp;
    i = 0;
    for(;;) {
        idxcompv[i].attrname = (char *) RDB_expr_var_name(attrnodep->exp);
        idxcompv[i].asc = RDB_TRUE;
        attrnodep = attrnodep->nextp;
        if (attrnodep == NULL)
            break;
        /* Skip comma */
        attrnodep = attrnodep->nextp;
        i++;
    }

    ret = RDB_create_table_index(indexname, tbp, idxcompc,
            idxcompv, RDB_ORDERED, ecp, &interp->txnp->tx);
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Index %s created.\n", indexname);
    RDB_free(idxcompv);
    return ret;
}

static int
exec_constrdrop(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;
    const char *constrname = RDB_expr_var_name(nodep->exp);

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (interp->txnp == NULL) {
        RDB_database *dbp = Duro_get_db(interp, ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    ret = RDB_drop_constraint(constrname, ecp, interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (interp->txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Constraint %s dropped.\n", constrname);
    return ret;

error:
    if (interp->txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_indexdrop(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int ret;
    const char *indexname = RDB_expr_var_name(nodep->exp);

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    ret = RDB_drop_table_index(indexname, ecp, &interp->txnp->tx);
    if (ret != RDB_OK)
        goto error;

    if (RDB_parse_get_interactive())
        printf("Index %s dropped.\n", indexname);
    return ret;

error:
    return RDB_ERROR;
}

static int
exec_pkgdrop(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_object *pkgname;
    RDB_object *ops;
    RDB_object *types;
    RDB_object *ptables;
    RDB_parse_node *np;
    int i, len;
    YY_BUFFER_STATE oldbuf;
    int lineno = yylineno;

    int pbuf_was_valid = RDB_parse_buffer_valid;
    RDB_bool was_interactive = RDB_parse_get_interactive();

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (Duro_add_varmap(interp, ecp) != RDB_OK)
        return RDB_ERROR;

    /* If the parse buffer is valid, save it */
    if (RDB_parse_buffer_valid) {
        if (RDB_parse_get_interactive())
            yy_delete_buffer(RDB_parse_buffer);
        else
            oldbuf = RDB_parse_buffer;
    }

    yylineno = 1;

    if (Duro_dt_execute_str("var pkgname string;", interp, ecp) != RDB_OK)
        goto error;

    pkgname = Duro_lookup_transient_var(interp, "pkgname");

    if (RDB_string_to_obj(pkgname,
            RDB_expr_var_name(nodep->val.children.firstp->exp), ecp) != RDB_OK)
        goto error;
    np = nodep->val.children.firstp->nextp;
    while (np != NULL) {
        if (RDB_append_string(pkgname, ".", ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(pkgname,
                RDB_expr_var_name(np->nextp->exp), ecp) != RDB_OK)
            goto error;
        np = np->nextp->nextp;
    }

    if (Duro_dt_execute_str("var types array tuple { typename string };"
            "load types from sys_types where typename like pkgname || '.*' { typename } order();",
            interp, ecp) != RDB_OK) {
        goto error;
    }

    types = Duro_lookup_transient_var(interp, "types");
    len = (int) RDB_array_length(types, ecp);
    for (i = 0; i < len; i++) {
        RDB_object *elem = RDB_array_get(types, (RDB_int) i, ecp);
        if (elem == NULL)
            goto error;
        if (RDB_drop_type(RDB_tuple_get_string(elem, "typename"), ecp, &interp->txnp->tx)
                != RDB_OK)
            goto error;
    }

    if (Duro_dt_execute_str("var ops array tuple { opname string };"
            "load ops from sys_ro_ops where opname like pkgname || '.*' { opname }"
                    "union sys_upd_ops where opname like pkgname || '.*' { opname } order();",
            interp, ecp) != RDB_OK) {
        goto error;
    }

    ops = Duro_lookup_transient_var(interp, "ops");
    len = (int) RDB_array_length(ops, ecp);
    for (i = 0; i < len; i++) {
        RDB_object *elem = RDB_array_get(ops, (RDB_int) i, ecp);
        if (elem == NULL)
            goto error;
        if (RDB_drop_op(RDB_tuple_get_string(elem, "opname"), ecp, &interp->txnp->tx)
                != RDB_OK)
            goto error;
    }

    if (Duro_dt_execute_str("var ptables array tuple { tablename string };"
            "load ptables from sys_ptables where tablename like pkgname || '.*' { tablename } order();",
            interp, ecp) != RDB_OK) {
        goto error;
    }

    ptables = Duro_lookup_transient_var(interp, "ptables");
    len = (int) RDB_array_length(ptables, ecp);
    for (i = 0; i < len; i++) {
        RDB_object *elem = RDB_array_get(ptables, (RDB_int) i, ecp);
        if (elem == NULL)
            goto error;
        if (RDB_drop_table_by_name(RDB_tuple_get_string(elem, "tablename"), ecp, &interp->txnp->tx)
                != RDB_OK)
            goto error;
    }

    Duro_remove_varmap(interp);
    RDB_parse_set_interactive(was_interactive);
    /* If the parse buffer was valid, restore it */
    if (pbuf_was_valid) {
        if (RDB_parse_get_interactive()) {
            RDB_parse_buffer = yy_scan_string("");
        } else {
            yy_switch_to_buffer(oldbuf);
            RDB_parse_buffer = oldbuf;
        }
    }
    yylineno = lineno;
    return RDB_OK;

error:
    Duro_remove_varmap(interp);
    RDB_parse_set_interactive(was_interactive);
    /* If the parse buffer was valid, restore it */
    if (pbuf_was_valid) {
        if (RDB_parse_get_interactive()) {
            RDB_parse_buffer = yy_scan_string("");
        } else {
            yy_switch_to_buffer(oldbuf);
            RDB_parse_buffer = oldbuf;
        }
    }
    yylineno = lineno;
    return RDB_ERROR;
}

static int
exec_map(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_object idobj;
    const char *tbname;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&idobj);

    if (*RDB_obj_string(&interp->pkg_name) != '\0') {
        if (Duro_package_q_id(&idobj, RDB_expr_var_name(nodep->exp), interp, ecp)
                != RDB_OK) {
            goto error;
        }
        tbname = RDB_obj_string(&idobj);
    } else {
        tbname = RDB_expr_var_name(nodep->exp);
    }

    exp = RDB_parse_node_expr(nodep->nextp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (exp == NULL)
        goto error;

    if (RDB_map_public_table(tbname, exp, ecp, &interp->txnp->tx) != RDB_OK)
        goto error;

    RDB_destroy_obj(&idobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&idobj, ecp);
    return RDB_ERROR;
}

static int
exec_leave(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    if (nodep->kind == RDB_NODE_TOK) {
        interp->leave_targetname = NULL;
    } else {
        interp->leave_targetname = RDB_expr_var_name(nodep->exp);
    }
    return DURO_LEAVE;
}

static RDB_operator *
interp_get_op(Duro_interp *interp, const char *opname, int argc,
        RDB_object *argpv[], RDB_exec_context *ecp) {
    RDB_operator *op = RDB_get_op_by_args(&interp->sys_upd_op_map, opname, argc, argpv, ecp);
    if (op == NULL) {
        RDB_bool type_mismatch =
                (RDB_obj_type(RDB_get_err(ecp)) == &RDB_TYPE_MISMATCH_ERROR);

        op = RDB_get_update_op_by_args(opname, argc, argpv, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
        /*
         * If the operator was not found and no transaction is running start a transaction
         * for reading the operator from the catalog only
         */
        if (op == NULL && RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR
                && interp->txnp == NULL) {
            RDB_transaction tx;
            RDB_database *dbp = Duro_get_db(interp, ecp);
            if (dbp == NULL) {
                if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
                    RDB_raise_operator_not_found("no database", ecp);
                }
                return NULL;
            }
            if (RDB_begin_tx(ecp, &tx, dbp, NULL) != RDB_OK)
                return NULL;
            op = RDB_get_update_op_by_args(opname, argc, argpv, NULL, ecp, &tx);
            if (RDB_commit(ecp, &tx) != RDB_OK)
                return NULL;
        }
        if (op == NULL) {
            /*
             * If the error is operator_not_found_error but the
             * previous lookup returned type_mismatch_error,
             * raise type_mismatch_error
             */
            if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR
                    && type_mismatch) {
                RDB_raise_type_mismatch(opname, ecp);
            }
            return NULL;
        }
    }
    return op;
}

/*
 * Call update operator. nodep points to the operator name token.
 */
static int
exec_call(const RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_object *argpv[DURO_MAX_LLEN];
    RDB_object argv[DURO_MAX_LLEN];
    int argflags[DURO_MAX_LLEN];
    RDB_operator *op;
    RDB_parameter *paramp;
    const char *varname;
    const char *opname;
    RDB_object qop_nameobj;
    int i;
    RDB_parse_node *argp = nodep->nextp->nextp->val.children.firstp;
    int argc = 0;

    RDB_init_obj(&qop_nameobj);

    if (nodep->kind == RDB_NODE_INNER) {
        if (RDB_parse_node_pkgname(nodep->val.children.firstp, &qop_nameobj, ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&qop_nameobj, ".", ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&qop_nameobj,
                RDB_expr_var_name(nodep->val.children.firstp->nextp->nextp->exp),
                ecp) != RDB_OK)
            goto error;

        opname = RDB_obj_string(&qop_nameobj);
    } else {
        opname = RDB_expr_var_name(nodep->exp);
    }

    /*
     * Evaluate arguments
     */
    while (argp != NULL) {
        if (argc > 0)
            argp = argp->nextp;
        exp = RDB_parse_node_expr(argp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (exp == NULL)
            goto error;
        varname = RDB_expr_var_name(exp);
        if (varname != NULL) {
            /*
             * If the expression is a variable, look it up
             * (If the parameter is not an update parameter,
             * the callee must not modify the variable)
             */
            argpv[argc] = Duro_lookup_sym(varname, interp, &argflags[argc], ecp);
            if (argpv[argc] == NULL) {
                goto error;
            }
        } else {
            if (RDB_expr_is_table_ref(exp)) {
                /* Special handling of table refs */
                argpv[argc] = RDB_expr_obj(exp);
            } else {
                RDB_init_obj(&argv[argc]);
                if (RDB_evaluate(exp, &Duro_get_var, interp, interp->envp, ecp,
                        interp->txnp != NULL ? &interp->txnp->tx : NULL, &argv[argc]) != RDB_OK) {
                    RDB_destroy_obj(&argv[argc], ecp);
                    goto error;
                }
                /* Set type if missing */
                if (RDB_obj_type(&argv[argc]) == NULL) {
                    RDB_type *typ = RDB_expr_type(exp, &Duro_get_var_type,
                            interp, interp->envp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
                    if (typ == NULL) {
                        RDB_destroy_obj(&argv[argc], ecp);
                        goto error;
                    }
                    RDB_obj_set_typeinfo(&argv[argc], typ);
                }
                argpv[argc] = &argv[argc];
            }
            argflags[argc] = 0;
        }

        argp = argp->nextp;
        argc++;
    }

    /*
     * Get operator
     */
    op = interp_get_op(interp, opname, argc, argpv, ecp);
    if (op == NULL) {
        const char *calling_opname;
        const char *ldotpos;
        RDB_object opnameobj;

        /*
         * If not found and we're inside an operator that belongs to a package,
         * try again with the package name prepended
         */
        if (interp->inner_op == NULL
                || RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR) {
            goto error;
        }
        calling_opname = RDB_operator_name(interp->inner_op);
        ldotpos = strrchr(calling_opname, '.');
        if (ldotpos == NULL)
            goto error;

        RDB_init_obj(&opnameobj);
        if (RDB_string_n_to_obj(&opnameobj, calling_opname,
                ldotpos - calling_opname + 1, ecp) != RDB_OK) {
            RDB_destroy_obj(&opnameobj, ecp);
            goto error;
        }
        if (RDB_append_string(&opnameobj, opname, ecp) != RDB_OK) {
            RDB_destroy_obj(&opnameobj, ecp);
            goto error;
        }
        op = interp_get_op(interp, RDB_obj_string(&opnameobj),
                    argc, argpv, ecp);
        RDB_destroy_obj(&opnameobj, ecp);
        if (op == NULL)
            goto error;
    }

    for (i = 0; i < argc; i++) {
        paramp = RDB_get_parameter(op, i);
        /*
         * paramp may be NULL for n-ary operators
         */
        if (paramp != NULL && paramp->update) {
            /*
             * If it's an update argument and the argument is not a variable,
             * raise an error
             */
            if (argpv[i] == &argv[i]) {
                RDB_raise_invalid_argument(
                        "update argument must be a variable", ecp);
                goto error;
            } else if (DURO_VAR_CONST & argflags[i]) {
                RDB_raise_invalid_argument("constant not allowed", ecp);
                goto error;
            }
        }
    }

    /* Invoke function */
    if (RDB_call_update_op(op, argc, argpv, ecp, interp->txnp != NULL ?
            &interp->txnp->tx : NULL) != RDB_OK)
        goto error;

    for (i = 0; i < argc; i++) {
        if (argpv[i] == &argv[i])
            RDB_destroy_obj(&argv[i], ecp);
    }
    RDB_destroy_obj(&qop_nameobj, ecp);
    return RDB_OK;

error:
    for (i = 0; i < argc; i++) {
        if (argpv[i] == &argv[i])
            RDB_destroy_obj(&argv[i], ecp);
    }
    RDB_destroy_obj(&qop_nameobj, ecp);
    return RDB_ERROR;
}

static int
exec_explain(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    RDB_expression *exp;
    RDB_object strobj;
    int seqitc;
    RDB_expression *resexp = NULL;
    RDB_seq_item *seqitv = NULL;
    RDB_expression *optexp = NULL;
    RDB_transaction *txp = interp->txnp != NULL ? &interp->txnp->tx : NULL;

    RDB_init_obj(&strobj);

    exp = RDB_parse_node_expr(nodep, ecp, txp);
    if (exp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    seqitc = RDB_parse_nodelist_length(nodep->nextp->nextp->nextp);
    if (seqitc > 0) {
        seqitv = RDB_alloc(sizeof(RDB_seq_item) * seqitc, ecp);
        if (seqitv == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    }

    /* Resolve local variables */
    resexp = RDB_expr_resolve_varnames(exp, &Duro_get_var, interp, ecp, txp);
    if (resexp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    ret = Duro_nodes_to_seqitv(seqitv,
            nodep->nextp->nextp->nextp->val.children.firstp, interp, ecp);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    /* Optimize */
    optexp = RDB_optimize_expr(resexp, seqitc, seqitv, NULL, ecp, txp);
    if (optexp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* Convert tree to string */
    ret = RDB_expr_to_str(&strobj, optexp, ecp, txp, RDB_SHOW_INDEX);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    ret = puts(RDB_obj_string(&strobj));
    if (ret == EOF) {
        RDB_handle_errcode(errno, ecp, txp);
    } else {
        ret = RDB_OK;
    }
    fflush(stdout);

cleanup:
    if (seqitv != NULL)
        RDB_free(seqitv);
    if (optexp != NULL) {
        RDB_del_expr(optexp, ecp);
    }
    if (resexp != NULL) {
        RDB_del_expr(resexp, ecp);
    }
    RDB_destroy_obj(&strobj, ecp);
    return ret;
}

static int
Duro_exec_stmt(RDB_parse_node *stmtp, Duro_interp *interp,
        RDB_exec_context *ecp, Duro_return_info *retinfop)
{
    int ret = RDB_OK;
    RDB_parse_node *firstchildp;

    if (stmtp->kind != RDB_NODE_INNER) {
        RDB_raise_internal("interpreter encountered invalid node", ecp);
        return RDB_ERROR;
    }
    firstchildp = stmtp->val.children.firstp;
    if (firstchildp->kind == RDB_NODE_TOK) {
        switch (firstchildp->val.token) {
        case TOK_IF:
            ret = exec_if(firstchildp->nextp, interp, ecp, retinfop);
            break;
        case TOK_CASE:
            ret = exec_case(firstchildp->nextp, interp, ecp, retinfop);
            break;
        case TOK_FOR:
            if (firstchildp->nextp->nextp->val.token == TOK_IN) {
                ret = exec_foreach(firstchildp->nextp, NULL, interp, ecp, retinfop);
            } else {
                ret = exec_for(firstchildp->nextp, NULL, interp, ecp, retinfop);
            }
            break;
        case TOK_WHILE:
            ret = exec_while(firstchildp->nextp, NULL, interp, ecp, retinfop);
            break;
        case TOK_OPERATOR:
            ret = exec_opdef(stmtp, interp, ecp);
            break;
        case TOK_TRY:
            ret = exec_try(firstchildp->nextp, interp, ecp, retinfop);
            break;
        case TOK_LEAVE:
            ret = exec_leave(firstchildp->nextp, interp, ecp);
            break;
        case ';':
            /* Empty statement */
            ret = RDB_OK;
            break;
        case TOK_CALL:
            ret = exec_call(firstchildp->nextp, interp, ecp);
            break;
        case TOK_VAR:
            if (firstchildp->nextp->nextp->kind == RDB_NODE_TOK) {
                switch (firstchildp->nextp->nextp->val.token) {
                case TOK_REAL:
                    ret = Duro_exec_vardef_real(firstchildp->nextp, interp, ecp);
                    break;
                case TOK_VIRTUAL:
                    ret = Duro_exec_vardef_virtual(firstchildp->nextp, interp, ecp);
                    break;
                case TOK_PRIVATE:
                    ret = Duro_exec_vardef_private(firstchildp->nextp, interp, ecp);
                    break;
                case TOK_PUBLIC:
                    ret = Duro_exec_vardef_public(firstchildp->nextp, interp, ecp);
                    break;
                default:
                    ret = Duro_exec_vardef(firstchildp->nextp, interp, ecp);
                }
            } else {
                ret = Duro_exec_vardef(firstchildp->nextp, interp, ecp);
            }
            break;
        case TOK_CONST:
            ret = Duro_exec_constdef(firstchildp->nextp, interp, ecp);
            break;
        case TOK_DROP:
            switch (firstchildp->nextp->val.token) {
            case TOK_VAR:
                ret = Duro_exec_vardrop(firstchildp->nextp->nextp, interp, ecp);
                break;
            case TOK_CONSTRAINT:
                ret = exec_constrdrop(firstchildp->nextp->nextp, interp, ecp);
                break;
            case TOK_TYPE:
                ret = exec_typedrop(firstchildp->nextp->nextp, interp, ecp);
                break;
            case TOK_OPERATOR:
                ret = exec_opdrop(firstchildp->nextp->nextp, interp, ecp);
                break;
            case TOK_INDEX:
                ret = exec_indexdrop(firstchildp->nextp->nextp, interp, ecp);
                break;
            case TOK_PACKAGE:
                ret = exec_pkgdrop(firstchildp->nextp->nextp, interp, ecp);
                break;
            }
            break;
            case TOK_BEGIN:
                if (firstchildp->nextp->kind == RDB_NODE_INNER) {
                    /* BEGIN ... END */
                    ret = exec_stmts(firstchildp->nextp->val.children.firstp,
                            interp, ecp, retinfop);
                } else {
                    /* BEGIN TRANSACTION */
                    ret = exec_begin_tx(interp, ecp);
                }
                break;
            case TOK_COMMIT:
                ret = exec_commit(interp, ecp);
                break;
            case TOK_ROLLBACK:
                ret = exec_rollback(interp, ecp);
                break;
            case TOK_TYPE:
                ret = exec_typedef(firstchildp->nextp, interp, ecp);
                break;
            case TOK_RETURN:
                ret = exec_return(firstchildp->nextp, interp, ecp, retinfop);
                break;
            case TOK_LOAD:
                ret = Duro_exec_load(firstchildp->nextp, interp, ecp);
                break;
            case TOK_CONSTRAINT:
                ret = exec_constrdef(firstchildp->nextp, interp, ecp);
                break;
            case TOK_INDEX:
                ret = exec_indexdef(firstchildp->nextp, interp, ecp);
                break;
            case TOK_EXPLAIN:
                if (firstchildp->nextp->nextp->nextp == NULL) {
                    ret = Duro_exec_explain_assign(firstchildp->nextp, interp, ecp);
                } else {
                    ret = exec_explain(firstchildp->nextp, interp, ecp);
                }
                break;
            case TOK_RAISE:
                ret = exec_raise(firstchildp->nextp, interp, ecp);
                break;
            case TOK_IMPLEMENT:
                if (firstchildp->nextp->val.token == TOK_TYPE) {
                    ret = exec_typeimpl(firstchildp->nextp->nextp, interp, ecp);
                } else {
                    ret = exec_packagedef(firstchildp->nextp->nextp, interp, ecp, retinfop);
                }
                break;
            case TOK_MAP:
                ret = exec_map(firstchildp->nextp, interp, ecp);
                break;
            case TOK_RENAME:
                ret = Duro_exec_rename(firstchildp->nextp->nextp, interp, ecp);
                break;
            case TOK_PACKAGE:
                ret = exec_packagedef(firstchildp->nextp, interp, ecp, retinfop);
                break;
            default:
                RDB_raise_internal("invalid token", ecp);
                ret = RDB_ERROR;
        }
        if (ret == RDB_ERROR) {
            if (interp->err_line < 0) {
                interp->err_line = stmtp->lineno;
            }
        }
        return ret;
    }
    if (firstchildp->kind == RDB_NODE_EXPR) {
        if (firstchildp->nextp->val.token == '(') {
            /* Operator invocation */
            ret = exec_call(firstchildp, interp, ecp);
        } else {
            /* Loop with label */
            switch (firstchildp->nextp->nextp->val.token) {
            case TOK_WHILE:
                ret = exec_while(firstchildp->nextp->nextp->nextp,
                        firstchildp, interp, ecp, retinfop);
                break;
            case TOK_FOR:
                if (firstchildp->nextp->nextp->nextp->nextp->val.token == TOK_IN) {
                    ret = exec_foreach(firstchildp->nextp->nextp->nextp,
                            firstchildp, interp, ecp, retinfop);
                } else {
                    ret = exec_for(firstchildp->nextp->nextp->nextp,
                            firstchildp, interp, ecp, retinfop);
                }
                break;
            default:
                RDB_raise_internal("invalid token", ecp);
                ret = RDB_ERROR;
            }
        }
        if (ret == RDB_ERROR && interp->err_line < 0) {
            interp->err_line = stmtp->lineno;
        }
        return ret;
    }
    if (firstchildp->kind != RDB_NODE_INNER) {
        RDB_raise_internal("interpreter encountered invalid node", ecp);
        return RDB_ERROR;
    }
    if (firstchildp->nextp->val.token == '(') {
        /* Operator invocation with qualified operator name */
        ret = exec_call(firstchildp, interp, ecp);
        if (ret == RDB_ERROR && interp->err_line < 0) {
            interp->err_line = stmtp->lineno;
        }
        return ret;
    }

    /* Assignment */
    ret = Duro_exec_assign(firstchildp, interp, ecp);
    if (ret == RDB_ERROR && interp->err_line < 0) {
        interp->err_line = stmtp->lineno;
    }
    return ret;
}

static int
Duro_exec_stmt_impl_tx(RDB_parse_node *stmtp, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    /*
     * No implicit transaction if the statement is a BEGIN TX, COMMIT,
     * or ROLLBACK.
     */
    RDB_bool implicit_tx = RDB_obj_bool(interp->implicit_tx_objp)
                    && !is_tx_stmt(stmtp) && (interp->txnp == NULL);

    /* No implicit tx if no database is available. */
    if (implicit_tx) {
        if (Duro_get_db(interp, ecp) == NULL) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
                return RDB_ERROR;
            RDB_clear_err(ecp);
            implicit_tx = RDB_FALSE;
        }
    }

    if (implicit_tx) {
        if (do_begin_tx(interp, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    }
    if (Duro_exec_stmt(stmtp, interp, ecp, NULL) != RDB_OK) {
        if (implicit_tx) {
            do_rollback(interp, ecp);
        }
        return RDB_ERROR;
    }

    if (implicit_tx) {
        return do_commit(interp, ecp);
    }
    return RDB_OK;
}

/*
 * Parse next statement and execute it.
 * Returning RDB_ERROR with no error in *ecp means that the end of input
 * has been reached.
 */
int
Duro_process_stmt(Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    RDB_parse_node *stmtp;
    Duro_var_entry *dbnameentryp = Duro_varmap_get(&interp->root_varmap, "current_db");

    if (RDB_parse_get_interactive()) {
        /* Build interp->prompt */
        if (dbnameentryp != NULL && dbnameentryp->varp != NULL
                && *RDB_obj_string(dbnameentryp->varp) != '\0') {
            ret = RDB_string_to_obj(&interp->prompt, RDB_obj_string(dbnameentryp->varp), ecp);
        } else {
            ret = RDB_string_to_obj(&interp->prompt, "no db", ecp);
        }
        if (ret != RDB_OK)
            return ret;
        RDB_append_string(&interp->prompt, "> ", ecp);
    }

    stmtp = RDB_parse_stmt(ecp);

    if (stmtp == NULL) {
        interp->err_line = yylineno;
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    ret = Duro_exec_stmt_impl_tx(stmtp, interp, ecp);

    if (ret != RDB_OK) {
        if (ret == DURO_RETURN) {
            RDB_raise_syntax("invalid RETURN", ecp);
            interp->err_line = yylineno;
            return RDB_ERROR;
        }
        if (ret == DURO_LEAVE) {
            RDB_raise_syntax("unmatched LEAVE", ecp);
            interp->err_line = yylineno;
            return RDB_ERROR;
        }
        RDB_parse_del_node(stmtp, ecp);
        if (RDB_get_err(ecp) == NULL) {
            RDB_raise_internal("statement execution failed, no error available", ecp);
        }
        return RDB_ERROR;
    }

    return RDB_parse_del_node(stmtp, ecp);
}
