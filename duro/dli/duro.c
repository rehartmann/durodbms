/*
 * $Id$
 *
 * Copyright (C) 2007 René Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Interpreter for Duro D.
 */

#include <rel/rdb.h>
#include <rel/internal.h>
#include "parse.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>

typedef struct tx_node {
    RDB_transaction tx;
    struct tx_node *nextp;
} tx_node;

#ifdef _WIN32
#define _RDB_EXTERN_VAR __declspec(dllimport)
#else
#define _RDB_EXTERN_VAR extern
#endif

_RDB_EXTERN_VAR FILE *yyin;

extern int yydebug;

static RDB_hashmap varmap;

static RDB_op_map opmap;

static RDB_environment *envp = NULL;

static char *dbname = NULL;

static tx_node *txnp = NULL;

typedef int upd_op_func(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *, RDB_transaction *);

static void
exit_interp(int rcode) {
    if (txnp != NULL) {
        RDB_exec_context ec;

        RDB_init_exec_context(&ec);
        RDB_rollback(&ec, &txnp->tx);
        RDB_destroy_exec_context(&ec);

        if (_RDB_parse_interactive)
            printf("Transaction rolled back.\n");
    }
    if (envp != NULL)
        RDB_close_env(envp);
    exit(rcode);
}

static int
println_op(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    puts(RDB_obj_string(argv[0]));
    return RDB_OK;
}   

static int
exit_op(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    exit_interp(0);
    return 0; /* only to avoid compiling error */
}   

static int
exit_op_int(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    exit_interp(RDB_obj_int(argv[0]));
    return 0; /* only to avoid compiling error */
}   

static RDB_object *
get_var(const char *name, void *maparg)
{
    return (RDB_object *) RDB_hashmap_get((RDB_hashmap *) maparg, name);
}

static int
init_obj(RDB_object *objp, RDB_type *typ, RDB_exec_context *ecp)
{
    int i;

    if (typ == &RDB_BOOLEAN) {
        RDB_bool_to_obj(objp, RDB_FALSE);
    } else if (typ == &RDB_INTEGER) {
        RDB_int_to_obj(objp, 0);
    } else if (typ == &RDB_STRING) {
        return RDB_string_to_obj(objp, "", ecp);
    } else if (typ->kind == RDB_TP_TUPLE) {
        for (i = 0; i < typ->var.tuple.attrc; i++) {
            if (RDB_tuple_set(objp, typ->var.tuple.attrv[i].name,
                    NULL, ecp) != RDB_OK)
                return RDB_ERROR;
            if (init_obj(RDB_tuple_get(objp, typ->var.tuple.attrv[i].name),
                    typ->var.tuple.attrv[i].typ, ecp) != RDB_OK)
                return RDB_ERROR;
        }
    } else {
        abort();
    }
    return RDB_OK;
}

static int
exec_vardef(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object *objp;
    char *varname = RDB_obj_string(&stmtp->var.vardef.varname);

    /*
     * Check if the variable already exists
     */
    if (RDB_hashmap_get(&varmap, varname) != NULL) {
        RDB_raise_element_exists(varname, ecp);
        return RDB_ERROR;
    }
    objp = malloc(sizeof (RDB_object));
    if (objp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    RDB_init_obj(objp);
    if (stmtp->var.vardef.initexp == NULL) {
        init_obj(objp, stmtp->var.vardef.typ, ecp); /* !! */
    } else {
        ret = RDB_evaluate(stmtp->var.vardef.initexp, &get_var, &varmap,
                ecp, NULL, objp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(objp, ecp);
            return RDB_ERROR;
        }
        if (stmtp->var.vardef.typ != NULL
                && !RDB_type_equals(stmtp->var.vardef.typ,
                                    RDB_obj_type(objp))) {
            RDB_destroy_obj(objp, ecp);
            RDB_raise_type_mismatch("", ecp);
            return RDB_ERROR;            
        }
    }
    ret = RDB_hashmap_put(&varmap, varname, objp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(objp, ecp);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
exec_vardef_real(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    RDB_string_vec *keyv;
    int keyc = 0;
    RDB_parse_keydef *keyp;
    RDB_expression *keyattrp;
    int i, j;
    RDB_bool freekey;
    char *varname = RDB_obj_string(&stmtp->var.vardef_real.varname);
    RDB_type *tbtyp = RDB_dup_nonscalar_type(stmtp->var.vardef_real.typ, ecp);
    if (tbtyp == NULL)
        return RDB_ERROR;

    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    keyp = stmtp->var.vardef_real.firstkeyp;
    while (keyp != NULL) {
        keyc++;
        keyp = keyp->nextp;
    }

    if (keyc > 0) {
        keyv = malloc(sizeof(RDB_string_vec) * keyc);
        if (keyv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        keyp = stmtp->var.vardef_real.firstkeyp;
        for (i = 0; i < keyc; i++) {
            keyv[i].strc = RDB_expr_list_length(&keyp->attrlist);
            keyv[i].strv = malloc(keyv[i].strc * sizeof(char *));
            if (keyv[i].strv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            keyattrp = keyp->attrlist.firstp;
            for (j = 0; j < keyv[i].strc; j++) {
                keyv[i].strv[j] = RDB_obj_string(RDB_expr_obj(keyattrp));
                keyattrp = keyattrp->nextp;
            }
        }       
    } else {
        /* Get keys from expression */

        printf("Inferring keys\n");

        keyc = _RDB_infer_keys(stmtp->var.vardef_real.initexp, ecp, &keyv,
                &freekey);
        if (keyc == RDB_ERROR)
            return RDB_ERROR;
    }

    tbp = RDB_create_table_from_type(varname, tbtyp, keyc, keyv, ecp, &txnp->tx);
    if (tbp == NULL) {
        RDB_drop_type(tbtyp, ecp, NULL);
        return RDB_ERROR;
    }

    /* !! */   

    if (_RDB_parse_interactive)
        printf("Table %s created.\n", varname);
    return RDB_OK;
}

static int
exec_vardef_virtual(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    char *varname = RDB_obj_string(&stmtp->var.vardef_virtual.varname);
    RDB_expression *texp = RDB_dup_expr(stmtp->var.vardef_virtual.exp,
            ecp);
    if (texp == NULL)
        return RDB_ERROR;

    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }
    tbp = RDB_expr_to_vtable(texp, ecp, &txnp->tx);
    if (tbp == NULL) {
        RDB_drop_expr(texp, ecp);
        return RDB_ERROR;
    }
    if (RDB_set_table_name(tbp, varname, ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;
    if (RDB_add_table(tbp, ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    if (_RDB_parse_interactive)
        printf("Table %s created.\n", varname);
    return RDB_OK;
}

static int
exec_vardrop(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    char *varname = RDB_obj_string(&stmtp->var.vardrop.varname);
    RDB_object *tbp;
    
    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    tbp = RDB_get_table(varname, ecp, &txnp->tx);
    if (tbp == NULL)
        return RDB_ERROR;

    if (RDB_drop_table(tbp, ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    if (_RDB_parse_interactive)
        printf("Table %s dropped.\n", varname);
    return RDB_OK;
}

static int
exec_call(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int ret;
    int i;
    int argc;
    RDB_object argv[DURO_MAX_LLEN];
    RDB_object *argpv[DURO_MAX_LLEN];
    RDB_type *argtv[DURO_MAX_LLEN];
    char *opname;
    void *op;
    upd_op_func *opfnp;
    RDB_expression *argp;

    argp = stmtp->var.call.arglist.firstp;
    i = 0;
    while (argp != NULL) {
        RDB_init_obj(&argv[i]);
        if (RDB_evaluate(argp, &get_var, &varmap,
                ecp, txnp != NULL ? &txnp->tx : NULL, &argv[i]) != RDB_OK)
            return RDB_ERROR;
        argpv[i] = &argv[i];
        argtv[i] = RDB_obj_type(&argv[i]);
        i++;
    }
    argc = i;
    opname = RDB_obj_string(&stmtp->var.call.opname);
    op = RDB_get_op(&opmap, opname, argc, argtv);
    if (op == NULL) {
        RDB_raise_operator_not_found(opname, ecp);
        for (i = 0; i < argc; i++)
            RDB_destroy_obj(&argv[i], ecp);
        return RDB_ERROR;
    }
    opfnp = (upd_op_func *) op;
    ret = (*opfnp) (opname, argc, argpv, ecp,
            txnp != NULL ? &txnp->tx : NULL);
    for (i = 0; i < argc; i++)
        RDB_destroy_obj(&argv[i], ecp);
    return ret;
}

static int
exec_stmt(const RDB_parse_statement *, RDB_exec_context *);

static int
exec_stmtlist(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    do {
        if (exec_stmt(stmtp, ecp) != RDB_OK)
            return RDB_ERROR;
        stmtp = stmtp->nextp;
    } while(stmtp != NULL);
    return RDB_OK;
}

static int
exec_if(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_bool b;

    if (RDB_evaluate_bool(stmtp->var.ifthen.condp, &get_var, &varmap,
            ecp, NULL, &b) != RDB_OK)
        return RDB_ERROR;
    if (b) {
        return exec_stmtlist(stmtp->var.ifthen.ifp, ecp);
    }
    if (stmtp->var.ifthen.elsep != NULL) {
        return exec_stmtlist(stmtp->var.ifthen.elsep, ecp);
    }
    return RDB_OK;
}

static int
exec_while(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_bool b;

    for(;;) {
        if (RDB_evaluate_bool(stmtp->var.whileloop.condp, &get_var, &varmap,
                ecp, NULL, &b) != RDB_OK)
            return RDB_ERROR;
        if (!b)
            return RDB_OK;
        if (exec_stmtlist(stmtp->var.whileloop.bodyp, ecp) != RDB_OK)
            return RDB_ERROR;
    }
}

static int
exec_for(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_object *varp;
    RDB_object endval;

    if (stmtp->var.forloop.varexp->kind != RDB_EX_VAR) {
        RDB_raise_syntax("variable name expected", ecp);
        return RDB_ERROR;
    }
    varp = RDB_hashmap_get(&varmap, stmtp->var.forloop.varexp->var.varname);
    if (varp == NULL) {
        printf("undefined variable %s\n",
                stmtp->var.forloop.varexp->var.varname);
        return RDB_ERROR;
    }
    if (RDB_obj_type(varp) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("loop variable must be INTEGER", ecp);
        return RDB_ERROR;
    }
    if (RDB_evaluate(stmtp->var.forloop.fromp, &get_var,
            &varmap, ecp, NULL, varp) != RDB_OK) {
        return RDB_ERROR;
    }
    RDB_init_obj(&endval);
    if (RDB_evaluate(stmtp->var.forloop.top, &get_var,
            &varmap, ecp, NULL, &endval) != RDB_OK) {
        RDB_destroy_obj(&endval, ecp);
        return RDB_ERROR;
    }
    if (RDB_obj_type(&endval) != &RDB_INTEGER) {
        RDB_destroy_obj(&endval, ecp);
        RDB_raise_type_mismatch("expression must be INTEGER", ecp);
        return RDB_ERROR;
    }
    while (varp->var.int_val <= endval.var.int_val) {
        if (exec_stmtlist(stmtp->var.forloop.bodyp, ecp) != RDB_OK) {
            RDB_destroy_obj(&endval, ecp);
            return RDB_ERROR;
        }
        varp->var.int_val++;
    }
    RDB_destroy_obj(&endval, ecp);
    return RDB_OK;
}

static RDB_object *
lookup_var(const char *name, RDB_exec_context *ecp)
{
    RDB_object *objp = RDB_hashmap_get(&varmap, name);
    if (objp == NULL && txnp != NULL) {
        /* Try to get table from DB */
        objp = RDB_get_table(name, ecp, &txnp->tx);
    }
    if (objp == NULL)
        RDB_raise_attribute_not_found(name, ecp);
    return objp;
}

static RDB_attr_update *
convert_attr_assigns(RDB_parse_attr_assign *assignlp, int *updcp)
{
    int i;
    RDB_attr_update *updv;
    RDB_parse_attr_assign *ap = assignlp;
    *updcp = 0;
    do {
        (*updcp)++;
        ap = ap->nextp;
    } while (ap != NULL);

    updv = malloc(*updcp * sizeof(RDB_attr_update));
    if (updv == NULL)
        return NULL;
    ap = assignlp;
    for (i = 0; i < *updcp; i++) {
        updv[i].name = ap->dstp->var.varname;
        updv[i].exp = ap->srcp;
        ap = ap->nextp;
    }
    return updv;
}

static int
exec_assign(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i;
    int cnt;
    RDB_ma_copy copyv[DURO_MAX_LLEN];
    RDB_ma_insert insv[DURO_MAX_LLEN];
    RDB_ma_update updv[DURO_MAX_LLEN];
    RDB_ma_delete delv[DURO_MAX_LLEN];
    RDB_object srcobjv[DURO_MAX_LLEN];
    int copyc = 0;
    int insc = 0;
    int updc = 0;
    int delc = 0;

    for (i = 0; i < stmtp->var.assignment.ac; i++) {
        RDB_init_obj(&srcobjv[i]);
    }

    for (i = 0; i < stmtp->var.assignment.ac; i++) {
        switch (stmtp->var.assignment.av[i].kind) {
            case RDB_STMT_COPY:
                if (stmtp->var.assignment.av[i].var.copy.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid assignment target", ecp);
                    return RDB_ERROR;
                }
                copyv[copyc].dstp = lookup_var(
                        stmtp->var.assignment.av[i].var.copy.dstp->var.varname,
                        ecp);
                if (copyv[copyc].dstp == NULL) {
                    return RDB_ERROR;
                }
                copyv[copyc].srcp = &srcobjv[i];
                if (RDB_evaluate(stmtp->var.assignment.av[i].var.copy.srcp, &get_var,
                        &varmap, ecp, txnp != NULL ? &txnp->tx : NULL, &srcobjv[i])
                               != RDB_OK)
                    return RDB_ERROR;
                copyc++;
                break;
            case RDB_STMT_INSERT:
                if (stmtp->var.assignment.av[i].var.copy.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid assignment target", ecp);
                    return RDB_ERROR;
                }
                insv[insc].tbp = lookup_var(
                        stmtp->var.assignment.av[i].var.ins.dstp->var.varname,
                        ecp);
                if (insv[insc].tbp == NULL) {
                    return RDB_ERROR;
                }
                insv[insc].tplp = &srcobjv[i];
                if (RDB_evaluate(stmtp->var.assignment.av[i].var.ins.srcp, &get_var,
                        &varmap, ecp, txnp != NULL ? &txnp->tx : NULL, &srcobjv[i])
                               != RDB_OK)
                    return RDB_ERROR;
                insc++;
                break;                
            case RDB_STMT_UPDATE:
                if (stmtp->var.assignment.av[i].var.upd.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid assignment target", ecp);
                    return RDB_ERROR;
                }
                updv[updc].tbp = lookup_var(
                        stmtp->var.assignment.av[i].var.upd.dstp->var.varname,
                        ecp);
                if (updv[updc].tbp == NULL) {
                    return RDB_ERROR;
                }
                updv[updc].updv = convert_attr_assigns(
                        stmtp->var.assignment.av[i].var.upd.assignlp,
                        &updv[updc].updc);
                if (updv[updc].updv == NULL) {
                    RDB_raise_no_memory(ecp);
                    return RDB_ERROR;
                }
                updv[updc].condp = stmtp->var.assignment.av[i].var.upd.condp;
                updc++;
                break;
            case RDB_STMT_DELETE:
                if (stmtp->var.assignment.av[i].var.del.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid assignment target", ecp);
                    return RDB_ERROR;
                }
                delv[delc].tbp = lookup_var(
                        stmtp->var.assignment.av[i].var.del.dstp->var.varname,
                        ecp);
                if (delv[insc].tbp == NULL) {
                    return RDB_ERROR;
                }
                delv[delc].condp = stmtp->var.assignment.av[i].var.del.condp;
                delc++;
                break;                
        }
    }
    cnt = RDB_multi_assign(insc, insv, updc, updv, delc, delv, copyc, copyv,
            ecp, txnp != NULL ? &txnp->tx : NULL);
    if (cnt == (RDB_int) RDB_ERROR)
        return RDB_ERROR;

    if (_RDB_parse_interactive)
        printf("%d elements affected.\n", (int) cnt);

    return RDB_OK;
}

static int
exec_begin_tx(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int ret;
    RDB_database *dbp;

    if (envp == NULL || dbname == NULL) {
        printf("no database\n");
        return RDB_ERROR;
    }
    if (txnp != NULL) {
        printf("Hier wäre eine nested Tx fällig!\n");
        return RDB_OK;
    }

    dbp = RDB_get_db_from_env(dbname, envp, ecp);
    if (dbp == NULL)
        return RDB_ERROR;

    txnp = malloc(sizeof(tx_node));
    if (txnp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }    

    ret = RDB_begin_tx(ecp, &txnp->tx, dbp, NULL);
    if (ret != RDB_OK) {
        free(txnp);
        txnp = NULL;
        return RDB_ERROR;
    }
    txnp->nextp = NULL;

    if (_RDB_parse_interactive)
        printf("Transaction started.\n");
    return RDB_OK;
}

static int
exec_commit(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    if (RDB_commit(ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    free(txnp);
    txnp = NULL;

    if (_RDB_parse_interactive)
        printf("Transaction commited.\n");
    return RDB_OK;
}

static int
exec_rollback(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    if (RDB_rollback(ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    free(txnp);
    txnp = NULL;

    if (_RDB_parse_interactive)
        printf("Transaction rolled back.\n");
    return RDB_OK;
}

static int
exec_stmt(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    switch (stmtp->kind) {
        case RDB_STMT_CALL:
            return exec_call(stmtp, ecp);
        case RDB_STMT_VAR_DEF:
            return exec_vardef(stmtp, ecp);
        case RDB_STMT_VAR_DEF_REAL:
            return exec_vardef_real(stmtp, ecp);
        case RDB_STMT_VAR_DEF_VIRTUAL:
            return exec_vardef_virtual(stmtp, ecp);
        case RDB_STMT_VAR_DROP:
            return exec_vardrop(stmtp, ecp);
        case RDB_STMT_NOOP:
            return RDB_OK;
        case RDB_STMT_IF:
            return exec_if(stmtp, ecp);
        case RDB_STMT_FOR:
            return exec_for(stmtp, ecp);
        case RDB_STMT_WHILE:
            return exec_while(stmtp, ecp);
        case RDB_STMT_ASSIGN:
            return exec_assign(stmtp, ecp);
        case RDB_STMT_BEGIN_TX:
            return exec_begin_tx(stmtp, ecp);
        case RDB_STMT_COMMIT:
            return exec_commit(stmtp, ecp);
        case RDB_STMT_ROLLBACK:
            return exec_rollback(stmtp, ecp);
    }
    abort();
}

static void
print_error(const RDB_object *errobjp)
{
    RDB_exec_context ec;
    RDB_object msgobj;
    RDB_type *errtyp = RDB_obj_type(errobjp);

    RDB_init_exec_context(&ec);
    RDB_init_obj(&msgobj);

    fputs(RDB_type_name(errtyp), stdout);

    if (RDB_obj_comp(errobjp, "MSG", &msgobj, &ec, NULL) == RDB_OK) {
        printf(": %s", RDB_obj_string(&msgobj));
    }

    fputs("\n", stdout);

    RDB_destroy_obj(&msgobj, &ec);
    RDB_destroy_exec_context(&ec);
}

int
init_upd_ops(RDB_exec_context *ecp)
{
    static RDB_type *println_types[1];
    static RDB_type *exit_int_types[1];
    println_types[0] = &RDB_STRING;
    exit_int_types[0] = &RDB_INTEGER;

    RDB_init_op_map(&opmap);

    if (RDB_put_op(&opmap, "PRINTLN", 1, println_types, println_op, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_op(&opmap, "EXIT", 0, NULL, exit_op, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_op(&opmap, "EXIT", 1, exit_int_types, exit_op_int, ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}

static void
usage_error(void)
{
    puts("usage: duro [-e envpath] [-d database] [file]");
    exit(1);
}

int
main(int argc, char *argv[])
{
    int ret;
    RDB_exec_context ec;
    RDB_parse_statement *stmtp;
    char *envname = NULL;

    while (argc > 1) {
        if (strcmp(argv[1], "-e") == 0) {
            if (argc < 3)
                usage_error();
            envname = argv[2];
            argc -= 2;
            argv += 2;
        } else if (strcmp(argv[1], "-d") == 0) {
            if (argc < 3)
                usage_error();
            dbname = argv[2];
            argc -= 2;
            argv += 2;
        } else
            break;
    }
    if (argc > 2)
        usage_error();
    if (argc == 2) {        
        yyin = fopen(argv[1], "r");
        if (yyin == NULL) {
            fprintf(stderr, "unable to open %s: %s\n", argv[1],
                strerror(errno));
            return 1;
        }
    } else {
        yyin = stdin;
    }

    if (envname != NULL) {
        ret = RDB_open_env(envname, &envp);
        if (ret != RDB_OK) {
            fprintf(stderr, "unable to open environment %s: %s\n", envname,
                db_strerror(errno));
            return 1;
        }
    }

    _RDB_parse_interactive = (RDB_bool) isatty(fileno(yyin));

    /* yydebug = 1; */

    RDB_init_hashmap(&varmap, 256);

    RDB_init_exec_context(&ec);

    if (_RDB_init_builtin_types(&ec) != RDB_OK) {
        print_error(RDB_get_err(&ec));
        return 1;
    }

    if (init_upd_ops(&ec) != RDB_OK) {
        print_error(RDB_get_err(&ec));
        return 1;
    }

    if (_RDB_parse_interactive) {
        puts("Interactive Duro D early development version");
        _RDB_parse_init_buf();
    }

    for(;;) {
        stmtp = RDB_parse_stmt(&ec);
        if (stmtp == NULL) {
            RDB_object *errobjp = RDB_get_err(&ec);
            if (errobjp != NULL) {
                print_error(errobjp);
                if (!_RDB_parse_interactive)
                    exit_interp(1);
                RDB_clear_err(&ec);
            } else {
                /* Exit on EOF  */
                puts("");
                exit_interp(0);
            }
        } else {
            RDB_object *errobjp;

            ret = exec_stmt(stmtp, &ec);
            if (ret != RDB_OK) {
                RDB_parse_del_stmt(stmtp, &ec);
                errobjp = RDB_get_err(&ec);
                if (errobjp != NULL) {
                    print_error(errobjp);
                    RDB_clear_err(&ec);
                }
            } else {
                ret = RDB_parse_del_stmt(stmtp, &ec);
                if (ret != RDB_OK) {
                    RDB_parse_del_stmt(stmtp, &ec);
                    errobjp = RDB_get_err(&ec);
                    print_error(errobjp);
                    exit_interp(2);
                }
            }
        }
    }
}
