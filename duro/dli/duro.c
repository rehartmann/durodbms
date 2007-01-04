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
#include <unistd.h>
#include <string.h>
#include <errno.h>

extern int yydebug;

static RDB_hashmap varmap;

static RDB_op_map opmap;

typedef int upd_op_func(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *, RDB_transaction *);

typedef struct {
    upd_op_func *fnp;
} upd_op_data;

static int
println_op(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    puts(RDB_obj_string(argv[0]));
    return RDB_OK;
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
        printf("%s already exists.\n", varname);
        return RDB_OK;
    }
    objp = malloc(sizeof (RDB_object));
    /* !! */
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
exec_call(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int ret;
    int i;
    RDB_object argv[DURO_MAX_LLEN];
    RDB_object *argpv[DURO_MAX_LLEN];
    RDB_type *argtv[DURO_MAX_LLEN];
    char *opname;
    void *op;
    upd_op_data *opdatap;

    for (i = 0; i < stmtp->var.call.argc; i++) {
        RDB_init_obj(&argv[i]);
        if (RDB_evaluate(stmtp->var.call.argv[i], &get_var, &varmap,
                ecp, NULL, &argv[i]) != RDB_OK)
            return RDB_ERROR;
        argpv[i] = &argv[i];
        argtv[i] = RDB_obj_type(&argv[i]);
    }
    opname = RDB_obj_string(&stmtp->var.call.opname);
    op = RDB_get_op(&opmap, opname, stmtp->var.call.argc, argtv);
    if (op == NULL) {
        RDB_raise_operator_not_found(opname, ecp);
        for (i = 0; i < stmtp->var.call.argc; i++)
            RDB_destroy_obj(&argv[i], ecp);
        return RDB_ERROR;
    }
    opdatap = (upd_op_data *) op;
    ret = (*opdatap->fnp) (opname, stmtp->var.call.argc, argpv, ecp, NULL);
    for (i = 0; i < stmtp->var.call.argc; i++)
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
        return RDB_OK;
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

static int
exec_assign(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i;
    RDB_ma_copy copyv[DURO_MAX_LLEN];
    RDB_object srcobjv[DURO_MAX_LLEN];

    for (i = 0; i < stmtp->var.assignment.ac; i++) {
        RDB_init_obj(&srcobjv[i]);
    }

    for (i = 0; i < stmtp->var.assignment.ac; i++) {
        if (stmtp->var.assignment.av[i].dstp->kind != RDB_EX_VAR) {
            printf("invalid assignment target\n");
            return RDB_OK;
        }
        copyv[i].dstp = RDB_hashmap_get(&varmap,
                stmtp->var.assignment.av[i].dstp->var.varname);
        if (copyv[i].dstp == NULL) {
            printf("undefined variable %s\n",
                    stmtp->var.assignment.av[i].dstp->var.varname);
            return RDB_OK;
        }
        copyv[i].srcp = &srcobjv[i];
        if (RDB_evaluate(stmtp->var.assignment.av[i].srcp, &get_var, &varmap,
                ecp, NULL, &srcobjv[i]) != RDB_OK)
            return RDB_ERROR;
    }
    if (RDB_multi_assign(0, NULL, 0, NULL, 0, NULL,
           stmtp->var.assignment.ac, copyv, ecp, NULL) == (RDB_int) RDB_ERROR)
        return RDB_ERROR;

    /* !! */

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
        case RDB_STMT_NOOP:
            return RDB_OK;
        case RDB_STMT_IF:
            return exec_if(stmtp, ecp);
        case RDB_STMT_ASSIGN:
            return exec_assign(stmtp, ecp);
        case RDB_STMT_FOR:
            return exec_for(stmtp, ecp);
        case RDB_STMT_WHILE:
            return exec_while(stmtp, ecp);
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
    static RDB_type *println_types[] = { &RDB_STRING };
    static upd_op_data println_data = { &println_op };

    RDB_init_op_map(&opmap);

    if (RDB_put_op(&opmap, "PRINTLN", 1, println_types, &println_data, ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}

extern FILE *yyin;

int
main(int argc, char *argv[])
{
    int ret;
    RDB_exec_context ec;
    RDB_parse_statement *stmtp;

    if (argc == 2) {        
        yyin = fopen(argv[1], "r");
        if (yyin == NULL) {
            fprintf(stderr, "unable to open %s: %s\n", argv[1], strerror(errno));
            return 1;
        }
    } else {
        yyin = stdin;
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
                    return 1;
                RDB_clear_err(&ec);
            } else {
                /* Exit on EOF  */
                return 0;
            }
        } else {
            ret = exec_stmt(stmtp, &ec);
            if (ret != RDB_OK) {
                RDB_parse_del_stmt(stmtp, &ec);
                RDB_object *errobjp = RDB_get_err(&ec);
                print_error(errobjp);
                RDB_clear_err(&ec);
            } else {
                ret = RDB_parse_del_stmt(stmtp, &ec);
                if (ret != RDB_OK) {
                    RDB_parse_del_stmt(stmtp, &ec);
                    RDB_object *errobjp = RDB_get_err(&ec);
                    print_error(errobjp);
                    exit(2);
                }
            }
        }
    }
}
