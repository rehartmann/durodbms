/*
 * $Id$
 *
 * Copyright (C) 2007-2008 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Statement execution functions.
 */

#include "iinterp.h"
#include "stmtser.h"
#include "tabletostr.h"
#include <gen/hashmap.h>
#include <gen/hashmapit.h>
#include <rel/rdb.h>
#include <rel/internal.h>

#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

enum {
    DURO_MAX_LLEN = 64
};

typedef struct tx_node {
    RDB_transaction tx;
    struct tx_node *parentp;
} tx_node;

varmap_node toplevel_vars;

extern FILE *yyin;

extern int yylineno;

int err_line;

static sig_atomic_t interrupted;

static varmap_node *current_varmapp;

static RDB_op_map opmap;

static RDB_environment *envp = NULL;

static tx_node *txnp = NULL;

static RDB_hashmap ro_op_cache;

static int
add_varmap(RDB_exec_context *ecp)
{
    varmap_node *nodep = RDB_alloc(sizeof(varmap_node), ecp);
    if (nodep == NULL) {
        return RDB_ERROR;
    }
    RDB_init_hashmap(&nodep->map, 128);
    nodep->parentp = current_varmapp;
    current_varmapp = nodep;
    return RDB_OK;
}

static int
drop_local_var(RDB_object *objp, RDB_exec_context *ecp)
{
    RDB_type *typ = RDB_obj_type(objp);

    /* Array and tuple types must be destroyed */
    if (!RDB_type_is_scalar(typ) && !RDB_type_is_relation(typ)) {
        if (RDB_drop_type(typ, ecp, NULL) != RDB_OK)
            return RDB_ERROR;
    }

    if (RDB_destroy_obj(objp, ecp) != RDB_OK)
        return RDB_ERROR;
    
    RDB_free(objp);
    return RDB_OK;
}

static void
destroy_varmap(RDB_hashmap *map) {
    RDB_hashmap_iter it;
    char *name;
    RDB_object *objp;
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);
    RDB_init_hashmap_iter(&it, map);
    for(;;) {
        objp = RDB_hashmap_next(&it, &name);
        if (name == NULL)
            break;
        if (objp != NULL) {
            drop_local_var(objp, &ec);
        }
    }

    RDB_destroy_hashmap(map);
    RDB_destroy_exec_context(&ec);
}

static void
remove_varmap(void) {
    varmap_node *parentp = current_varmapp->parentp;
    destroy_varmap(&current_varmapp->map);
    RDB_free(current_varmapp);
    current_varmapp = parentp;
}

static RDB_object *
lookup_local_var(const char *name, varmap_node *varmapp)
{
    RDB_object *objp;
    varmap_node *nodep = varmapp;

    do {
        objp = RDB_hashmap_get(&nodep->map, name);
        if (objp != NULL)
            return objp;
        nodep = nodep->parentp;
    } while (nodep != NULL);

    return NULL;
}

static RDB_object *
lookup_var(const char *name, RDB_exec_context *ecp)
{
    RDB_object *objp = lookup_local_var(name, current_varmapp);
    if (objp != NULL)
        return objp;

    if (txnp != NULL) {
        /* Try to get table from DB */
        objp = RDB_get_table(name, ecp, &txnp->tx);
    }
    if (objp == NULL)
        RDB_raise_name(name, ecp);
    return objp;
}

static RDB_object *
get_var(const char *name, void *maparg)
{
    return lookup_local_var(name, (varmap_node *) maparg);
}

static RDB_type *
get_var_type(const char *name, void *maparg)
{
    RDB_object *objp = lookup_local_var(name, (varmap_node *) maparg);
    return objp != NULL ? RDB_obj_type(objp) : NULL;
}

struct op_data {
    RDB_parse_statement *stmtlistp;
    int argnamec;
    char **argnamev;
};

void
Duro_exit_interp(void)
{
    int i;
    RDB_exec_context ec;
    RDB_hashmap_iter it;
    char *name;
    struct op_data *op;

    RDB_init_exec_context(&ec);

    RDB_init_hashmap_iter(&it, &ro_op_cache);
    while((op = RDB_hashmap_next(&it, &name)) != NULL) {
        RDB_parse_del_stmtlist(op->stmtlistp, &ec);
        for (i = 0; i < op->argnamec; i++) {
            RDB_free(op->argnamev[i]);
        }
        RDB_free(op->argnamev);
    }
    RDB_destroy_hashmap_iter(&it);
    RDB_destroy_hashmap(&ro_op_cache);

    if (txnp != NULL) {
        RDB_rollback(&ec, &txnp->tx);

        if (_RDB_parse_interactive)
            printf("Transaction rolled back.\n");
    }
    if (envp != NULL)
        RDB_close_env(envp);
    RDB_destroy_exec_context(&ec);
}

static int
println_string_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (puts(RDB_obj_string(argv[0])) == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
println_int_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%d\n", (int) RDB_obj_int(argv[0])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
println_float_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%f\n", (double) RDB_obj_float(argv[0])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
println_bool_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (puts(RDB_obj_bool(argv[0]) ? "TRUE" : "FALSE") == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
print_nonscalar_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object strobj;

    if (argc != 1) {
        RDB_raise_invalid_argument("invalid # of arguments", ecp);
        return RDB_ERROR;
    }
    if (RDB_obj_type(argv[0]) != NULL
            && RDB_type_is_scalar(RDB_obj_type(argv[0]))) {
        RDB_raise_type_mismatch(name, ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&strobj);
    if (_RDB_obj_to_str(&strobj, argv[0], ecp, txp) != RDB_OK) {
        RDB_destroy_obj(&strobj, ecp);
        return RDB_ERROR;
    }
    if (fputs(RDB_obj_string(&strobj), stdout) == EOF) {
        RDB_destroy_obj(&strobj, ecp);
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
        
    RDB_destroy_obj(&strobj, ecp);
    return RDB_OK;
}

static int
println_nonscalar_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (print_nonscalar_op("PRINT", argc, argv, updv, iargp, iarglen,
            ecp, txp) != RDB_OK)
        return RDB_ERROR;
    if (puts("") == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
print_string_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (fputs(RDB_obj_string(argv[0]), stdout) == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
print_int_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%d", (int) RDB_obj_int(argv[0])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
print_float_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%f", (double) RDB_obj_float(argv[0])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
print_bool_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (fputs(RDB_obj_bool(argv[0]) ? "TRUE" : "FALSE", stdout) == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
readln_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    char buf[128];
    size_t len;

    if (RDB_string_to_obj(argv[0], "", ecp) != RDB_OK)
        return RDB_ERROR;

    if (fgets(buf, sizeof(buf), stdin) == NULL) {
        if (ferror(stdin)) {
            _RDB_handle_errcode(errno, ecp, txp);
            return RDB_ERROR;
        }
        return RDB_OK;
    }
    len = strlen(buf);

    /* Read until a complete line has been read */
    while (buf[len - 1] != '\n') {
        if (RDB_append_string(argv[0], buf, ecp) != RDB_OK)
            return RDB_ERROR;
        if (fgets(buf, sizeof(buf), stdin) == NULL) {
            if (ferror(stdin)) {
                _RDB_handle_errcode(errno, ecp, txp);
                return RDB_ERROR;
            }
            return RDB_OK;
        }
        len = strlen(buf);
    }
    buf[len - 1] = '\0';
    return RDB_append_string(argv[0], buf, ecp);
}

static int
exit_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    Duro_exit_interp();
    exit(0);
}   

static int
exit_int_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    Duro_exit_interp();
    exit(RDB_obj_int(argv[0]));
}   

static int
connect_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret = RDB_open_env(RDB_obj_string(argv[1]), &envp);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, txp);
        envp = NULL;
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
create_db_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (envp == NULL) {
        RDB_raise_resource_not_found("no environment", ecp);
        return RDB_ERROR;
    }

    if (RDB_create_db_from_env(RDB_obj_string(argv[0]), envp, ecp) == NULL)
        return RDB_ERROR;
    return RDB_OK;
}   

static int
create_env_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    /* Create directory if does not exist */
    if (mkdir(RDB_obj_string(argv[0]),
            S_IREAD | S_IWRITE | S_IEXEC) == -1
            && errno != EEXIST) {
        RDB_raise_system(strerror(errno), ecp);
        return RDB_ERROR;
    }

    ret = RDB_open_env(RDB_obj_string(argv[0]), &envp);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, txp);
        envp = NULL;
        return RDB_ERROR;
    }
    return RDB_OK;
}   

static int
system_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret = system(RDB_obj_string(argv[0]));
    if (ret == -1 || ret == 127) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    RDB_int_to_obj(argv[1], (RDB_int) ret);
    return RDB_OK;
}

static int
load_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_seq_item *seqitv = NULL;
    int seqitc = 0;

    if (argc < 2) {
        RDB_raise_invalid_argument("Too few arguments for LOAD", ecp);
        return RDB_ERROR;
    }

    if (RDB_obj_type(argv[0]) == NULL
            || RDB_obj_type(argv[0])->kind != RDB_TP_ARRAY) {
        RDB_raise_type_mismatch("first argument must be array", ecp);
        goto error;
    }        

    if (argc > 2) {
        int i;
        
        if (argc % 2 != 0) {
            RDB_raise_invalid_argument("load", ecp);
            return RDB_ERROR;
        }
        seqitv = RDB_alloc(sizeof(RDB_seq_item) * (argc - 2), ecp);
        if (seqitv == NULL)
            return RDB_ERROR;
        seqitc = (argc - 2) / 2;
        for (i = 0; i < seqitc; i++) {
            if (RDB_obj_type(argv[2 + i * 2]) != &RDB_STRING) {
                RDB_raise_type_mismatch("invalid order", ecp);
                goto error;
            }
            if (RDB_obj_type(argv[2 + i * 2 + 1]) != &RDB_STRING) {
                RDB_raise_type_mismatch("invalid order", ecp);
                goto error;
            }
            seqitv[i].attrname = RDB_obj_string(argv[2 + i * 2]);
            seqitv[i].asc = (RDB_bool) (strcmp(RDB_obj_string(argv[2 + i * 2 + 1]), "ASC") == 0);
        }
    }

    if (RDB_table_to_array(argv[0], argv[1], seqitc, seqitv, 0, ecp, txp) != RDB_OK)
        goto error;
    RDB_free(seqitv);
    return RDB_OK;

error:
    RDB_free(seqitv);
    return RDB_ERROR;
}   

static RDB_object *
new_obj(RDB_exec_context *ecp)
{
    RDB_object *objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        return NULL;
    }
    RDB_init_obj(objp);
    return objp;
}

static int
put_op(RDB_op_map *opmap, const char *name, int argc, RDB_type **argtv,
        RDB_upd_op_func *opfp, RDB_bool *updv, RDB_exec_context *ecp)
{
    RDB_op_data *datap = RDB_alloc(sizeof(RDB_op_data), ecp);
    if (datap == NULL)
        return RDB_ERROR;
    RDB_init_obj(&datap->iarg);
    datap->modhdl = NULL;
    datap->opfn.upd_fp = opfp;
    datap->rtyp = NULL;
    datap->updv = updv;

    if (RDB_put_op(opmap, name, argc, argtv, datap, ecp) != RDB_OK) {
        RDB_destroy_obj(&datap->iarg, ecp);
        RDB_free(datap);
        return RDB_ERROR;
    }
    return RDB_OK;
}

int
Duro_init_exec(RDB_exec_context *ecp, const char *dbname)
{
    int i;

    static RDB_type *print_string_types[1];
    static RDB_type *print_int_types[1];
    static RDB_type *print_float_types[1];
    static RDB_type *print_bool_types[1];
    static RDB_type *readln_types[1];
    static RDB_type *exit_int_types[1];
    static RDB_type *connect_types[2];
    static RDB_type *create_db_types[1];
    static RDB_type *create_env_types[1];
    static RDB_type *system_types[2];

    static RDB_bool print_updv[] = { RDB_FALSE };
    static RDB_bool readln_updv[] = { RDB_TRUE };
    static RDB_bool exit_int_updv[] = { RDB_FALSE };
    static RDB_bool connect_updv[] = { RDB_FALSE, RDB_FALSE };
    static RDB_bool create_db_updv[] = { RDB_FALSE };
    static RDB_bool create_env_updv[] = { RDB_FALSE };
    static RDB_bool system_updv[] = { RDB_FALSE, RDB_TRUE };
    static RDB_bool load_updv[DURO_MAX_LLEN];

    RDB_object *objp;

    print_string_types[0] = &RDB_STRING;
    print_int_types[0] = &RDB_INTEGER;
    print_float_types[0] = &RDB_FLOAT;
    print_bool_types[0] = &RDB_BOOLEAN;
    readln_types[0] = &RDB_STRING;
    exit_int_types[0] = &RDB_INTEGER;
    connect_types[0] = &RDB_STRING;
    connect_types[1] = &RDB_STRING;
    create_db_types[0] = &RDB_STRING;
    create_env_types[0] = &RDB_STRING;
    system_types[0] = &RDB_STRING;
    system_types[1] = &RDB_INTEGER;

    RDB_init_hashmap(&toplevel_vars.map, 256);
    toplevel_vars.parentp = NULL;
    current_varmapp = &toplevel_vars;

    RDB_init_op_map(&opmap);

    if (put_op(&opmap, "PRINTLN", 1, print_string_types, &println_string_op,
            print_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "PRINTLN", 1, print_int_types, &println_int_op, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "PRINTLN", 1, print_float_types, &println_float_op, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "PRINTLN", 1, print_bool_types, &println_bool_op, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "PRINTLN", -1, NULL, &println_nonscalar_op, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "PRINT", 1, print_string_types, &print_string_op, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "PRINT", 1, print_int_types, &print_int_op, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "PRINT", 1, print_float_types, &print_float_op, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "PRINT", 1, print_bool_types, &print_bool_op, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "PRINT", -1, NULL, &print_nonscalar_op, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "READLN", 1, readln_types, &readln_op, readln_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "EXIT", 0, NULL, &exit_op, NULL, ecp) != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "EXIT", 1, exit_int_types, &exit_int_op, exit_int_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "CONNECT", 2, connect_types, &connect_op, connect_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "CREATE_DB", 1, create_db_types, &create_db_op, create_db_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "CREATE_ENV", 1, create_env_types, &create_env_op, create_env_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (put_op(&opmap, "SYSTEM", 2, system_types, &system_op, system_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;

    load_updv[0] = RDB_TRUE;
    for (i = 1; i < DURO_MAX_LLEN; i++) {
        load_updv[i] = RDB_FALSE;
    }
    if (put_op(&opmap, "LOAD", -1, NULL, &load_op, load_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;

    objp = new_obj(ecp);
    if (objp == NULL)
        return RDB_ERROR;

    if (RDB_string_to_obj(objp, dbname, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (RDB_hashmap_put(&toplevel_vars.map, "CURRENT_DB", objp) != RDB_OK) {
        RDB_destroy_obj(objp, ecp);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    RDB_init_hashmap(&ro_op_cache, 256);

    return RDB_OK;
}

static int
init_obj(RDB_object *, RDB_type *, RDB_exec_context *, RDB_transaction *);

static int
init_obj_by_selector(RDB_object *objp, RDB_possrep *rep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object *objv;
    RDB_object **objpv;

    objv = RDB_alloc(sizeof(RDB_object) * rep->compc, ecp);
    if (objv == NULL) {
        return RDB_ERROR;
    }
    for (i = 0; i < rep->compc; i++)
        RDB_init_obj(&objv[i]);
    objpv = RDB_alloc(sizeof(RDB_object *) * rep->compc, ecp);
    if (objpv == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* Get selector arguments */
    for (i = 0; i < rep->compc; i++) {
        ret = init_obj(&objv[i], rep->compv[i].typ, ecp, txp);
        if (ret != RDB_OK)
            goto cleanup;
        objpv[i] = &objv[i];
    }

    /* Call selector */
    ret = RDB_call_ro_op(rep->name, rep->compc, objpv, ecp, txp, objp);

cleanup:
    for (i = 0; i < rep->compc; i++)
        RDB_destroy_obj(&objv[i], ecp);
    RDB_free(objv);
    RDB_free(objpv);

    return ret;
}

static int
init_obj(RDB_object *objp, RDB_type *typ, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;

    if (typ == &RDB_BOOLEAN) {
        RDB_bool_to_obj(objp, RDB_FALSE);
    } else if (typ == &RDB_INTEGER) {
        RDB_int_to_obj(objp, 0);
    } else if (typ == &RDB_FLOAT) {
        RDB_float_to_obj(objp, 0.0);
    } else if (typ == &RDB_STRING) {
        return RDB_string_to_obj(objp, "", ecp);
    } else if (typ == &RDB_BINARY) {
        return RDB_binary_set(objp, 0, NULL, (size_t) 0, ecp);
    } else if (RDB_type_is_tuple(typ)) {
        for (i = 0; i < typ->var.tuple.attrc; i++) {
            if (RDB_tuple_set(objp, typ->var.tuple.attrv[i].name,
                    NULL, ecp) != RDB_OK)
                return RDB_ERROR;
            if (init_obj(RDB_tuple_get(objp, typ->var.tuple.attrv[i].name),
                    typ->var.tuple.attrv[i].typ, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        }
        typ = RDB_dup_nonscalar_type(typ, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        RDB_obj_set_typeinfo(objp, typ);
    } else if (typ->kind == RDB_TP_ARRAY) {
        typ = RDB_dup_nonscalar_type(typ, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        RDB_obj_set_typeinfo(objp, typ);
    } else {
        if (typ->var.scalar.repc > 0) {
            if (txp == NULL) {
                RDB_raise_no_running_tx(ecp);
                return RDB_ERROR;
            }
            return init_obj_by_selector(objp, &typ->var.scalar.repv[0],
                    ecp, txp);
        }
    }
    return RDB_OK;
}

static RDB_type *
expr_to_type(RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int attrc;
    RDB_attr *attrv;
    RDB_expression *argp;
    RDB_type *typ = NULL;

    if (exp->kind == RDB_EX_VAR) {
        /* CHAR can be used for STRING */
        if (strcmp(exp->var.varname, "CHAR") == 0)
            return &RDB_STRING;
        return RDB_get_type(exp->var.varname, ecp, txp);
    }

    if (exp->kind != RDB_EX_RO_OP) {
        RDB_raise_invalid_argument("invalid type definition", ecp);
        return NULL;
    }

    if (strcmp(exp->var.op.name, "ARRAY") == 0) {
        typ = expr_to_type(exp->var.op.args.firstp, ecp, txp);
        if (typ == NULL)
            return NULL;
        return RDB_create_array_type(typ, ecp);
    }
    if (strcmp(exp->var.op.name, "SAME_TYPE_AS") == 0) {
        typ = RDB_expr_type(exp->var.op.args.firstp, get_var_type,
                current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL);
        if (typ == NULL)
            return NULL;
        return RDB_dup_nonscalar_type(typ, ecp);
    }
    if (strcmp(exp->var.op.name, "RELATION_SAME_HEADING_AS") == 0) {
        typ = RDB_expr_type(exp->var.op.args.firstp, get_var_type,
                current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL);
        if (typ == NULL)
            return NULL;
        if (RDB_type_is_relation(typ)) {
            return RDB_dup_nonscalar_type(typ, ecp);
        }
        if (RDB_type_is_tuple(typ)) {
            typ = RDB_dup_nonscalar_type(typ, ecp);
            if (typ == NULL)
                return NULL;
            return RDB_create_relation_type_from_base(typ, ecp);
        }
        RDB_raise_invalid_argument("tuple or relation type required", ecp);
        return NULL;
    }
    if (strcmp(exp->var.op.name, "TUPLE_SAME_HEADING_AS") == 0) {
        typ = RDB_expr_type(exp->var.op.args.firstp, get_var_type,
                current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL);
        if (typ == NULL)
            return NULL;
        if (RDB_type_is_relation(typ)) {
            return RDB_dup_nonscalar_type(typ->var.basetyp, ecp);
        }
        if (RDB_type_is_tuple(typ)) {
            return RDB_dup_nonscalar_type(typ, ecp);
        }
        RDB_raise_invalid_argument("tuple or relation type required", ecp);
        return NULL;
    }

    attrc = RDB_expr_list_length(&exp->var.op.args);
    if (attrc % 2 != 0) {
        RDB_raise_invalid_argument("invalid type definition", ecp);
        return NULL;
    }
    attrc /= 2;

    attrv = RDB_alloc(attrc * sizeof(RDB_attr), ecp);
    if (attrv == NULL)
        return NULL;
    for (i = 0; i < attrc; i++) {
        attrv[i].typ = NULL;
    }

    argp = exp->var.op.args.firstp;
    for (i = 0; i < attrc; i++) {
        if (argp->kind != RDB_EX_VAR) {
            RDB_raise_invalid_argument("invalid type definition", ecp);
            goto cleanup;
        }
        attrv[i].name = argp->var.varname;
        argp = argp->nextp;
        attrv[i].typ = expr_to_type(argp, ecp, txp);
        if (attrv[i].typ == NULL)
            goto cleanup;
        argp = argp->nextp;
    }

    if (strcmp(exp->var.op.name, "TUPLE") == 0)
        typ = RDB_create_tuple_type(attrc, attrv, ecp);
    else if (strcmp(exp->var.op.name, "RELATION") == 0)
        typ = RDB_create_relation_type(attrc, attrv, ecp);
    else
        RDB_raise_invalid_argument("invalid type definition", ecp);

cleanup:
    for (i = 0; i < attrc; i++) {
        if (attrv[i].typ != NULL && !RDB_type_is_scalar(attrv[i].typ))
            RDB_drop_type(attrv[i].typ, ecp, NULL);
    }
    RDB_free(attrv);
    return typ;
}

static int
eval_parse_type(RDB_parse_type *ptyp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (ptyp->typ != NULL)
        return RDB_OK;

    ptyp->typ = expr_to_type(ptyp->exp, ecp, txp);
    if (ptyp->typ == NULL)
        return RDB_ERROR;

    return RDB_OK;
}

static int
exec_vardef(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_object *objp;
    RDB_type *typ;
    char *varname = RDB_obj_string(&stmtp->var.vardef.varname);
    RDB_transaction *txp = txnp != NULL ? &txnp->tx : NULL;

    /*
     * Check if the variable already exists
     */
    if (RDB_hashmap_get(&current_varmapp->map, varname) != NULL) {
        RDB_raise_element_exists(varname, ecp);
        return RDB_ERROR;
    }

    objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        return RDB_ERROR;
    }
    RDB_init_obj(objp);

    if (stmtp->var.vardef.type.exp != NULL) {
        if (eval_parse_type(&stmtp->var.vardef.type, ecp, txp) != RDB_OK) {
            goto error;
        }
    }

    if (stmtp->var.vardef.exp == NULL) {
        if (init_obj(objp, stmtp->var.vardef.type.typ, ecp, txp) != RDB_OK) {
            goto error;
        }
    } else {
        if (RDB_evaluate(stmtp->var.vardef.exp, &get_var,
                current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL,
                objp) != RDB_OK) {
            goto error;
        }
        if (RDB_obj_type(objp) != NULL) {
            /* Check type if type was given */
            if (stmtp->var.vardef.type.typ != NULL &&
                    !RDB_type_equals(stmtp->var.vardef.type.typ, RDB_obj_type(objp))) {
                RDB_raise_type_mismatch("", ecp);
                goto error;
            }
        } else {
            /* No type available (tuple or array) - set type */
            typ = RDB_expr_type(stmtp->var.vardef.exp, get_var_type,
                    current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL);
            if (typ == NULL)
                goto error;
            typ = RDB_dup_nonscalar_type(typ, ecp);
            if (typ == NULL)
                goto error;
            RDB_obj_set_typeinfo(objp, typ);
        }
    }
    if (RDB_hashmap_put(&current_varmapp->map, varname, objp) != RDB_OK) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    return RDB_OK;

error:
    RDB_destroy_obj(objp, ecp);
    RDB_free(objp);
    return RDB_ERROR;
}

static RDB_string_vec *
keylist_to_keyv(RDB_parse_keydef *firstkeyp, int *keycp, RDB_exec_context *ecp)
{
    int i, j;
    RDB_string_vec *keyv;
    RDB_expression *keyattrp;
    RDB_parse_keydef *keyp = firstkeyp;
    *keycp = 0;
    while (keyp != NULL) {
        (*keycp)++;
        keyp = keyp->nextp;
    }

    keyv = RDB_alloc(sizeof(RDB_string_vec) * (*keycp), ecp);
    if (keyv == NULL) {
        return NULL;
    }
    keyp = firstkeyp;
    for (i = 0; i < *keycp; i++) {
        keyv[i].strc = RDB_expr_list_length(&keyp->attrlist);
        keyv[i].strv = RDB_alloc(keyv[i].strc * sizeof(char *), ecp);
        if (keyv[i].strv == NULL) {
            return NULL;
        }
        keyattrp = keyp->attrlist.firstp;
        for (j = 0; j < keyv[i].strc; j++) {
            keyv[i].strv[j] = RDB_obj_string(RDB_expr_obj(keyattrp));
            keyattrp = keyattrp->nextp;
        }
        keyp = keyp->nextp;
    }
    return keyv;
}

static int
exec_vardef_real(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_string_vec *keyv;
    RDB_bool freekey;
    RDB_object tb;
    RDB_type *tbtyp = NULL;
    RDB_object *tbp = NULL;
    int keyc = 0;
    char *varname = RDB_obj_string(&stmtp->var.vardef.varname);

    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (stmtp->var.vardef.type.exp != NULL) {
        if (eval_parse_type(&stmtp->var.vardef.type, ecp, NULL) != RDB_OK)
            goto error;
    }

    if (stmtp->var.vardef.exp != NULL) {        
        RDB_init_obj(&tb);
        if (RDB_evaluate(stmtp->var.vardef.exp, get_var, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : NULL, &tb) != RDB_OK) {
            goto error;
        }
        if (RDB_obj_type(&tb) == NULL) {
            RDB_raise_type_mismatch("relation type required", ecp);
            goto error;
        }
    }

    if (stmtp->var.vardef.type.typ != NULL) {
        tbtyp = RDB_dup_nonscalar_type(stmtp->var.vardef.type.typ, ecp);
    } else {
        tbtyp = RDB_dup_nonscalar_type(RDB_obj_type(&tb), ecp);
    }
    if (tbtyp == NULL)
        goto error;

    if (stmtp->var.vardef.firstkeyp != NULL) {
        keyv = keylist_to_keyv(stmtp->var.vardef.firstkeyp, &keyc, ecp);
        if (keyv == NULL)
            return RDB_ERROR;
    } else {
        /* Get keys from expression */

        keyc = _RDB_infer_keys(stmtp->var.vardef.exp, ecp, &keyv, &freekey);
        if (keyc == RDB_ERROR)
            return RDB_ERROR;
    }

    tbp = RDB_create_table_from_type(varname, tbtyp, keyc, keyv, ecp, &txnp->tx);
    if (tbp == NULL) {
        goto error;
    }

    if (stmtp->var.vardef.exp != NULL) {        
        if (_RDB_move_tuples(tbp, &tb, ecp, &txnp->tx) == (RDB_int) RDB_ERROR)
            goto error;
    }

    if (_RDB_parse_interactive)
        printf("Table %s created.\n", varname);

    if (stmtp->var.vardef.exp != NULL) {
        RDB_destroy_obj(&tb, ecp);
    }
    return RDB_OK;

error:
    {
        RDB_exec_context ec;
        
        RDB_init_exec_context(&ec);
        if (tbp != NULL) {
            RDB_drop_table(tbp, &ec, &txnp->tx);
        } else if (tbtyp != NULL && !RDB_type_is_scalar(tbtyp)) {
            RDB_drop_type(tbtyp, &ec, NULL);
        }
        if (stmtp->var.vardef.exp != NULL) {        
            RDB_destroy_obj(&tb, &ec);
        }
        RDB_destroy_exec_context(&ec);
    }
    return RDB_ERROR;
}

static int
exec_vardef_virtual(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    char *varname = RDB_obj_string(&stmtp->var.vardef.varname);
    RDB_expression *texp = RDB_dup_expr(stmtp->var.vardef.exp,
            ecp);
    if (texp == NULL)
        return RDB_ERROR;

    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
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
exec_vardef_private(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    RDB_string_vec *keyv;
    RDB_bool freekey;
    RDB_type *tbtyp = NULL;
    RDB_object tb;
    int keyc = 0;
    char *varname = RDB_obj_string(&stmtp->var.vardef.varname);

    if (stmtp->var.vardef.type.exp != NULL) {
        if (eval_parse_type(&stmtp->var.vardef.type, ecp, NULL) != RDB_OK)
            return RDB_ERROR;
    }

    if (stmtp->var.vardef.exp != NULL) {        
        RDB_init_obj(&tb);
        if (RDB_evaluate(stmtp->var.vardef.exp, get_var, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : NULL, &tb) != RDB_OK) {
            goto error;
        }
        if (RDB_obj_type(&tb) == NULL) {
            RDB_raise_type_mismatch("relation type required", ecp);
            goto error;
        }
    }
    
    if (stmtp->var.vardef.type.typ != NULL) {
        tbtyp = RDB_dup_nonscalar_type(stmtp->var.vardef.type.typ, ecp);
    } else {
        tbtyp = RDB_dup_nonscalar_type(RDB_obj_type(&tb), ecp);
    }
    if (tbtyp == NULL)
        goto error;

    if (stmtp->var.vardef.firstkeyp != NULL) {
        keyv = keylist_to_keyv(stmtp->var.vardef.firstkeyp, &keyc, ecp);
        if (keyv == NULL)
            goto error;
    } else {
        /* Get keys from expression */
        keyc = _RDB_infer_keys(stmtp->var.vardef.exp, ecp, &keyv, &freekey);
        if (keyc == RDB_ERROR) {
            if (stmtp->var.vardef.exp != NULL) {        
                RDB_destroy_obj(&tb, ecp);
            }
            return RDB_ERROR;
        }
    }

    tbp = RDB_alloc(sizeof(RDB_object), ecp);
    if (tbp == NULL) {
        goto error;
    }

    RDB_init_obj(tbp);
    if (RDB_init_table_from_type(tbp, varname, tbtyp, keyc, keyv, ecp)
            != RDB_OK) {
        RDB_destroy_obj(tbp, ecp);
        RDB_free(tbp);
        goto error;
    }

    if (stmtp->var.vardef.exp != NULL) {        
        if (RDB_copy_obj(tbp, &tb, ecp) != RDB_OK) {
            goto error;
        }        
    }

    if (RDB_hashmap_put(&current_varmapp->map, varname, tbp) != RDB_OK) {
        RDB_destroy_obj(tbp, ecp);
        RDB_free(tbp);
        RDB_raise_no_memory(ecp);
        goto error;
    }

    if (_RDB_parse_interactive)
        printf("Local table %s created.\n", varname);

    if (stmtp->var.vardef.exp != NULL) {        
        RDB_destroy_obj(&tb, ecp);
    }
    return RDB_OK;

error:
    if (tbtyp != NULL && !RDB_type_is_scalar(tbtyp))
        RDB_drop_type(tbtyp, ecp, NULL);
    if (stmtp->var.vardef.exp != NULL) {        
        RDB_destroy_obj(&tb, ecp);
    }
    return RDB_ERROR;
}

static int
exec_vardrop(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    char *varname = RDB_obj_string(&stmtp->var.vardrop.varname);

    /* Try to look up local variable */
    RDB_object *objp = RDB_hashmap_get(&current_varmapp->map, varname);
    if (objp != NULL) {
        /* Local variable found */
        if (RDB_hashmap_put(&current_varmapp->map, varname, NULL) != RDB_OK)
            return RDB_ERROR;
        return drop_local_var(objp, ecp);
    }

    /*
     * Delete persistent table
     */
    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    objp = RDB_get_table(varname, ecp, &txnp->tx);
    if (objp == NULL)
        return RDB_ERROR;

    if (RDB_drop_table(objp, ecp, &txnp->tx) != RDB_OK)
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
    RDB_op_data *op;
    RDB_expression *argp;
    char *opname = RDB_obj_string(&stmtp->var.call.opname);

    /*
     * Get argument types
     */
    argp = stmtp->var.call.arglist.firstp;
    i = 0;
    while (argp != NULL) {
        if (i >= DURO_MAX_LLEN) {
            RDB_raise_not_supported("too many arguments", ecp);
            return RDB_ERROR;
        }
        argtv[i] = RDB_expr_type(argp, &get_var_type, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : NULL);
        if (argtv[i] == NULL)
            return RDB_ERROR;
        argp = argp->nextp;
        i++;
    }
    argc = i;

    /*
     * Get operator
     */
    op = RDB_get_op(&opmap, opname, argc, argtv, ecp);
    if (op == NULL) {
        if (txnp == NULL) {
            return RDB_ERROR;
        }
        RDB_clear_err(ecp);

        op = _RDB_get_upd_op(opname, argc, argtv, ecp, &txnp->tx);
        if (op == NULL) {
            return RDB_ERROR;
        }
    }

    for (i = 0; i < argc; i++) {
        argpv[i] = NULL;
    }

    argp = stmtp->var.call.arglist.firstp;
    i = 0;
    while (argp != NULL) {
        if (op->updv[i]) {
            /*
             * Update argument - lookup variable
             */
            if (argp->kind == RDB_EX_VAR) {
                argpv[i] = lookup_var(argp->var.varname, ecp);
            } else if (argp->kind == RDB_EX_TBP) {
                argpv[i] = argp->var.tbref.tbp;
            } else {
                RDB_raise_invalid_argument(
                        "update argument must be a variable", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        } else {
            RDB_init_obj(&argv[i]);
            if (RDB_evaluate(argp, &get_var, current_varmapp,
                    ecp, txnp != NULL ? &txnp->tx : NULL, &argv[i]) != RDB_OK) {
                ret = RDB_ERROR;
                goto cleanup;
            }
            argpv[i] = &argv[i];
        }            
        argp = argp->nextp;
        i++;
    }

    /* Invoke function */
    ret = (*op->opfn.upd_fp) (opname, argc, argpv, op->updv,
            op->iarg.var.bin.datap, op->iarg.var.bin.len, ecp,
            txnp != NULL ? &txnp->tx : NULL);

cleanup:
    for (i = 0; i < argc; i++) {
        if ((argpv[i] != NULL) && !op->updv[i]
                && (argpv[i]->kind != RDB_OB_TABLE
                    || !RDB_table_is_persistent(argpv[i])))
            RDB_destroy_obj(&argv[i], ecp);
    }
    return ret;
}

static int
Duro_exec_stmt(RDB_parse_statement *, RDB_exec_context *, RDB_object *);

static int
exec_stmtlist(RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    int ret;
    do {
        ret = Duro_exec_stmt(stmtp, ecp, retvalp);
        if (ret != RDB_OK)
            return ret;
        stmtp = stmtp->nextp;
    } while (stmtp != NULL);
    return RDB_OK;
}

static int
exec_if(const RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    RDB_bool b;
    int ret;

    if (RDB_evaluate_bool(stmtp->var.ifthen.condp, &get_var, current_varmapp,
            ecp, NULL, &b) != RDB_OK)
        return RDB_ERROR;
    if (add_varmap(ecp) != RDB_OK)
        return RDB_ERROR;
    if (b) {
        ret = exec_stmtlist(stmtp->var.ifthen.ifp, ecp, retvalp);
    } else if (stmtp->var.ifthen.elsep != NULL) {
        ret = exec_stmtlist(stmtp->var.ifthen.elsep, ecp, retvalp);
    } else {
        ret = RDB_OK;
    }
    remove_varmap();
    return ret;
}

static int
exec_while(const RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    RDB_bool b;

    for(;;) {
        if (RDB_evaluate_bool(stmtp->var.whileloop.condp, &get_var, current_varmapp,
                ecp, NULL, &b) != RDB_OK)
            return RDB_ERROR;
        if (!b)
            return RDB_OK;
        if (add_varmap(ecp) != RDB_OK)
            return RDB_ERROR;
        if (exec_stmtlist(stmtp->var.whileloop.bodyp, ecp, retvalp) != RDB_OK) {
            remove_varmap();
            return RDB_ERROR;
        }
        remove_varmap();
        if (interrupted) {
            interrupted = 0;
            RDB_raise_system("interrupted", ecp);
            return RDB_ERROR;
        }
    }
}

static int
exec_for(const RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    RDB_object *varp;
    RDB_object endval;

    if (stmtp->var.forloop.varexp->kind != RDB_EX_VAR) {
        RDB_raise_syntax("variable name expected", ecp);
        return RDB_ERROR;
    }
    varp = lookup_local_var(stmtp->var.forloop.varexp->var.varname, current_varmapp);
    if (varp == NULL) {
        RDB_raise_name(stmtp->var.forloop.varexp->var.varname, ecp);
        return RDB_ERROR;
    }
    if (RDB_obj_type(varp) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("loop variable must be INTEGER", ecp);
        return RDB_ERROR;
    }
    if (RDB_evaluate(stmtp->var.forloop.fromp, &get_var,
            current_varmapp, ecp, NULL, varp) != RDB_OK) {
        return RDB_ERROR;
    }
    RDB_init_obj(&endval);
    if (RDB_evaluate(stmtp->var.forloop.top, &get_var,
            current_varmapp, ecp, NULL, &endval) != RDB_OK) {
        RDB_destroy_obj(&endval, ecp);
        return RDB_ERROR;
    }
    if (RDB_obj_type(&endval) != &RDB_INTEGER) {
        RDB_destroy_obj(&endval, ecp);
        RDB_raise_type_mismatch("expression must be INTEGER", ecp);
        return RDB_ERROR;
    }

    for(;;) {
        if (add_varmap(ecp) != RDB_OK)
            return RDB_ERROR;
        if (exec_stmtlist(stmtp->var.forloop.bodyp, ecp, retvalp) != RDB_OK) {
            remove_varmap();
            RDB_destroy_obj(&endval, ecp);
            return RDB_ERROR;
        }
        remove_varmap();
        if (varp->var.int_val == endval.var.int_val)
            break;
        if (interrupted) {
            interrupted = 0;
            RDB_destroy_obj(&endval, ecp);
            RDB_raise_system("interrupted", ecp);
            return RDB_ERROR;
        }
        varp->var.int_val++;
    }
    RDB_destroy_obj(&endval, ecp);
    return RDB_OK;
}

static int
attr_assign_list_length(const RDB_parse_assign *assignlp)
{
    int assignc = 0;
    const RDB_parse_assign *ap = assignlp;

    do {
        assignc++;
        ap = ap->nextp;
    } while (ap != NULL);
    return assignc;
}

static RDB_attr_update *
convert_attr_assigns(RDB_parse_assign *assignlp, int *updcp,
        RDB_exec_context *ecp)
{
    int i;
    RDB_attr_update *updv;
    RDB_parse_assign *ap = assignlp;
    *updcp = attr_assign_list_length(assignlp);

    updv = RDB_alloc(*updcp * sizeof(RDB_attr_update), ecp);
    if (updv == NULL)
        return NULL;
    ap = assignlp;
    for (i = 0; i < *updcp; i++) {
        updv[i].name = ap->var.copy.dstp->var.varname;
        updv[i].exp = ap->var.copy.srcp;
        ap = ap->nextp;
    }
    return updv;
}

static int
exec_assign(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i;
    int cnt;
    RDB_expression *rexp;
    RDB_object *dstp;
    RDB_ma_copy copyv[DURO_MAX_LLEN];
    RDB_ma_insert insv[DURO_MAX_LLEN];
    RDB_ma_update updv[DURO_MAX_LLEN];
    RDB_ma_delete delv[DURO_MAX_LLEN];
    RDB_object srcobjv[DURO_MAX_LLEN];
    int copyc = 0;
    int insc = 0;
    int updc = 0;
    int delc = 0;
    int srcobjc = 0;
    RDB_parse_assign *assignp = stmtp->var.assignment.assignp;

    while (assignp != NULL) {
        switch (assignp->kind) {
            case RDB_STMT_COPY:
                if (copyc >= DURO_MAX_LLEN) {
                    RDB_raise_not_supported("too many assignents", ecp);
                    return RDB_ERROR;
                }
                if (assignp->var.copy.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid assignment target", ecp);
                    goto error;
                }
                copyv[copyc].dstp = lookup_var(
                        assignp->var.copy.dstp->var.varname,
                        ecp);
                if (copyv[copyc].dstp == NULL) {
                    goto error;
                }
                RDB_init_obj(&srcobjv[srcobjc++]);
                copyv[copyc].srcp = &srcobjv[srcobjc - 1];
                if (RDB_evaluate(assignp->var.copy.srcp, get_var,
                        current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL, &srcobjv[srcobjc - 1])
                               != RDB_OK) {
                    goto error;
                }
                copyc++;
                break;
            case RDB_STMT_INSERT:
                if (insc >= DURO_MAX_LLEN) {
                    RDB_raise_not_supported("too many assignents", ecp);
                    return RDB_ERROR;
                }
                if (assignp->var.copy.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid assignment target", ecp);
                    goto error;
                }
                insv[insc].tbp = lookup_var(
                        assignp->var.ins.dstp->var.varname,
                        ecp);
                if (insv[insc].tbp == NULL) {
                    goto error;
                }
                /* Only tables are allowed as target */
                if (insv[insc].tbp->typ == NULL
                        || !RDB_type_is_relation(insv[insc].tbp->typ)) {
                    RDB_raise_type_mismatch("INSERT target must be relation", ecp);
                    goto error;
                }
                RDB_init_obj(&srcobjv[srcobjc++]);
                insv[insc].objp = &srcobjv[srcobjc - 1];
                rexp = RDB_expr_resolve_varnames(assignp->var.ins.srcp,
                        &get_var, current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL);
                if (rexp == NULL)
                    goto error;
                if (RDB_evaluate(rexp, NULL, NULL, ecp,
                            txnp != NULL ? &txnp->tx : NULL, &srcobjv[srcobjc - 1]) != RDB_OK) {
                    RDB_drop_expr(rexp, ecp);
                    goto error;
                }
                RDB_drop_expr(rexp, ecp);
                insc++;
                break;
            case RDB_STMT_UPDATE:
                if (updc >= DURO_MAX_LLEN) {
                    RDB_raise_not_supported("too many assignents", ecp);
                    return RDB_ERROR;
                }
                if (assignp->var.upd.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid UPDATE target", ecp);
                    goto error;
                }
                dstp = lookup_var(assignp->var.upd.dstp->var.varname,
                        ecp);
                if (dstp == NULL) {
                    goto error;
                }
                if (RDB_type_is_relation(dstp->typ)) {
                    updv[updc].tbp = dstp;
                    updv[updc].updv = convert_attr_assigns(
                            assignp->var.upd.assignlp,
                            &updv[updc].updc, ecp);
                    if (updv[updc].updv == NULL) {
                        RDB_raise_no_memory(ecp);
                        goto error;
                    }
                    updv[updc].condp = assignp->var.upd.condp;
                    updc++;
                } else if (RDB_type_is_tuple(dstp->typ)) {
                    RDB_parse_assign *ap;
                    if (assignp->var.upd.condp != NULL) {
                        RDB_raise_syntax("WHERE not allowed with tuple UPDATE", ecp);
                        goto error;
                    }
                    ap = assignp->var.upd.assignlp;
                    while (ap != NULL) {
                        copyv[copyc].dstp = RDB_tuple_get(dstp, ap->var.copy.dstp->var.varname);
                        if (copyv[copyc].dstp == NULL) {
                            RDB_raise_invalid_argument("target attribute not found", ecp);
                            goto error;
                        }
                        RDB_init_obj(&srcobjv[srcobjc++]);
                        copyv[copyc].srcp = &srcobjv[srcobjc - 1];
                        if (RDB_evaluate(ap->var.copy.srcp, get_var, current_varmapp, ecp,
                                txnp != NULL ? &txnp->tx : NULL, &srcobjv[srcobjc - 1])
                                       != RDB_OK) {
                            goto error;
                        }
                        ap = ap->nextp;
                        copyc++;
                    }
                } else {
                    RDB_raise_syntax("invalid UPDATE target", ecp);
                    goto error;
                }
                break;
            case RDB_STMT_DELETE:
                if (delc >= DURO_MAX_LLEN) {
                    RDB_raise_not_supported("too many assignents", ecp);
                    return RDB_ERROR;
                }
                if (assignp->var.del.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid assignment target", ecp);
                    goto error;
                }
                delv[delc].tbp = lookup_var(
                        assignp->var.del.dstp->var.varname,
                        ecp);
                if (delv[delc].tbp == NULL) {
                    goto error;
                }
                /* Only tables are allowed as target */
                if (delv[delc].tbp->typ == NULL
                        || !RDB_type_is_relation(delv[delc].tbp->typ)) {
                    RDB_raise_type_mismatch("DELETE target must be relation", ecp);
                    goto error;
                }
                delv[delc].condp = assignp->var.del.condp;
                delc++;
                break;                
        }
        assignp = assignp->nextp;
    }
    cnt = RDB_multi_assign(insc, insv, updc, updv, delc, delv, copyc, copyv,
            ecp, txnp != NULL ? &txnp->tx : NULL);
    if (cnt == (RDB_int) RDB_ERROR)
        goto error;

    if (_RDB_parse_interactive) {
        if (cnt == 1) {
            printf("1 element affected.\n");
        } else {
            printf("%d elements affected.\n", (int) cnt);
        }
    }

    for (i = 0; i < srcobjc; i++)
        RDB_destroy_obj(&srcobjv[i], ecp);

    return RDB_OK;

error:
    for (i = 0; i < srcobjc; i++)
        RDB_destroy_obj(&srcobjv[i], ecp);
    return RDB_ERROR;
}

static RDB_database *
get_db(RDB_exec_context *ecp)
{
    char *dbname;
    RDB_object *dbnameobjp = RDB_hashmap_get(&toplevel_vars.map, "CURRENT_DB");
    if (dbnameobjp == NULL) {
        RDB_raise_resource_not_found("no database", ecp);
        return NULL;
    }
    dbname = RDB_obj_string(dbnameobjp);
    if (*dbname == '\0') {
        RDB_raise_resource_not_found("no database", ecp);
        return NULL;
    }
    if (envp == NULL) {
        RDB_raise_resource_not_found("no connection", ecp);
        return NULL;
    }

    return RDB_get_db_from_env(dbname, envp, ecp);
}

static int
exec_begin_tx(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_database *dbp = get_db(ecp);
    if (dbp == NULL)
        return RDB_ERROR;

    if (txnp != NULL) {
        tx_node *ntxnp = RDB_alloc(sizeof(tx_node), ecp);
        if (ntxnp == NULL) {        
            return RDB_ERROR;
        }

        if (RDB_begin_tx(ecp, &ntxnp->tx, dbp, &txnp->tx) != RDB_OK) {
            RDB_free(ntxnp);
            return RDB_ERROR;
        }
        ntxnp->parentp = txnp;
        txnp = ntxnp;

        if (_RDB_parse_interactive)
            printf("Subtransaction started.\n");
        return RDB_OK;
    }


    txnp = RDB_alloc(sizeof(tx_node), ecp);
    if (txnp == NULL) {        
        return RDB_ERROR;
    }

    if (RDB_begin_tx(ecp, &txnp->tx, dbp, NULL) != RDB_OK) {
        RDB_free(txnp);
        txnp = NULL;
        return RDB_ERROR;
    }
    txnp->parentp = NULL;

    if (_RDB_parse_interactive)
        printf("Transaction started.\n");
    return RDB_OK;
}

static int
exec_commit(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    tx_node *ptxnp;

    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (RDB_commit(ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    ptxnp = txnp->parentp;
    RDB_free(txnp);
    txnp = ptxnp;

    if (_RDB_parse_interactive)
        printf("Transaction committed.\n");
    return RDB_OK;
}

static int
exec_rollback(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (RDB_rollback(ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    RDB_free(txnp);
    txnp = NULL;

    if (_RDB_parse_interactive)
        printf("Transaction rolled back.\n");
    return RDB_OK;
}

static int
parserep_to_rep(const RDB_parse_possrep *prep, RDB_possrep *rep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *exp;
    
    rep->name = prep->namexp != NULL ? RDB_obj_string(&prep->namexp->var.obj) : NULL;
    rep->compc = RDB_expr_list_length(&prep->attrlist) / 2;

    rep->compv = RDB_alloc(rep->compc * sizeof(RDB_attr), ecp);
    if (rep->compv == NULL)
        return RDB_ERROR;
    i = 0;
    exp = prep->attrlist.firstp;
    while (exp != NULL) {
        rep->compv[i].name = exp->var.varname;
        exp = exp->nextp;
        rep->compv[i].typ = expr_to_type(exp, ecp, txp);
        if (rep->compv[i].typ == NULL)
            return RDB_ERROR;
        exp = exp->nextp;
        i++;
    }
    return RDB_OK;
}

static int
exec_typedef(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i, j;
    int repc;
    RDB_possrep *repv;
    RDB_parse_possrep *prep;

    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    repc = 0;
    prep = stmtp->var.deftype.replistp;
    while (prep != NULL) {
        repc++;
        prep = prep->nextp;
    }

    repv = RDB_alloc(repc * sizeof(RDB_possrep), ecp);
    if (repv == NULL)
        return RDB_ERROR;

    i = 0;
    prep = stmtp->var.deftype.replistp;
    while (prep != NULL) {
        if (parserep_to_rep(prep, &repv[i], ecp, &txnp->tx) != RDB_OK)
            goto error;
        prep = prep->nextp;
        i++;
    }
    
    if (RDB_define_type(RDB_obj_string(&stmtp->var.deftype.typename),
            repc, repv, stmtp->var.deftype.constraintp, ecp, &txnp->tx) != RDB_OK)
        goto error;

    for (i = 0; i < repc; i++) {
        for (j = 0; j < repv[i].compc; j++) {
            if (!RDB_type_is_scalar(repv[i].compv[j].typ))
                RDB_drop_type(repv[i].compv[j].typ, ecp, NULL);
        }
    }
    RDB_free(repv);
    return RDB_OK;

error:
    for (i = 0; i < repc; i++) {
        for (j = 0; j < repv[i].compc; j++) {
            if (!RDB_type_is_scalar(repv[i].compv[j].typ))
                RDB_drop_type(repv[i].compv[j].typ, ecp, NULL);
        }
    }
    RDB_free(repv);
    return RDB_ERROR;
}

static int
exec_typedrop(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_type *typ;
    
    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }
    
    typ = RDB_get_type(RDB_obj_string(&stmtp->var.vardrop.varname), ecp, &txnp->tx);
    if (typ == NULL)
        return RDB_ERROR;

    return RDB_drop_type(typ, ecp, &txnp->tx);
}

static int
exec_opdrop(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;

    if (txnp == NULL) {
        RDB_database *dbp = get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    if (RDB_drop_op(RDB_obj_string(&stmtp->var.opdrop.opname), ecp,
            txnp == NULL ? &tmp_tx : &txnp->tx) != RDB_OK) {
        if (txnp == NULL)
            RDB_rollback(ecp, &tmp_tx);
        return RDB_ERROR;
    }
    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if (ret == RDB_OK && _RDB_parse_interactive)
        printf("Operator %s dropped.\n", RDB_obj_string(&stmtp->var.opdrop.opname));
    return ret;
}

int
Duro_dt_invoke_ro_op(const char *name, int argc, RDB_object *argv[],
          const void *iargp, size_t iarglen,
          RDB_exec_context *ecp, RDB_transaction *txp,
          RDB_object *retvalp)
{
    int i;
    int ret;
    varmap_node vars;
    RDB_parse_statement *stmtlistp;
    char **argnamev;
    struct op_data *op;
    varmap_node *ovarmapp = current_varmapp;

    if (interrupted) {
        interrupted = 0;
        RDB_raise_system("interrupted", ecp);
        return RDB_ERROR;
    }

    /* Try to get statements from the cache */
    op = RDB_hashmap_get(&ro_op_cache, name);
    if (op == NULL) {
        argnamev = RDB_alloc(argc * sizeof(char *), ecp);
        if (argnamev == NULL)
            return RDB_ERROR;
    
        stmtlistp = Duro_bin_to_stmts(iargp, iarglen, argc, ecp, txp, argnamev);
        if (stmtlistp == NULL) {
            RDB_free(argnamev);
            return RDB_ERROR;
        }

        op = RDB_alloc(sizeof(struct op_data), ecp);
        op->stmtlistp = stmtlistp;
        op->argnamec = argc;
        op->argnamev = argnamev;

        RDB_hashmap_put(&ro_op_cache, name, op);
    } else {
        stmtlistp = op->stmtlistp;
        argnamev = op->argnamev;
    }

    RDB_init_hashmap(&vars.map, 256);
    vars.parentp = NULL;
    current_varmapp = &vars;

    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&current_varmapp->map, argnamev[i], argv[i]) != RDB_OK) {
            RDB_parse_del_stmtlist(stmtlistp, ecp);
            for (i = 0; i < argc; i++)
                RDB_free(argnamev[i]);
            RDB_free(argnamev);
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    ret = exec_stmtlist(stmtlistp, ecp, retvalp);

    /*
     * Keep arguments from being destroyed
     */
    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&current_varmapp->map, argnamev[i], NULL) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    current_varmapp = ovarmapp;
    destroy_varmap(&vars.map);
    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

int
Duro_dt_invoke_update_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    varmap_node vars;
    RDB_parse_statement *stmtlistp;
    char **argnamev;
    varmap_node *ovarmapp = current_varmapp;

    if (interrupted) {
        interrupted = 0;
        RDB_raise_system("interrupted", ecp);
        return RDB_ERROR;
    }

    argnamev = RDB_alloc(argc * sizeof(char *), ecp);
    if (argnamev == NULL)
        return RDB_ERROR;

    stmtlistp = Duro_bin_to_stmts(iargp, iarglen, argc, ecp, txp, argnamev);
    if (stmtlistp == NULL) {
        RDB_free(argnamev);
        return RDB_ERROR;
    }

    RDB_init_hashmap(&vars.map, 256);
    vars.parentp = NULL;
    current_varmapp = &vars;

    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&current_varmapp->map, argnamev[i], argv[i]) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    ret = exec_stmtlist(stmtlistp, ecp, NULL);
    RDB_parse_del_stmtlist(stmtlistp, ecp);

    /*
     * Keep arguments from being destroyed
     */
    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&current_varmapp->map, argnamev[i], NULL) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }
    RDB_free(argnamev);

    current_varmapp = ovarmapp;
    destroy_varmap(&vars.map);
    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

static int
exec_ro_op_def(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i;
    int ret;
    RDB_object code;
    size_t sercodelen;
    void *sercodep;
    RDB_transaction tmp_tx;
    RDB_type **argtv = NULL;

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (txnp == NULL) {
        RDB_database *dbp = get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    /* Serialize the defining code */
    RDB_init_obj(&code);
    if (Duro_op_to_binobj(&code, stmtp, ecp) != RDB_OK) {
        goto error;
    }

    argtv = RDB_alloc(stmtp->var.opdef.argc * sizeof(RDB_type *),
            ecp);
    if (argtv == NULL) {
        goto error;
    }

    for (i = 0; i < stmtp->var.opdef.argc; i++) {
        if (eval_parse_type(&stmtp->var.opdef.argv[i].type, ecp,
                txnp != NULL ? &txnp->tx : &tmp_tx) != RDB_OK) {
            goto error;
        }
        argtv[i] = stmtp->var.opdef.argv[i].type.typ;
    }

    sercodelen = RDB_binary_length(&code);
    RDB_binary_get (&code, 0, sercodelen, ecp, &sercodep, NULL);

    if (eval_parse_type(&stmtp->var.opdef.rtype, ecp,
            txnp != NULL ? &txnp->tx : &tmp_tx) != RDB_OK) {
        goto error;
    }

    ret = RDB_create_ro_op(RDB_obj_string(&stmtp->var.opdef.opname),
            stmtp->var.opdef.argc, argtv, stmtp->var.opdef.rtype.typ,
#ifdef _WIN32
            "duro",
#else
            "libduro",
#endif
            "Duro_dt_invoke_ro_op",
            sercodep, sercodelen, ecp,
            txnp != NULL ? &txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    RDB_free(argtv);
    RDB_destroy_obj(&code, ecp);
    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && _RDB_parse_interactive)
        printf("Operator %s created.\n", RDB_obj_string(&stmtp->var.opdef.opname));
    return ret;

error:
    if (txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    RDB_free(argtv);
    RDB_destroy_obj(&code, ecp);
    return RDB_ERROR;
}

static int
exec_update_op_def(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i;
    int ret;
    RDB_object code;
    size_t sercodelen;
    void *sercodep;
    RDB_transaction tmp_tx;
    RDB_type **argtv = NULL;
    RDB_bool *updv = NULL;

    if (txnp == NULL) {
        RDB_database *dbp = get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    /* Serialize the defining code */
    RDB_init_obj(&code);
    if (Duro_op_to_binobj(&code, stmtp, ecp) != RDB_OK) {
        goto error;
    }

    argtv = RDB_alloc(stmtp->var.opdef.argc * sizeof(RDB_type *),
            ecp);
    if (argtv == NULL)
        goto error;
    updv = RDB_alloc(stmtp->var.opdef.argc * sizeof(RDB_bool), ecp);
    if (updv == NULL)
        goto error;
    for (i = 0; i < stmtp->var.opdef.argc; i++) {
        if (eval_parse_type(&stmtp->var.opdef.argv[i].type, ecp,
                txnp != NULL ? &txnp->tx : &tmp_tx) != RDB_OK) {
            goto error;
        }
        argtv[i] = stmtp->var.opdef.argv[i].type.typ;
        updv[i] = stmtp->var.opdef.argv[i].upd;
    }

    sercodelen = RDB_binary_length(&code);
    RDB_binary_get (&code, 0, sercodelen, ecp, &sercodep, NULL);

    ret = RDB_create_update_op(RDB_obj_string(&stmtp->var.opdef.opname),
            stmtp->var.opdef.argc, argtv, updv,
#ifdef _WIN32
            "duro",
#else
            "libduro",
#endif
            "Duro_dt_invoke_update_op",
            sercodep, sercodelen, ecp, txnp != NULL ? &txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    RDB_destroy_obj(&code, ecp);
    RDB_free(argtv);
    RDB_free(updv);
    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && _RDB_parse_interactive)
        printf("Operator %s created.\n", RDB_obj_string(&stmtp->var.opdef.opname));
    return ret;

error:
    RDB_destroy_obj(&code, ecp);
    RDB_free(argtv);
    RDB_free(updv);
    if (txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_return(const RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    if (stmtp->var.retexp != NULL) {
        if (retvalp == NULL) {
            RDB_raise_invalid_argument("return argument not allowed", ecp);
            return RDB_ERROR;
        }
        if (RDB_evaluate(stmtp->var.retexp, &get_var, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : NULL, retvalp) != RDB_OK)
            return RDB_ERROR;
    }
    return DURO_RETURN;
}

static int
exec_constr_def(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;
    RDB_expression *constrexp = NULL;

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (txnp == NULL) {
        RDB_database *dbp = get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    constrexp = RDB_dup_expr(stmtp->var.constrdef.constraintp, ecp);
    if (constrexp == NULL)
        goto error;
    ret = RDB_create_constraint(RDB_obj_string(&stmtp->var.constrdef.constrname),
            constrexp, ecp, txnp != NULL ? &txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && _RDB_parse_interactive)
        printf("Constraint %s created.\n", RDB_obj_string(&stmtp->var.opdef.opname));
    return ret;

error:
    if (constrexp != NULL)
        RDB_drop_expr(constrexp, ecp);
    if (txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_constr_drop(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (txnp == NULL) {
        RDB_database *dbp = get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    ret = RDB_drop_constraint(RDB_obj_string(&stmtp->var.constrdrop.constrname),
            ecp, txnp != NULL ? &txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && _RDB_parse_interactive)
        printf("Constraint %s dropped.\n", RDB_obj_string(&stmtp->var.opdef.opname));
    return ret;

error:
    if (txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
Duro_exec_stmt(RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    int ret;

    switch (stmtp->kind) {
        case RDB_STMT_CALL:
            ret = exec_call(stmtp, ecp);
            break;
        case RDB_STMT_VAR_DEF:
            ret = exec_vardef(stmtp, ecp);
            break;
        case RDB_STMT_VAR_DEF_REAL:
            ret = exec_vardef_real(stmtp, ecp);
            break;
        case RDB_STMT_VAR_DEF_VIRTUAL:
            ret = exec_vardef_virtual(stmtp, ecp);
            break;
        case RDB_STMT_VAR_DEF_PRIVATE:
            ret = exec_vardef_private(stmtp, ecp);
            break;
        case RDB_STMT_VAR_DROP:
            ret = exec_vardrop(stmtp, ecp);
            break;
        case RDB_STMT_NOOP:
            ret = RDB_OK;
            break;
        case RDB_STMT_IF:
            ret = exec_if(stmtp, ecp, retvalp);
            break;
        case RDB_STMT_FOR:
            ret = exec_for(stmtp, ecp, retvalp);
            break;
        case RDB_STMT_WHILE:
            ret = exec_while(stmtp, ecp, retvalp);
            break;
        case RDB_STMT_ASSIGN:
            ret = exec_assign(stmtp, ecp);
            break;
        case RDB_STMT_BEGIN_TX:
            ret = exec_begin_tx(stmtp, ecp);
            break;
        case RDB_STMT_COMMIT:
            ret = exec_commit(stmtp, ecp);
            break;
        case RDB_STMT_ROLLBACK:
            ret = exec_rollback(stmtp, ecp);
            break;
        case RDB_STMT_TYPE_DEF:
            ret = exec_typedef(stmtp, ecp);
            break;
        case RDB_STMT_TYPE_DROP:
            ret = exec_typedrop(stmtp, ecp);
            break;
        case RDB_STMT_RO_OP_DEF:
            ret = exec_ro_op_def(stmtp, ecp);
            break;
        case RDB_STMT_UPD_OP_DEF:
            ret = exec_update_op_def(stmtp, ecp);
            break;
        case RDB_STMT_OP_DROP:
            ret = exec_opdrop(stmtp, ecp);
            break;
        case RDB_STMT_RETURN:
            ret = exec_return(stmtp, ecp, retvalp);
            break;
        case RDB_STMT_CONSTRAINT_DEF:
            ret = exec_constr_def(stmtp, ecp);
            break;
        case RDB_STMT_CONSTRAINT_DROP:
            ret = exec_constr_drop(stmtp, ecp);
            break;
        default:
            abort();
    }
    if (ret == RDB_ERROR) {
        if (err_line < 0) {
            err_line = stmtp->lineno;
        }
    }
    return ret;
}

int
Duro_process_stmt(RDB_exec_context *ecp)
{
    RDB_parse_statement *stmtp;
    RDB_object prompt;
    RDB_object *dbnameobjp = RDB_hashmap_get(&toplevel_vars.map, "CURRENT_DB");

    RDB_init_obj(&prompt);
    if (dbnameobjp != NULL && *RDB_obj_string(dbnameobjp) != '\0') {
        RDB_string_to_obj(&prompt, RDB_obj_string(dbnameobjp), ecp);
    } else {
        RDB_string_to_obj(&prompt, "no db", ecp);
    }
    RDB_append_string(&prompt, "> ", ecp);

    _RDB_parse_prompt = RDB_obj_string(&prompt);

    stmtp = RDB_parse_stmt(ecp);
    RDB_destroy_obj(&prompt, ecp);
    if (stmtp == NULL) {
        err_line = yylineno;
        return RDB_ERROR;
    }
    assert(RDB_get_err(ecp) == NULL);
    if (Duro_exec_stmt(stmtp, ecp, NULL) != RDB_OK) {
        RDB_parse_del_stmt(stmtp, ecp);
        if (RDB_get_err(ecp) == NULL) {
            RDB_raise_internal("statement execution failed, no error available", ecp);
        }
        return RDB_ERROR;
    }
    return RDB_parse_del_stmt(stmtp, ecp);
}

void
Duro_print_error(const RDB_object *errobjp)
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

void
Duro_dt_interrupt(void)
{
    interrupted = 1;
}

int
Duro_dt_execute(RDB_environment *dbenvp, char *dbname, char *infilename,
        RDB_exec_context *ecp)
{
    envp = dbenvp;
    interrupted = 0;

    if (infilename != NULL) {
        yyin = fopen(infilename, "r");
        if (yyin == NULL) {
            RDB_raise_resource_not_found(infilename, ecp);
            return RDB_ERROR;
        }
    } else {
        yyin = stdin;
    }

    if (Duro_init_exec(ecp, dbname) != RDB_OK) {
        goto error;
    }

    err_line = -1;

    _RDB_parse_interactive = (RDB_bool) isatty(fileno(yyin));

    if (_RDB_parse_interactive) {
        printf("Duro D/T library version %d.%d\n", RDB_major_version(),
                RDB_minor_version());
        _RDB_parse_init_buf();
    }

    for(;;) {
        if (Duro_process_stmt(ecp) != RDB_OK) {
            RDB_object *errobjp = RDB_get_err(ecp);
            if (errobjp != NULL) {
                if (!_RDB_parse_interactive) {
                    printf("error in statement at or near line %d: ", err_line);
                }
                if (_RDB_parse_interactive) {
                    Duro_print_error(errobjp);
                    _RDB_parse_init_buf();
                } else {
                    Duro_exit_interp();
                    goto error;
                }
                RDB_clear_err(ecp);
            } else {
                /* Exit on EOF  */
                puts("");
                Duro_exit_interp();
                if (infilename != NULL) {
                    fclose(yyin);
                }
                return RDB_OK;
            }
        }
    }

error:
    if (infilename != NULL) {
        fclose(yyin);
    }
    return RDB_ERROR;
}
