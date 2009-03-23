/*
 * $Id$
 *
 * Copyright (C) 2007-2009 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "internal.h"
#include <gen/hashmapit.h>
#include <gen/strfns.h>

void
RDB_init_op_map(RDB_op_map *opmap)
{
    RDB_init_hashmap(&opmap->map, 256);
}

struct op_entry {
    char *name;
    int argc;
    RDB_type **argtv;
    RDB_op_data *datap;
    struct op_entry *nextp;
};

static int
free_op(struct op_entry *op, RDB_exec_context *ecp)
{
    int i;
    int ret;

    RDB_free(op->name);
    for (i = 0; i < op->argc; i++) {
        if (!RDB_type_is_scalar(op->argtv[i]))
            RDB_drop_type(op->argtv[i], ecp, NULL);
    }
    RDB_free(op->argtv);
    ret = RDB_free_op_data(op->datap, ecp);
    RDB_free(op);
    return ret;
}

static void
free_ops(struct op_entry *op, RDB_exec_context *ecp)
{
    struct op_entry *nextop;
    do {
        nextop = op->nextp;
        free_op(op, ecp);
        op = nextop;
    } while (op != NULL);
}

void
RDB_destroy_op_map(RDB_op_map *opmap)
{
    RDB_hashmap_iter it;
    char *keyp;
    void *datap;
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);
    RDB_init_hashmap_iter(&it, &opmap->map);

    while ((datap = RDB_hashmap_next(&it, &keyp)) != NULL) {
        struct op_entry *op = datap;

        if (op != NULL)
            free_ops(op, &ec);
    }

    RDB_destroy_hashmap_iter(&it);
    RDB_destroy_exec_context(&ec);

    RDB_destroy_hashmap(&opmap->map);
}

int
RDB_put_op(RDB_op_map *opmap, const char *name, int argc, RDB_type **argtv,
        RDB_op_data *datap, RDB_exec_context *ecp)
{
    int ret;
    int i;
    struct op_entry *fop, *op;

    op = RDB_alloc(sizeof(struct op_entry), ecp);
    if (op == NULL) {
        return RDB_ERROR;
    }

    op->argtv = NULL;
    op->name = RDB_dup_str(name);
    if (op->name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    op->argc = argc;
    if (argc > 0) {
        op->argtv = RDB_alloc(sizeof (RDB_type *) * argc, ecp);
        if (op->argtv == NULL) {
            goto error;
        }
    
        for (i = 0; i < argc; i++) {
            op->argtv[i] = NULL;
        }
        for (i = 0; i < argc; i++) {
            op->argtv[i] = RDB_dup_nonscalar_type(argtv[i], ecp);
            if (op->argtv[i] == NULL) {
                RDB_raise_no_memory(ecp);
                goto error;
            }
        }
    } else {
        op->argtv = NULL;
    }
    op->datap = datap;

    fop = RDB_hashmap_get(&opmap->map, op->name);

    if (fop == NULL) {
        op->nextp = NULL;
        ret = RDB_hashmap_put(&opmap->map, op->name, op);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, NULL);
            goto error;
        }
    } else {
        op->nextp = fop->nextp;
        fop->nextp = op;
    }
    return RDB_OK;

error:
    RDB_free(op->name);
    if (op->argtv != NULL) {
        for (i = 0; i < argc; i++) {
            if (op->argtv[i] != NULL && !RDB_type_is_scalar(op->argtv[i]))
                RDB_drop_type(op->argtv[i], ecp, NULL);
        }
    }
    RDB_free(op);
    return RDB_ERROR;   
}

RDB_op_data *
RDB_get_op(const RDB_op_map *opmap, const char *name, int argc,
        RDB_type *argtv[], RDB_exec_context *ecp)
{
    RDB_bool argc_match = RDB_FALSE;
    struct op_entry *op;
    struct op_entry *firstop = RDB_hashmap_get(&opmap->map, name);
    if (firstop == NULL) {
        RDB_raise_operator_not_found(name, ecp);
        return NULL;
    }

    /* Find an operator with same signature */
    op = firstop;
    while (op != NULL) {
        if (op->argc == argc) {
            int i;

            for (i = 0; (i < argc)
                    && RDB_type_equals(op->argtv[i], argtv[i]);
                 i++);
            if (i == argc) {
                /* Found */
                return op->datap;
            }
            argc_match = RDB_TRUE;
        }
        op = op->nextp;
    }

    /* If not, found, search generic operator (argc == -1) */
    op = firstop;
    while (op != NULL) {
        if (op->argc == -1) {
            return op->datap;
        }
        op = op->nextp;
    }

    if (argc_match) {
        RDB_raise_type_mismatch(name, ecp);
    } else {
        RDB_raise_operator_not_found(name, ecp);
    }    

    return NULL;
}

int
RDB_del_ops(RDB_op_map *opmap, const char *name, RDB_exec_context *ecp)
{
    struct op_entry *op = RDB_hashmap_get(&opmap->map, name);
    if (op != NULL) {
        int ret;

        free_ops(op, ecp);
        ret = RDB_hashmap_put(&opmap->map, name, NULL);
        if (ret != RDB_OK) {
            _RDB_handle_errcode(ret, ecp, NULL);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

RDB_op_data *
_RDB_new_ro_op_data(RDB_type *rtyp,
        RDB_ro_op_func *funcp, RDB_exec_context *ecp)
{
    RDB_op_data *op = RDB_alloc(sizeof (RDB_op_data), ecp);
    if (op == NULL) {
        return NULL;
    }

    RDB_init_obj(&op->iarg);
    op->rtyp = rtyp;
    op->modhdl = NULL;
    op->updv = NULL;
    op->opfn.ro_fp = funcp;

    return op;
}

int
RDB_put_upd_op(RDB_op_map *opmap, const char *name, int argc, RDB_type **argtv,
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
RDB_free_op_data(RDB_op_data *op, RDB_exec_context *ecp)
{
    int ret;

    if (op->rtyp != NULL && !RDB_type_is_scalar(op->rtyp))
        RDB_drop_type(op->rtyp, ecp, NULL);
    if (op->modhdl != NULL) {
        /* Operator loaded from module */
        lt_dlclose(op->modhdl);
    }
    ret = RDB_destroy_obj(&op->iarg, ecp);
    RDB_free(op->updv);
    RDB_free(op);
    return ret;
}
