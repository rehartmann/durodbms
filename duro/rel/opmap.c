/*
 * $Id$
 *
 * Copyright (C) 2007-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "internal.h"
#include <gen/hashmapit.h>
#include <gen/strfns.h>

#include <assert.h>

void
RDB_init_op_map(RDB_op_map *opmap)
{
    RDB_init_hashmap(&opmap->map, 256);
}

struct op_entry {
    RDB_operator *op;
    struct op_entry *nextp;
};

static int
free_op(struct op_entry *opep, RDB_exec_context *ecp)
{
    int ret;

    ret = RDB_free_op_data(opep->op, ecp);
    RDB_free(opep);
    return ret;
}

static void
free_ops(struct op_entry *opep, RDB_exec_context *ecp)
{
    struct op_entry *nextop;
    do {
        nextop = opep->nextp;
        free_op(opep, ecp);
        opep = nextop;
    } while (opep != NULL);
}

void
RDB_destroy_op_map(RDB_op_map *opmap)
{
    RDB_hashmap_iter it;
    char *keyp;
    void *op;
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);
    RDB_init_hashmap_iter(&it, &opmap->map);

    while ((op = RDB_hashmap_next(&it, &keyp)) != NULL) {
        struct op_entry *opep = op;

        if (opep != NULL)
            free_ops(opep, &ec);
    }

    RDB_destroy_hashmap_iter(&it);
    RDB_destroy_exec_context(&ec);

    RDB_destroy_hashmap(&opmap->map);
}

int
RDB_put_op(RDB_op_map *opmap, RDB_operator *op,
        RDB_exec_context *ecp)
{
    int ret;
    struct op_entry *fopep, *opep;

    opep = RDB_alloc(sizeof(struct op_entry), ecp);
    if (opep == NULL) {
        return RDB_ERROR;
    }

    opep->op = op;

    fopep = RDB_hashmap_get(&opmap->map, op->name);

    if (fopep == NULL) {
        opep->nextp = NULL;
        ret = RDB_hashmap_put(&opmap->map, op->name, opep);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp, NULL);
            goto error;
        }
    } else {
        opep->nextp = fopep->nextp;
        fopep->nextp = opep;
    }
    return RDB_OK;

error:
    RDB_free(opep);
    return RDB_ERROR;   
}

RDB_operator *
RDB_get_op(const RDB_op_map *opmap, const char *name, int argc,
        RDB_type *argtv[], RDB_exec_context *ecp)
{
    RDB_bool argc_match = RDB_FALSE;
    struct op_entry *opep;
    struct op_entry *firstopep = RDB_hashmap_get(&opmap->map, name);
    if (firstopep == NULL) {
        RDB_raise_operator_not_found(name, ecp);
        return NULL;
    }

    /* Find an operator with same signature */
    opep = firstopep;
    while (opep != NULL) {
        if (opep->op->paramc == argc) {
            int i;

            for (i = 0; (i < argc)
                    && RDB_type_equals(opep->op->paramv[i].typ, argtv[i]);
                 i++);
            if (i == argc) {
                /* Found */
                return opep->op;
            }
            argc_match = RDB_TRUE;
        }
        opep = opep->nextp;
    }

    /* If not, found, search generic operator (argc == -1) */
    opep = firstopep;
    while (opep != NULL) {
        if (opep->op->paramc == -1) {
            return opep->op;
        }
        opep = opep->nextp;
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
            RDB_errcode_to_error(ret, ecp, NULL);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

RDB_operator *
_RDB_new_operator(const char *name, int argc, RDB_type *argtv[], RDB_type *rtyp,
        RDB_exec_context *ecp)
{
    int i;
    RDB_operator *op = RDB_alloc(sizeof (RDB_operator), ecp);
    if (op == NULL) {
        return NULL;
    }

    RDB_init_obj(&op->source);
    op->name = RDB_dup_str(name);
    if (op->name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    op->paramc = argc;
    if (argc > 0) {
        op->paramv = RDB_alloc(sizeof (RDB_parameter) * argc, ecp);
        if (op->paramv == NULL) {
            goto error;
        }

        for (i = 0; i < argc; i++) {
            op->paramv[i].typ = NULL;
        }
        for (i = 0; i < argc; i++) {
            op->paramv[i].typ = RDB_dup_nonscalar_type(argtv[i], ecp);
            if (op->paramv[i].typ == NULL) {
                goto error;
            }
        }
    } else {
        op->paramv = NULL;
    }

    op->rtyp = rtyp;
    op->modhdl = NULL;
    op->u_data = NULL;
    op->cleanup_fp = NULL;

    return op;

error:
    RDB_destroy_obj(&op->source, ecp);
    if (op->name != NULL)
        RDB_free(op->name);
    if (op->paramv != NULL) {
        for (i = 0; i < op->paramc; i++) {
            if (op->paramv[i].typ != NULL
                   && !RDB_type_is_scalar(op->paramv[i].typ)) {
                RDB_drop_type(op->paramv[i].typ, ecp, NULL);
            }
        }
    }
    RDB_free(op);
    return NULL;
}

int
RDB_put_upd_op(RDB_op_map *opmap, const char *name, int paramc, RDB_parameter *paramv,
        RDB_upd_op_func *opfp, RDB_exec_context *ecp)
{
    RDB_operator *op = RDB_alloc(sizeof(RDB_operator), ecp);
    if (op == NULL)
        return RDB_ERROR;
    op->name = RDB_dup_str(name);
    if (op->name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    op->paramc = paramc;
    op->paramv = paramv;

    RDB_init_obj(&op->source);
    op->modhdl = NULL;
    op->opfn.upd_fp = opfp;
    op->rtyp = NULL;

    if (RDB_put_op(opmap, op, ecp) != RDB_OK) {
        RDB_destroy_obj(&op->source, ecp);
        RDB_free(op);
        return RDB_ERROR;
    }
    return RDB_OK;

error:
    if (op->name != NULL)
        RDB_free(op->name);
    RDB_free(op);
    return RDB_ERROR;
}

int
RDB_free_op_data(RDB_operator *op, RDB_exec_context *ecp)
{
    int i;
    int ret;

    if (op->rtyp != NULL && !RDB_type_is_scalar(op->rtyp))
        RDB_drop_type(op->rtyp, ecp, NULL);
    if (op->modhdl != NULL) {
        /* Operator loaded from module */
        lt_dlclose(op->modhdl);
    }
    for (i = 0; i < op->paramc; i++) {
        if (!RDB_type_is_scalar(op->paramv[i].typ))
            RDB_drop_type(op->paramv[i].typ, ecp, NULL);
    }
    RDB_free(op->paramv);
    RDB_free(op->name);
    if (op->cleanup_fp != NULL)
        (*op->cleanup_fp) (op);
    ret = RDB_destroy_obj(&op->source, ecp);
    RDB_free(op);
    return ret;
}
