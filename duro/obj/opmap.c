/*
 * $Id$
 *
 * Copyright (C) 2007-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "opmap.h"
#include "type.h"
#include <gen/hashmapit.h>
#include <gen/strfns.h>

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
    void *op;
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);
    RDB_init_hashmap_iter(&it, &opmap->map);

    while (RDB_hashmap_next(&it, &op) != NULL) {
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
            RDB_errno_to_error(ret, ecp);
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

    /* If not, found, search generic operator (argc == RDB_VAR_PARAMS) */
    opep = firstopep;
    while (opep != NULL) {
        if (opep->op->paramc == RDB_VAR_PARAMS) {
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
RDB_del_cmp_op(RDB_op_map *opmap, const char *name, RDB_type *typ,
        RDB_exec_context *ecp)
{
    struct op_entry *prevep = NULL;
    struct op_entry *opep = RDB_hashmap_get(&opmap->map, name);

    while (opep != NULL) {
        if (opep->op->paramc == 2 && opep->op->paramv[0].typ == typ) {
            if (prevep == NULL) {
                int ret = RDB_hashmap_put(&opmap->map, name, opep->nextp);
                if (ret != RDB_OK) {
                    RDB_errno_to_error(ret, ecp);
                    return RDB_ERROR;
                }
            } else {
                prevep->nextp = opep->nextp;
            }
            return free_op(opep, ecp);
        }
        prevep = opep;
        opep = opep->nextp;
    }

    return RDB_OK;
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
            RDB_errno_to_error(ret, ecp);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

int
RDB_put_upd_op(RDB_op_map *opmap, const char *name, int paramc, RDB_parameter paramv[],
        RDB_upd_op_func *opfp, RDB_exec_context *ecp)
{
    RDB_operator *op = RDB_new_upd_op(name, paramc, paramv, opfp, ecp);
    if (op == NULL)
        return RDB_ERROR;

    if (RDB_put_op(opmap, op, ecp) != RDB_OK) {
        RDB_free_op_data(op, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}
