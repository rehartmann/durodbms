/*
 * Operator map functions.
 *
 * Copyright (C) 2007-2009, 2011-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "opmap.h"
#include "type.h"
#include "objmatch.h"
#include "objinternal.h"
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
        if (op != NULL)
            free_ops((struct op_entry *) op, &ec);
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

    fopep = RDB_hashmap_get(&opmap->map, RDB_operator_name(op));

    if (fopep == NULL) {
        opep->nextp = NULL;
        ret = RDB_hashmap_put(&opmap->map, RDB_operator_name(op), opep);
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

    /*
     * Find an operator with same signature, NULL matches all types
     */
    opep = firstopep;
    while (opep != NULL) {
        if (RDB_operator_param_count(opep->op) == argc) {
            int i;

            for (i = 0; (i < argc)
                    && (opep->op->paramv[i].typ == NULL
                        || RDB_type_matches(argtv[i], opep->op->paramv[i].typ));
                 i++);
            if (i == argc) {
                /* Found */
                return opep->op;
            }
            argc_match = RDB_TRUE;
        }
        opep = opep->nextp;
    }

    /* If not, found, search completely generic operator (argc == RDB_VAR_PARAMS) */
    opep = firstopep;
    while (opep != NULL) {
        if (RDB_operator_param_count(opep->op) == RDB_VAR_PARAMS) {
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

static RDB_bool
obj_has_type(RDB_object *objp, RDB_type *typ)
{
    RDB_type *objtyp;
    if (!RDB_type_is_scalar(typ))
        return RDB_obj_matches_type(objp, typ);
    objtyp = objp->typ;
    if (objtyp == NULL)
        return RDB_obj_matches_type(objp, typ);
    if (RDB_type_is_dummy(objtyp))
        objtyp = objp->impl_typ;
    return RDB_is_subtype(objtyp, typ);
}

RDB_operator *
RDB_get_op_by_args(const RDB_op_map *opmap, const char *name, int argc,
        RDB_object *argv[], RDB_exec_context *ecp)
{
    RDB_bool argc_match = RDB_FALSE;
    struct op_entry *opep;
    struct op_entry *firstopep = RDB_hashmap_get(&opmap->map, name);
    if (firstopep == NULL) {
        RDB_raise_operator_not_found(name, ecp);
        return NULL;
    }

    /*
     * Find an operator with same signature
     */
    opep = firstopep;
    while (opep != NULL) {
        if (RDB_operator_param_count(opep->op) == argc
                && RDB_operator_is_implemented(opep->op)) {
            int i;

            for (i = 0; (i < argc)
                    && (opep->op->paramv[i].typ == NULL
                            || obj_has_type(argv[i], opep->op->paramv[i].typ));
                 i++);
            if (i == argc) {
                /* Found */
                return opep->op;
            }
            argc_match = RDB_TRUE;
        }
        opep = opep->nextp;
    }

    /* If not, found, search completely generic operator (argc == RDB_VAR_PARAMS) */
    opep = firstopep;
    while (opep != NULL) {
        if (RDB_operator_param_count(opep->op) == RDB_VAR_PARAMS) {
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
        if (RDB_operator_param_count(opep->op) == 2
                && opep->op->paramv[0].typ == typ) {
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

static RDB_operator *
RDB_new_upd_op(const char *name, int paramc, RDB_parameter paramv[],
        RDB_upd_op_func *opfp, RDB_exec_context *ecp)
{
    int i;
    RDB_operator *op = RDB_new_op_data(name, 0, NULL, NULL, ecp);
    if (op == NULL)
        return NULL;

    op->paramc = paramc;
    if (paramc > 0) {
        op->paramv = RDB_alloc(sizeof (RDB_parameter) * paramc, ecp);
        if (op->paramv == NULL) {
            goto error;
        }

        for (i = 0; i < paramc; i++) {
            op->paramv[i].typ = NULL;
        }
        for (i = 0; i < paramc; i++) {
            op->paramv[i].typ = RDB_dup_nonscalar_type(paramv[i].typ, ecp);
            if (op->paramv[i].typ == NULL) {
                goto error;
            }
            op->paramv[i].update = paramv[i].update;
        }
    }
    op->opfn.upd_fp = opfp;
    return op;

error:
    RDB_destroy_obj(&op->source, ecp);
    if (op->name != NULL)
        RDB_free(op->name);
    if (op->paramv != NULL) {
        for (i = 0; i < op->paramc; i++) {
            if (op->paramv[i].typ != NULL
                   && !RDB_type_is_scalar(op->paramv[i].typ)) {
                RDB_del_nonscalar_type(op->paramv[i].typ, ecp);
            }
        }
    }
    RDB_free(op);
    return NULL;
}

int
RDB_put_ro_op(RDB_op_map *opmap, const char *name, int argc, RDB_type **argtv,
        RDB_type *rtyp, RDB_ro_op_func *fp, RDB_exec_context *ecp)
{
    int ret;
    RDB_operator *datap = RDB_new_op_data(name, argc, argtv, rtyp, ecp);
    if (datap == NULL)
        return RDB_ERROR;
    datap->opfn.ro_fp = fp;

    ret = RDB_put_op(opmap, datap, ecp);
    if (ret != RDB_OK) {
        RDB_free_op_data(datap, ecp);
        return RDB_ERROR;
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
