/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include <gen/errors.h>
#include <string.h>

void *
RDB_value_irep(RDB_value *valp, size_t *lenp)
{
    switch (valp->typ->kind) {
        case RDB_TP_BOOLEAN:
            *lenp = sizeof (RDB_bool);
            return &valp->var.bool_val;
        case RDB_TP_INTEGER:
            *lenp = sizeof (RDB_int);
            return &valp->var.int_val;
        case RDB_TP_RATIONAL:
            *lenp = sizeof (RDB_rational);
            return &valp->var.rational_val;
        default:
            *lenp = valp->var.bin.len;
            return valp->var.bin.datap;
    }
} 

int
RDB_irep_to_value(RDB_value *valp, RDB_type *typ, void *datap, size_t len)
{
    if (valp->typ != NULL)
        RDB_destroy_value(valp);

    valp->typ = typ;
    switch (valp->typ->kind) {
        case RDB_TP_BOOLEAN:
            valp->var.bool_val = *(RDB_bool *) datap;
            break;
        case RDB_TP_INTEGER:
            memcpy(&valp->var.int_val, datap, sizeof (RDB_int));
            break;
        case RDB_TP_RATIONAL:
            memcpy(&valp->var.rational_val, datap, sizeof (RDB_rational));
            break;
        default:
            valp->var.bin.len = len;
            valp->var.bin.datap = malloc(len);
            if (valp->var.bin.datap == NULL)
                return RDB_NO_MEMORY;
            memcpy(valp->var.bin.datap, datap, len);
    }
    return RDB_OK;
} 

RDB_bool
RDB_value_equals(const RDB_value *valp1, const RDB_value *valp2)
{
    switch (valp1->typ->kind) {
        case RDB_TP_BOOLEAN:
            return (RDB_bool) valp1->var.bool_val == valp2->var.bool_val;
            break;
        case RDB_TP_INTEGER:
            return (RDB_bool) (valp1->var.int_val == valp2->var.int_val);
            break;
        case RDB_TP_RATIONAL:
            return (RDB_bool) (valp1->var.rational_val == valp2->var.rational_val);
            break;
        default:
            if (valp1->var.bin.len != valp2->var.bin.len)
                return RDB_FALSE;
            return (RDB_bool) (memcmp(valp1->var.bin.datap, valp2->var.bin.datap,
                    valp1->var.bin.len) == 0);
    }
} 

static int
copy_value(RDB_value *dstvalp, const RDB_value *srcvalp)
{
    switch (srcvalp->typ->kind) {
        case RDB_TP_BOOLEAN:
            dstvalp->var.bool_val = srcvalp->var.bool_val;
            break;
        case RDB_TP_INTEGER:
            dstvalp->var.int_val = srcvalp->var.int_val;
            break;
        case RDB_TP_RATIONAL:
            dstvalp->var.rational_val = srcvalp->var.rational_val;
            break;
        default:
            dstvalp->var.bin.len = srcvalp->var.bin.len;
            dstvalp->var.bin.datap = malloc(srcvalp->var.bin.len);
            if (dstvalp->var.bin.datap == NULL)
                return RDB_NO_MEMORY;
            memcpy(dstvalp->var.bin.datap, srcvalp->var.bin.datap,
                        srcvalp->var.bin.len);
    }
    return RDB_OK;
}

int
RDB_copy_value(RDB_value *dstvalp, const RDB_value *srcvalp)
{
    if (dstvalp->typ != NULL)
        RDB_destroy_value(dstvalp);

    dstvalp->typ = srcvalp->typ;
    return copy_value(dstvalp, srcvalp);
} 

void
RDB_init_value(RDB_value *valp)
{
    valp->typ = NULL;
}

void
RDB_destroy_value(RDB_value *valp)
{
    if (valp->typ == NULL)
        return;

    switch (valp->typ->kind) {
        case RDB_TP_BOOLEAN:
        case RDB_TP_INTEGER:
        case RDB_TP_RATIONAL:
            break;
        default:
            free(valp->var.bin.datap);
    }
}

void
RDB_value_set_bool(RDB_value *valp, RDB_bool v)
{
    RDB_destroy_value(valp);
    valp->typ = &RDB_BOOLEAN;
    valp->var.bool_val = v;
}

void
RDB_value_set_int(RDB_value *valp, RDB_int v)
{
    RDB_destroy_value(valp);
    valp->typ = &RDB_INTEGER;
    valp->var.int_val = v;
}

void
RDB_value_set_rational(RDB_value *valp, RDB_rational v)
{
    RDB_destroy_value(valp);
    valp->typ = &RDB_RATIONAL;
    valp->var.rational_val = v;
}

int
RDB_value_set_string(RDB_value *valp, const char *str)
{
    RDB_destroy_value(valp);
    valp->typ = &RDB_STRING;
    valp->var.bin.len = strlen(str) + 1;
    valp->var.bin.datap = malloc(valp->var.bin.len);
    if (valp->var.bin.datap == NULL)
        return RDB_NO_MEMORY;
    strcpy(valp->var.bin.datap, str);
    return RDB_OK;
}

int
RDB_value_get_comp(const RDB_value *valp, const char *compname,
                   RDB_value *comp)
{
    RDB_destroy_value(comp);

    comp->typ = valp->typ->var.scalar.repv[0].compv[0].type;
    return copy_value(comp, valp);   
}

int
RDB_value_set_comp(RDB_value *valp, const char *compname,
                   const RDB_value *comp)
{
    RDB_destroy_value(valp);

    return copy_value(valp, comp);
}

static RDB_bool
check_constraint(RDB_value *valp) {
    int i, j;

    /* Check constraint for each possrep */
    for (i = 0; i < valp->typ->var.scalar.repc; i++) {
        RDB_tuple tpl;
        RDB_bool result;

        RDB_init_tuple(&tpl);
        /* Set tuple attributes */
        for (j = 0; j < valp->typ->var.scalar.repv[i].compc; j++) {
            RDB_value comp;
            char *compname = valp->typ->var.scalar.repv[i].compv[j].name;

            RDB_init_value(&comp);
            RDB_value_get_comp(valp, compname, &comp);
            RDB_tuple_set(&tpl, compname, &comp);
            RDB_destroy_value(&comp);
        }
        RDB_evaluate_bool(valp->typ->var.scalar.repv[i].constraintp,
                &tpl, NULL, &result);
        RDB_destroy_tuple(&tpl);
        if (!result)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

int
RDB_value_set(RDB_value *valp, RDB_type *typ, const char *repname,
              RDB_value **compv)
{
    /* RDB_possrep prp;
    int i; */
    int ret;

    RDB_destroy_value(valp);

    if (typ->var.scalar.repc == 0 || !RDB_type_is_scalar(typ))
        return RDB_INVALID_ARGUMENT;

    /* Find possrep - not used yet
    for (i = 0; i < typ->scalar.repc
            && strcmp(typ->scalar.repv[i].name, repname) == 0);
            i++);
    if (i > typ->scalar.repc)
        return RDB_INVALID_ARGUMENT;
    prp = typ->scalar.repv[i].exp;
    */

    if (repname == NULL)
        repname = typ->name;

    valp->typ = typ;
    
    ret = copy_value(valp, *compv);
    if (ret != RDB_OK)
        return ret;

    if (!check_constraint(valp))
        return RDB_TYPE_CONSTRAINT_VIOLATION;

    return RDB_OK;
}

RDB_bool
RDB_value_bool(const RDB_value *valp)
{
    return valp->var.bool_val;
}

RDB_int
RDB_value_int(const RDB_value *valp)
{
    return valp->var.int_val;
}

RDB_rational
RDB_value_rational(const RDB_value *valp)
{
    return valp->var.rational_val;
}

char *
RDB_value_string(RDB_value *valp)
{
    return valp->var.bin.datap;
}

int
RDB_binary_set(RDB_value *valp, size_t pos, void *srcp, size_t len)
{
    /* If the value is newly initialized, allocate memory */
    if (valp->typ == NULL) {
        valp->var.bin.len = pos + len;
        valp->var.bin.datap = malloc(valp->var.bin.len);
        if (valp->var.bin.datap == NULL)
            return RDB_NO_MEMORY;
        valp->typ = &RDB_BINARY;
    }

    /* If the memory block is to small, reallocate */
    if (valp->var.bin.len < pos + len) {
        void *datap;

        datap = realloc(valp->var.bin.datap, pos + len);
        if (datap == NULL)
            return RDB_NO_MEMORY;
        valp->var.bin.datap = datap;
        valp->var.bin.len = pos + len;
    }
    
    /* copy data */
    memcpy(((RDB_byte *)valp->var.bin.datap) + pos, srcp, len);
    return RDB_OK;
}

int
RDB_binary_get(const RDB_value *valp, size_t pos, void *dstp, size_t len)
{
    if (pos + len > valp->var.bin.len)
        return RDB_NOT_FOUND;
    memcpy(dstp, ((RDB_byte *)valp->var.bin.datap) + pos, len);
    return RDB_OK;
}

size_t
RDB_binary_get_length(const RDB_value *valp)
{
    return valp->var.bin.len;
}
