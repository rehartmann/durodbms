/* $Id$ */

#include "rdb.h"
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
        RDB_deinit_value(valp);

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

int
RDB_copy_value(RDB_value *dstvalp, const RDB_value *srcvalp)
{
    if (dstvalp->typ != NULL)
        RDB_deinit_value(dstvalp);

    dstvalp->typ = srcvalp->typ;
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

void
RDB_init_value(RDB_value *valp)
{
    valp->typ = NULL;
}

void
RDB_deinit_value(RDB_value *valp)
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
