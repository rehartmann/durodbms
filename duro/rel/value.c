/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/errors.h>
#include <string.h>

void *
RDB_value_irep(RDB_value *valp, size_t *lenp)
{
    if (lenp != NULL) {
        *lenp = valp->typ->ireplen;
        if (*lenp == RDB_VARIABLE_LEN)
            *lenp = valp->var.bin.len;
    }

    switch(valp->kind) {
        case _RDB_BOOL:
            return &valp->var.bool_val;
        case _RDB_INT:
            return &valp->var.int_val;
        case _RDB_RATIONAL:
            return &valp->var.rational_val;
        default:
            return valp->var.bin.datap;
    }
} 

static enum _RDB_value_kind
val_kind(const RDB_type *typ)
{
    if (typ == &RDB_BOOLEAN)
        return _RDB_BOOL;
    if (typ == &RDB_INTEGER)
        return _RDB_INT;
    if (typ == &RDB_RATIONAL)
        return _RDB_RATIONAL;
    if (typ->arep == &RDB_BOOLEAN)
        return _RDB_BOOL;
    if (typ->arep == &RDB_INTEGER)
        return _RDB_INT;
    if (typ->arep == &RDB_RATIONAL)
        return _RDB_RATIONAL;
    return _RDB_BIN;
}

int
RDB_irep_to_value(RDB_value *valp, RDB_type *typ, void *datap, size_t len)
{
    int ret;

    if (valp->typ != NULL) {
        ret = RDB_destroy_value(valp);
        if (ret != RDB_OK)
            return ret;
    }

    valp->typ = typ;
    valp->kind = val_kind(typ);

    switch(valp->kind) {
        case _RDB_BOOL:
            valp->var.bool_val = *(RDB_bool *) datap;
            break;
        case _RDB_INT:
            memcpy(&valp->var.int_val, datap, sizeof (RDB_int));
            break;
        case _RDB_RATIONAL:
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
RDB_value_equals(const RDB_value *val1p, const RDB_value *val2p)
{
    switch(val1p->kind) {
        case _RDB_BOOL:
            return (RDB_bool) val1p->var.bool_val == val2p->var.bool_val;
        case _RDB_INT:
            return (RDB_bool) (val1p->var.int_val == val2p->var.int_val);
        case _RDB_RATIONAL:
            return (RDB_bool) (val1p->var.rational_val == val2p->var.rational_val);
        default:
            if (val1p->var.bin.len != val2p->var.bin.len)
                return RDB_FALSE;
            return (RDB_bool) (memcmp(val1p->var.bin.datap, val2p->var.bin.datap,
                    val1p->var.bin.len) == 0);
    }
} 

static int
copy_value(RDB_value *dstvalp, const RDB_value *srcvalp)
{
    switch(srcvalp->kind) {
        case _RDB_BOOL:
            dstvalp->var.bool_val = srcvalp->var.bool_val;
            break;
        case _RDB_INT:
            dstvalp->var.int_val = srcvalp->var.int_val;
            break;
        case _RDB_RATIONAL:
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
    int ret;

    if (dstvalp->typ != NULL) {
        ret = RDB_destroy_value(dstvalp);
        if (ret != RDB_OK)
            return ret;
    }

    dstvalp->typ = srcvalp->typ;
    dstvalp->kind = srcvalp->kind;
    return copy_value(dstvalp, srcvalp);
} 

void
RDB_init_value(RDB_value *valp)
{
    valp->typ = NULL;
}

int
RDB_destroy_value(RDB_value *valp)
{
    if (valp->typ == NULL)
        return RDB_OK;

    if (valp->kind == _RDB_BIN) {
        free(valp->var.bin.datap);
    }
    return RDB_OK;
}

void
RDB_value_set_bool(RDB_value *valp, RDB_bool v)
{
    RDB_destroy_value(valp);
    valp->typ = &RDB_BOOLEAN;
    valp->kind = _RDB_BOOL;
    valp->var.bool_val = v;
}

void
RDB_value_set_int(RDB_value *valp, RDB_int v)
{
    RDB_destroy_value(valp);
    valp->typ = &RDB_INTEGER;
    valp->kind = _RDB_INT;
    valp->var.int_val = v;
}

void
RDB_value_set_rational(RDB_value *valp, RDB_rational v)
{
    RDB_destroy_value(valp);
    valp->typ = &RDB_RATIONAL;
    valp->kind = _RDB_RATIONAL;
    valp->var.rational_val = v;
}

int
RDB_value_set_string(RDB_value *valp, const char *str)
{
    RDB_destroy_value(valp);
    valp->typ = &RDB_STRING;
    valp->kind = _RDB_BIN;
    valp->var.bin.len = strlen(str) + 1;
    valp->var.bin.datap = malloc(valp->var.bin.len);
    if (valp->var.bin.datap == NULL)
        return RDB_NO_MEMORY;
    strcpy(valp->var.bin.datap, str);
    return RDB_OK;
}

int
RDB_value_get_comp(const RDB_value *valp, const char *compname,
                   RDB_value *compvalp)
{
    int ret;
    RDB_icomp *comp = _RDB_get_icomp(valp->typ, compname);

    if (comp->setterp != NULL) {
        return (*(comp->getterp))(valp, compvalp, valp->typ, compname);
    } else {
        ret = RDB_destroy_value(compvalp);
        if (ret != RDB_OK)
            return ret;

        _RDB_set_value_type(compvalp, valp->typ->var.scalar.repv[0].compv[0].typ);
        return copy_value(compvalp, valp);   
    }
}

int
RDB_value_set_comp(RDB_value *valp, const char *compname,
                   const RDB_value *compvalp)
{
    RDB_icomp *comp = _RDB_get_icomp(valp->typ, compname);
    int ret;

    if (comp->setterp != NULL) {
        return (*(comp->setterp))(valp, compvalp, valp->typ, compname);
    } else {
        ret = RDB_destroy_value(valp);
        if (ret != RDB_OK)
            return ret;

        return copy_value(valp, compvalp);
    }
}

static int
check_constraint(RDB_value *valp, RDB_bool *resultp)
{
    int i, j;
    int ret;

    *resultp = RDB_TRUE;

    /* Check constraint for each possrep */
    for (i = 0; i < valp->typ->var.scalar.repc; i++) {
        RDB_tuple tpl;

        if (valp->typ->var.scalar.repv[i].constraintp != NULL) {
            RDB_init_tuple(&tpl);
            /* Set tuple attributes */
            for (j = 0; j < valp->typ->var.scalar.repv[i].compc; j++) {
                RDB_value comp;
                char *compname = valp->typ->var.scalar.repv[i].compv[j].name;

                RDB_init_value(&comp);
                ret = RDB_value_get_comp(valp, compname, &comp);
                if (ret != RDB_OK) {
                    RDB_destroy_value(&comp);
                    RDB_destroy_tuple(&tpl);
                    return ret;
                }
                ret = RDB_tuple_set(&tpl, compname, &comp);
                RDB_destroy_value(&comp);
                if (ret != RDB_OK) {
                    RDB_destroy_tuple(&tpl);
                    return ret;
                }
            }
            RDB_evaluate_bool(valp->typ->var.scalar.repv[i].constraintp,
                    &tpl, NULL, &*resultp);
            RDB_destroy_tuple(&tpl);
            if (!*resultp) {
                return RDB_OK;
            }
        }
    }
    return RDB_OK;
}

int
RDB_select_value(RDB_value *valp, RDB_type *typ, const char *repname,
              RDB_value **compv)
{
    RDB_ipossrep *prp;
    int i;
    int ret;
    RDB_bool b;

    if (typ->var.scalar.repc == 0 || !RDB_type_is_scalar(typ))
        return RDB_INVALID_ARGUMENT;

    if (repname == NULL) {
        if (typ->var.scalar.repc == 1) {
            repname = typ->name;
            prp = &typ->var.scalar.repv[0];
        } else {
            return RDB_INVALID_ARGUMENT;
        }
    }

    /* Find possrep */
    prp  = _RDB_get_possrep(typ, repname);
    if (prp == NULL)
        return RDB_INVALID_ARGUMENT;

    if (prp->selectorp != NULL)
        ret = (prp->selectorp)(valp, compv, typ, repname);
    else {
        RDB_destroy_value(valp);
        _RDB_set_value_type(valp, typ);
        ret = copy_value(valp, *compv);
    }
    if (ret != RDB_OK)
        return ret;

    ret = check_constraint(valp, &b);
    if (ret != RDB_OK)
        return ret;

    return b ? RDB_OK : RDB_TYPE_CONSTRAINT_VIOLATION;
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
        valp->kind = _RDB_BIN;
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

void
_RDB_set_value_type(RDB_value *valp, RDB_type *typ)
{
    valp->typ = typ;
    valp->kind = val_kind(typ);
}
