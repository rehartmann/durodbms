/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/errors.h>
#include <string.h>

void *
RDB_obj_irep(RDB_object *valp, size_t *lenp)
{
    if (lenp != NULL) {
        *lenp = valp->typ->ireplen;
        if (*lenp == RDB_VARIABLE_LEN)
            *lenp = valp->var.bin.len;
    }

    switch (valp->kind) {
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

static size_t
obj_ilen(const RDB_object *objp)
{
    size_t len, l;
    int i;

    if (objp->typ->kind == RDB_TP_TUPLE) {
        len = 0;
        for (i = 0; i < objp->typ->var.tuple.attrc; i++) {
            l = objp->typ->var.tuple.attrv[i].typ->ireplen;
            if (l == RDB_VARIABLE_LEN) {
                l = sizeof(size_t) + obj_ilen(RDB_tuple_get(&objp->var.tpl,
                        objp->typ->var.tuple.attrv[i].name));
            }
            len += l;
        }
        return len;
    }
    len = objp->typ->ireplen;
    if (len == RDB_VARIABLE_LEN)
        len = objp->var.bin.len;
    return len;
}

static enum _RDB_obj_kind
val_kind(const RDB_type *typ)
{
    if (typ == &RDB_BOOLEAN)
        return _RDB_BOOL;
    if (typ == &RDB_INTEGER)
        return _RDB_INT;
    if (typ == &RDB_RATIONAL)
        return _RDB_RATIONAL;
    if (typ->kind == RDB_TP_TUPLE)
        return _RDB_TUPLE;
    if (typ->arep == &RDB_BOOLEAN)
        return _RDB_BOOL;
    if (typ->arep == &RDB_INTEGER)
        return _RDB_INT;
    if (typ->arep == &RDB_RATIONAL)
        return _RDB_RATIONAL;
    return _RDB_BIN;
}

static int
irep_to_tuple(RDB_tuple *tplp, RDB_type *typ, const void *datap)
{
    int i;
    int ret;
    const RDB_byte *bp = (RDB_byte *)datap;

    for (i = 0; i < typ->var.tuple.attrc; i++) {
        RDB_object obj;
        size_t l = typ->var.tuple.attrv[i].typ->ireplen;

        if (l == RDB_VARIABLE_LEN) {
            memcpy(&l, bp, sizeof l);
            bp += sizeof (size_t);
        }
        RDB_init_obj(&obj);
        ret = RDB_irep_to_obj(&obj, typ->var.tuple.attrv[i].typ, bp, l);
        if (ret != RDB_OK)
            return ret;
        bp += l;
        RDB_tuple_set(tplp, typ->var.tuple.attrv[i].name, &obj);
        
        RDB_destroy_obj(&obj);
    }
    return RDB_OK;
}

int
RDB_irep_to_obj(RDB_object *valp, RDB_type *typ, const void *datap, size_t len)
{
    int ret;

    if (valp->typ != NULL) {
        ret = RDB_destroy_obj(valp);
        if (ret != RDB_OK)
            return ret;
    }

    valp->typ = typ;
    valp->kind = val_kind(typ);

    switch (valp->kind) {
        case _RDB_BOOL:
            valp->var.bool_val = *(RDB_bool *) datap;
            break;
        case _RDB_INT:
            memcpy(&valp->var.int_val, datap, sizeof (RDB_int));
            break;
        case _RDB_RATIONAL:
            memcpy(&valp->var.rational_val, datap, sizeof (RDB_rational));
            break;
        case _RDB_TUPLE:
            RDB_init_tuple(&valp->var.tpl);
            return irep_to_tuple(&valp->var.tpl, typ, datap);
        default:
            valp->var.bin.len = len;
            valp->var.bin.datap = malloc(len);
            if (valp->var.bin.datap == NULL)
                return RDB_NO_MEMORY;
            memcpy(valp->var.bin.datap, datap, len);
    }
    return RDB_OK;
}

static void *
obj_to_irep(void *dstp, const void *srcp, size_t len)
{
    int i;
    const RDB_object *objp = (RDB_object *) srcp;
    RDB_byte *bp = dstp;

    if (objp->typ->kind == RDB_TP_TUPLE) {
        for (i = 0; i < objp->typ->var.tuple.attrc; i++) {
            RDB_object *attrp = RDB_tuple_get(&objp->var.tpl,
                    objp->typ->var.tuple.attrv[i].name);
            size_t l = objp->typ->var.tuple.attrv[i].typ->ireplen;

            if (l == RDB_VARIABLE_LEN) {
                l = obj_ilen(attrp);
                memcpy(bp, &l, sizeof l);
                bp += sizeof l;
            }
            obj_to_irep(bp, attrp, l);
            bp += l;
        }
        return dstp;
    }

    switch (objp->kind) {
        case _RDB_BOOL:
            *bp = objp->var.bool_val;
            break;
        case _RDB_INT:
            memcpy(bp, &objp->var.int_val, sizeof (RDB_int));
            break;
        case _RDB_RATIONAL:
            memcpy(bp, &objp->var.rational_val, sizeof (RDB_rational));
            break;
        default:
            memcpy(bp, objp->var.bin.datap, objp->var.bin.len);
    }
    return dstp;
} 

void
_RDB_obj_to_field(RDB_field *fvp, RDB_object *objp)
{
    fvp->datap = objp;
    fvp->copyfp = obj_to_irep;
    fvp->len = obj_ilen(objp);
}

RDB_bool
RDB_obj_equals(const RDB_object *val1p, const RDB_object *val2p)
{
    switch (val1p->kind) {
        case _RDB_BOOL:
            return (RDB_bool) val1p->var.bool_val == val2p->var.bool_val;
        case _RDB_INT:
            return (RDB_bool) (val1p->var.int_val == val2p->var.int_val);
        case _RDB_RATIONAL:
            return (RDB_bool) (val1p->var.rational_val == val2p->var.rational_val);
        default:
            if (val1p->var.bin.len != val2p->var.bin.len)
                return RDB_FALSE;
            if (val1p->var.bin.len == 0)
                return RDB_TRUE;
            return (RDB_bool) (memcmp(val1p->var.bin.datap, val2p->var.bin.datap,
                    val1p->var.bin.len) == 0);
    }
} 

static int
copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp)
{
    dstvalp->kind = srcvalp->kind;
    dstvalp->typ = dstvalp->typ;
    switch (srcvalp->kind) {
        case _RDB_BOOL:
            dstvalp->var.bool_val = srcvalp->var.bool_val;
            break;
        case _RDB_INT:
            dstvalp->var.int_val = srcvalp->var.int_val;
            break;
        case _RDB_RATIONAL:
            dstvalp->var.rational_val = srcvalp->var.rational_val;
            break;
        case _RDB_TUPLE:
            RDB_copy_tuple(&dstvalp->var.tpl, &srcvalp->var.tpl);
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
RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp)
{
    int ret;

    if (dstvalp->typ != NULL) {
        ret = RDB_destroy_obj(dstvalp);
        if (ret != RDB_OK)
            return ret;
    }

    _RDB_set_obj_type(dstvalp, srcvalp->typ);
/*
    dstvalp->typ = srcvalp->typ;
    dstvalp->kind = srcvalp->kind;
*/
    return copy_obj(dstvalp, srcvalp);
} 

void
RDB_init_obj(RDB_object *valp)
{
    valp->typ = NULL;
}

int
RDB_destroy_obj(RDB_object *valp)
{
    if (valp->typ == NULL)
        return RDB_OK;

    if (valp->kind == _RDB_BIN && (valp->var.bin.len > 0)) {
        free(valp->var.bin.datap);
    } else if (valp->kind == _RDB_TUPLE) {
        RDB_destroy_tuple(&valp->var.tpl);
    }
    return RDB_OK;
}

void
RDB_obj_set_bool(RDB_object *valp, RDB_bool v)
{
    RDB_destroy_obj(valp);
    valp->typ = &RDB_BOOLEAN;
    valp->kind = _RDB_BOOL;
    valp->var.bool_val = v;
}

void
RDB_obj_set_int(RDB_object *valp, RDB_int v)
{
    RDB_destroy_obj(valp);
    valp->typ = &RDB_INTEGER;
    valp->kind = _RDB_INT;
    valp->var.int_val = v;
}

void
RDB_obj_set_rational(RDB_object *valp, RDB_rational v)
{
    RDB_destroy_obj(valp);
    valp->typ = &RDB_RATIONAL;
    valp->kind = _RDB_RATIONAL;
    valp->var.rational_val = v;
}

int
RDB_obj_set_string(RDB_object *valp, const char *str)
{
    RDB_destroy_obj(valp);
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
RDB_obj_comp(const RDB_object *valp, const char *compname,
                   RDB_object *compvalp)
{
    int ret;
    RDB_icomp *comp;
    
    if (!RDB_type_is_scalar(valp->typ) || valp->typ->var.scalar.repc == 0)
        return RDB_INVALID_ARGUMENT;

    comp = _RDB_get_icomp(valp->typ, compname);
    if (comp->setterp != NULL) {
        return (*(comp->getterp))(valp, compvalp, valp->typ, compname);
    } else {
        ret = RDB_destroy_obj(compvalp);
        if (ret != RDB_OK)
            return ret;

        _RDB_set_obj_type(compvalp, valp->typ->var.scalar.repv[0].compv[0].typ);
        return copy_obj(compvalp, valp);   
    }
}

int
RDB_obj_set_comp(RDB_object *valp, const char *compname,
                   const RDB_object *compvalp)
{
    RDB_icomp *comp = _RDB_get_icomp(valp->typ, compname);
    int ret;

    if (comp->setterp != NULL) {
        return (*(comp->setterp))(valp, compvalp, valp->typ, compname);
    } else {
        ret = RDB_destroy_obj(valp);
        if (ret != RDB_OK)
            return ret;

        return copy_obj(valp, compvalp);
    }
}

static int
check_constraint(RDB_object *valp, RDB_bool *resultp)
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
                RDB_object comp;
                char *compname = valp->typ->var.scalar.repv[i].compv[j].name;

                RDB_init_obj(&comp);
                ret = RDB_obj_comp(valp, compname, &comp);
                if (ret != RDB_OK) {
                    RDB_destroy_obj(&comp);
                    RDB_destroy_tuple(&tpl);
                    return ret;
                }
                ret = RDB_tuple_set(&tpl, compname, &comp);
                RDB_destroy_obj(&comp);
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
RDB_select_obj(RDB_object *valp, RDB_type *typ, const char *repname,
              RDB_object **compv)
{
    RDB_ipossrep *prp;
    int ret;
    RDB_bool b;

    if (!RDB_type_is_scalar(typ) || typ->var.scalar.repc == 0)
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
        RDB_destroy_obj(valp);
        _RDB_set_obj_type(valp, typ);
        ret = copy_obj(valp, *compv);
    }
    if (ret != RDB_OK)
        return ret;

    ret = check_constraint(valp, &b);
    if (ret != RDB_OK)
        return ret;

    return b ? RDB_OK : RDB_TYPE_CONSTRAINT_VIOLATION;
}

RDB_bool
RDB_obj_bool(const RDB_object *valp)
{
    return valp->var.bool_val;
}

RDB_int
RDB_obj_int(const RDB_object *valp)
{
    return valp->var.int_val;
}

RDB_rational
RDB_obj_rational(const RDB_object *valp)
{
    return valp->var.rational_val;
}

char *
RDB_obj_string(RDB_object *valp)
{
    return valp->var.bin.datap;
}

int
RDB_binary_set(RDB_object *valp, size_t pos, const void *srcp, size_t len)
{
    /* If the value is newly initialized, allocate memory */
    if (valp->typ == NULL) {
        valp->var.bin.len = pos + len;
        if (valp->var.bin.len > 0)
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
    if (len > 0)
        memcpy(((RDB_byte *)valp->var.bin.datap) + pos, srcp, len);
    return RDB_OK;
}

int
RDB_binary_get(const RDB_object *valp, size_t pos, void *dstp, size_t len)
{
    if (pos + len > valp->var.bin.len)
        return RDB_NOT_FOUND;
    memcpy(dstp, ((RDB_byte *)valp->var.bin.datap) + pos, len);
    return RDB_OK;
}

size_t
RDB_binary_length(const RDB_object *valp)
{
    return valp->var.bin.len;
}

void
_RDB_set_obj_type(RDB_object *valp, RDB_type *typ)
{
    valp->typ = typ; /* !! copy type? */
    valp->kind = val_kind(typ);
    if (valp->kind == _RDB_BIN)
        valp->var.bin.datap = NULL;
    else if (valp->kind == _RDB_TUPLE)
        RDB_init_tuple(&valp->var.tpl);
}

RDB_tuple *
RDB_obj_tuple(RDB_object *objp)
{
    return &objp->var.tpl;
}
