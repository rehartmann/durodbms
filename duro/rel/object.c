/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/hashmapit.h>
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
        case RDB_OB_BOOL:
            return &valp->var.bool_val;
        case RDB_OB_INT:
            return &valp->var.int_val;
        case RDB_OB_RATIONAL:
            return &valp->var.rational_val;
        default:
            return valp->var.bin.datap;
    }
} 

static size_t
obj_ilen(const RDB_object *objp)
{
    size_t len = 0;

    if (objp->kind == RDB_OB_TUPLE) {
        RDB_hashmap_iter it;
        char *key;
        void *datap;

        RDB_init_hashmap_iter(&it, (RDB_hashmap *) &objp->var.tpl_map);
        while ((datap = RDB_hashmap_next(&it, &key, NULL)) != NULL)
            len += obj_ilen((RDB_object *) datap);
        RDB_destroy_hashmap_iter(&it);
        return len + sizeof(size_t);
    }
    if (objp->kind == RDB_OB_TABLE) {
        RDB_object tpl;
        RDB_qresult *qrp;
        int ret;

        ret = _RDB_table_qresult(objp->var.tbp, NULL, &qrp);
        if (ret != RDB_OK)
            return ret;

        RDB_init_obj(&tpl);

        while ((ret = _RDB_next_tuple(qrp, &tpl, NULL)) == RDB_OK) {
            len += obj_ilen(&tpl);
        }
        RDB_destroy_obj(&tpl);
        if (ret != RDB_NOT_FOUND) {
            _RDB_drop_qresult(qrp, NULL);
            return ret;
        }

        ret = _RDB_drop_qresult(qrp, NULL);
        if (ret != RDB_OK)
            return ret;
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
        return RDB_OB_BOOL;
    if (typ == &RDB_INTEGER)
        return RDB_OB_INT;
    if (typ == &RDB_RATIONAL)
        return RDB_OB_RATIONAL;
    if (typ->arep == &RDB_BOOLEAN)
        return RDB_OB_BOOL;
    if (typ->arep == &RDB_INTEGER)
        return RDB_OB_INT;
    if (typ->arep == &RDB_RATIONAL)
        return RDB_OB_RATIONAL;
    if (typ->kind == RDB_TP_TUPLE)
        return RDB_OB_TUPLE;
    if (typ->kind == RDB_TP_RELATION)
        return RDB_OB_TABLE;
    return RDB_OB_BIN;
}

static int
irep_to_tuple(RDB_object *tplp, RDB_type *typ, const void *datap)
{
    int i;
    int ret;
    size_t len = 0;
    const RDB_byte *bp = (RDB_byte *)datap;

    for (i = 0; i < typ->var.tuple.attrc; i++) {
        RDB_object obj;
        size_t l = typ->var.tuple.attrv[i].typ->ireplen;

        if (l == RDB_VARIABLE_LEN) {
            memcpy(&l, bp, sizeof l);
            bp += sizeof (size_t);
            len += sizeof(size_t);
        }
        RDB_init_obj(&obj);
        ret = RDB_irep_to_obj(&obj, typ->var.tuple.attrv[i].typ, bp, l);
        if (ret != RDB_OK)
            return ret;
        bp += l;
        len += l;
        ret = RDB_tuple_set(tplp, typ->var.tuple.attrv[i].name, &obj);
        if (ret != RDB_OK)
            return ret;
        
        RDB_destroy_obj(&obj);
    }
    return len;
}

static int
irep_to_table(RDB_table **tbpp, RDB_type *typ, const void *datap, size_t len)
{
    int ret;
    RDB_object tpl;
    RDB_byte *bp = (RDB_byte *)datap;

    ret = RDB_create_table(NULL, RDB_FALSE,
                        typ->var.basetyp->var.tuple.attrc,
                        typ->var.basetyp->var.tuple.attrv,
                        0, NULL, NULL, tbpp);
    if (ret != RDB_OK)
        return ret;

    RDB_init_obj(&tpl);
    while (len > 0) {
        int l;
        
        l = irep_to_tuple(&tpl, typ->var.basetyp, bp);
        if (l < 0) {
            RDB_destroy_obj(&tpl);
            return l;
        }
        ret = RDB_insert(*tbpp, &tpl, NULL);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            return ret;
        }
        bp += l;
        len -= l;
    }
    RDB_destroy_obj(&tpl);

    return RDB_OK;
}

int
RDB_irep_to_obj(RDB_object *valp, RDB_type *typ, const void *datap, size_t len)
{
    int ret;
    enum _RDB_obj_kind kind;

    if (valp->kind != RDB_OB_INITIAL) {
        ret = RDB_destroy_obj(valp);
        if (ret != RDB_OK)
            return ret;
    }

    valp->typ = typ;
    kind = val_kind(typ);

    switch (kind) {
        case RDB_OB_BOOL:
            valp->var.bool_val = *(RDB_bool *) datap;
            break;
        case RDB_OB_INT:
            memcpy(&valp->var.int_val, datap, sizeof (RDB_int));
            break;
        case RDB_OB_RATIONAL:
            memcpy(&valp->var.rational_val, datap, sizeof (RDB_rational));
            break;
        case RDB_OB_TUPLE:
            ret = irep_to_tuple(valp, typ, datap);
            if (ret > 0)
                ret = RDB_OK;
            return ret;
        case RDB_OB_TABLE:
        {
            RDB_table *tbp;
        
            ret = irep_to_table(&tbp, typ, datap, len);
            if (ret != RDB_OK)
                return ret;
            RDB_table_to_obj(valp, tbp);
            return RDB_OK;
        }
        default:
            valp->var.bin.len = len;
            valp->var.bin.datap = malloc(len);
            if (valp->var.bin.datap == NULL)
                return RDB_NO_MEMORY;
            memcpy(valp->var.bin.datap, datap, len);
    }
    valp->kind = kind;
    return RDB_OK;
}

typedef struct {
    RDB_byte *bp;
    size_t len;
} irep_info;

static void *
obj_to_irep(void *dstp, const void *srcp, size_t len)
{
    const RDB_object *objp = (RDB_object *) srcp;
    RDB_byte *bp = dstp;

    switch (objp->kind) {
        case RDB_OB_BOOL:
            *bp = objp->var.bool_val;
            break;
        case RDB_OB_INT:
            memcpy(bp, &objp->var.int_val, sizeof (RDB_int));
            break;
        case RDB_OB_RATIONAL:
            memcpy(bp, &objp->var.rational_val, sizeof (RDB_rational));
            break;
        case RDB_OB_TUPLE:
        {
            int i;

            for (i = 0; i < objp->typ->var.tuple.attrc; i++) {
                RDB_object *attrp = RDB_tuple_get(objp,
                        objp->typ->var.tuple.attrv[i].name);

                int l = objp->typ->var.tuple.attrv[i].typ->ireplen;
                if (l == RDB_VARIABLE_LEN) {
                    l = obj_ilen(attrp);
                    memcpy(bp, &l, sizeof (size_t));
                    bp += sizeof (size_t);
                }
                obj_to_irep(bp, attrp, l);
                bp += l;
            }
            break;
        }
        case RDB_OB_TABLE:
        {
            RDB_object tpl;
            RDB_qresult *qrp;
            int ret;
            size_t l;

            ret = _RDB_table_qresult(objp->var.tbp, NULL, &qrp);
            if (ret != RDB_OK)
                return NULL;

            RDB_init_obj(&tpl);

            while ((ret = _RDB_next_tuple(qrp, &tpl, NULL)) == RDB_OK) {
                tpl.typ = objp->typ->var.basetyp;
                l = obj_ilen(&tpl);
                obj_to_irep(bp, &tpl, l);
                bp += l;
            }
            RDB_destroy_obj(&tpl);
            if (ret != RDB_NOT_FOUND) {
                _RDB_drop_qresult(qrp, NULL);
                return NULL;
            }

            ret = _RDB_drop_qresult(qrp, NULL);
            if (ret != RDB_OK)
                return NULL;
            break;
        }
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
        case RDB_OB_BOOL:
            return (RDB_bool) val1p->var.bool_val == val2p->var.bool_val;
        case RDB_OB_INT:
            return (RDB_bool) (val1p->var.int_val == val2p->var.int_val);
        case RDB_OB_RATIONAL:
            return (RDB_bool) (val1p->var.rational_val == val2p->var.rational_val);
        case RDB_OB_INITIAL:
            return RDB_FALSE;
        case RDB_OB_BIN:
            if (val1p->var.bin.len != val2p->var.bin.len)
                return RDB_FALSE;
            if (val1p->var.bin.len == 0)
                return RDB_TRUE;
            return (RDB_bool) (memcmp(val1p->var.bin.datap, val2p->var.bin.datap,
                    val1p->var.bin.len) == 0);
        case RDB_OB_TUPLE:
            return _RDB_tuple_equals(val1p, val2p);
        case RDB_OB_TABLE:
        case RDB_OB_ARRAY:
            /* !! */
            return RDB_FALSE;
    }
    return RDB_FALSE;
} 

/* Copy data only, not the type information */
int
_RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp)
{
    switch (srcvalp->kind) {
        case RDB_OB_BOOL:
            dstvalp->kind = srcvalp->kind;
            dstvalp->var.bool_val = srcvalp->var.bool_val;
            break;
        case RDB_OB_INT:
            dstvalp->kind = srcvalp->kind;
            dstvalp->var.int_val = srcvalp->var.int_val;
            break;
        case RDB_OB_RATIONAL:
            dstvalp->kind = srcvalp->kind;
            dstvalp->var.rational_val = srcvalp->var.rational_val;
            break;
        case RDB_OB_TUPLE:
            return _RDB_copy_tuple(dstvalp, srcvalp);
        case RDB_OB_TABLE:
            /* The table itself is not copied, only the pointer */
            dstvalp->kind = srcvalp->kind;
            dstvalp->var.tbp = srcvalp->var.tbp;
            srcvalp->var.tbp->refcount++;
            break;
        default:
            dstvalp->kind = srcvalp->kind;
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

    if (dstvalp->kind != RDB_OB_INITIAL) {
        ret = RDB_destroy_obj(dstvalp);
        if (ret != RDB_OK)
            return ret;
    }

    if (srcvalp->typ != NULL && RDB_type_is_scalar(srcvalp->typ))
        dstvalp->typ = srcvalp->typ;
    return _RDB_copy_obj(dstvalp, srcvalp);
} 

void
RDB_init_obj(RDB_object *valp)
{
    valp->kind = RDB_OB_INITIAL;
    valp->typ = NULL;
}

int
RDB_destroy_obj(RDB_object *objp)
{
    switch (objp->kind) {
        case RDB_OB_BIN:
            if (objp->var.bin.len > 0)
                free(objp->var.bin.datap);
            break;
        case RDB_OB_TUPLE:
        {
            RDB_hashmap_iter it;
            char *key;
            void *datap;

            RDB_init_hashmap_iter(&it, (RDB_hashmap *) &objp->var.tpl_map);
            while ((datap = RDB_hashmap_next(&it, &key, NULL)) != NULL)
                RDB_destroy_obj((RDB_object *) datap);
            RDB_destroy_hashmap_iter(&it);
            RDB_destroy_hashmap(&objp->var.tpl_map);
            break;
        }
        case RDB_OB_ARRAY:
        {
            int ret = RDB_OK;
            int ret2 = RDB_OK;

            if (objp->var.arr.tbp == NULL)
                return RDB_OK;
            
            if (objp->var.arr.qrp != NULL) {
                ret = _RDB_drop_qresult(objp->var.arr.qrp,
                        objp->var.arr.txp);

                if (ret != RDB_OK) {
                    if (RDB_is_syserr(ret) && objp->var.arr.txp != NULL)
                        RDB_rollback_all(objp->var.arr.txp);
                }
            }
            if (objp->var.arr.tplp != NULL) {
                ret2 = RDB_destroy_obj(objp->var.arr.tplp);
                free(objp->var.arr.tplp);
            }
            if (ret != RDB_OK)
                return ret;
            return ret2;
        }
        case RDB_OB_TABLE:
            objp->var.tbp->refcount--;
            if (objp->var.tbp->refcount == 0
                    && objp->var.tbp->name == NULL) {
                return RDB_drop_table(objp->var.tbp, NULL);
            }
            break;
        default: ;
    }
    return RDB_OK;
}

void
RDB_bool_to_obj(RDB_object *valp, RDB_bool v)
{
    RDB_destroy_obj(valp);
    valp->typ = &RDB_BOOLEAN;
    valp->kind = RDB_OB_BOOL;
    valp->var.bool_val = v;
}

void
RDB_int_to_obj(RDB_object *valp, RDB_int v)
{
    RDB_destroy_obj(valp);
    valp->typ = &RDB_INTEGER;
    valp->kind = RDB_OB_INT;
    valp->var.int_val = v;
}

void
RDB_rational_to_obj(RDB_object *valp, RDB_rational v)
{
    RDB_destroy_obj(valp);
    valp->typ = &RDB_RATIONAL;
    valp->kind = RDB_OB_RATIONAL;
    valp->var.rational_val = v;
}

int
RDB_string_to_obj(RDB_object *valp, const char *str)
{
    RDB_destroy_obj(valp);
    valp->typ = &RDB_STRING;
    valp->kind = RDB_OB_BIN;
    valp->var.bin.len = strlen(str) + 1;
    valp->var.bin.datap = malloc(valp->var.bin.len);
    if (valp->var.bin.datap == NULL)
        return RDB_NO_MEMORY;
    strcpy(valp->var.bin.datap, str);
    return RDB_OK;
}

int
RDB_obj_comp(const RDB_object *valp, const char *compname, RDB_object *compvalp,
        RDB_transaction *txp)
{
    int ret;
    RDB_icomp *comp;
    
    if (!RDB_type_is_scalar(valp->typ) || valp->typ->var.scalar.repc == 0)
        return RDB_INVALID_ARGUMENT;

    comp = _RDB_get_icomp(valp->typ, compname);

    if (valp->typ->var.scalar.sysimpl) {
        ret = RDB_destroy_obj(compvalp);
        if (ret != RDB_OK)
            return ret;

        _RDB_set_obj_type(compvalp, valp->typ->var.scalar.repv[0].compv[0].typ);
        ret = _RDB_copy_obj(compvalp, valp);   
    } else {
        /* Getter is implemented by user */
        char *opname;
        RDB_object *argv[1];

        opname = malloc(strlen(valp->typ->name) + strlen(comp->name) + 6);
        if (opname == NULL)
            return RDB_NO_MEMORY;

        strcpy(opname, valp->typ->name);
        strcat(opname, "_get_");
        strcat(opname, comp->name);
        argv[0] = (RDB_object *) valp;

        ret = RDB_call_ro_op(opname, 1, argv, compvalp, txp);
        free(opname);
    }
    return ret;
}

int
RDB_obj_set_comp(RDB_object *valp, const char *compname,
        const RDB_object *compvalp, RDB_transaction *txp)
{
    RDB_icomp *comp = _RDB_get_icomp(valp->typ, compname);
    int ret;

    if (valp->typ->var.scalar.sysimpl) {
        /* Setter is implemented by the system */
        ret = RDB_destroy_obj(valp);
        if (ret != RDB_OK)
            return ret;

        ret = _RDB_copy_obj(valp, compvalp);
    } else {
        /* Setter is implemented by user */
        char *opname;
        RDB_object *argv[2];

        opname = malloc(strlen(valp->typ->name) + strlen(comp->name) + 6);
        if (opname == NULL)
            return RDB_NO_MEMORY;

        strcpy(opname, valp->typ->name);
        strcat(opname, "_set_");
        strcat(opname, comp->name);
        argv[0] = valp;
        argv[1] = (RDB_object *) compvalp;
        
        ret = RDB_call_update_op(opname, 2, argv, txp);
        free(opname);        
    }
    
    /* Check constraint !? */
    return ret;
}

void
RDB_table_to_obj(RDB_object *objp, RDB_table *tbp)
{
    RDB_destroy_obj(objp);
    objp->typ = tbp->typ;
    objp->kind = RDB_OB_TABLE;
    objp->var.tbp = tbp;
    tbp->refcount++;
}

RDB_table *
RDB_obj_table(const RDB_object *objp)
{
    return objp->var.tbp;
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
RDB_obj_string(const RDB_object *valp)
{
    return valp->var.bin.datap;
}

int
RDB_binary_set(RDB_object *valp, size_t pos, const void *srcp, size_t len)
{
    /* If the value is newly initialized, allocate memory */
    if (valp->kind == RDB_OB_INITIAL) {
        valp->var.bin.len = pos + len;
        if (valp->var.bin.len > 0)
            valp->var.bin.datap = malloc(valp->var.bin.len);
        if (valp->var.bin.datap == NULL)
            return RDB_NO_MEMORY;
        valp->typ = &RDB_BINARY;
        valp->kind = RDB_OB_BIN;
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

/* Works only for scalar types */
void
_RDB_set_obj_type(RDB_object *valp, RDB_type *typ)
{
    valp->typ = typ;
    valp->kind = val_kind(typ);
    if (valp->kind == RDB_OB_BIN)
        valp->var.bin.datap = NULL;
}
