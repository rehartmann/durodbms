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
#include <assert.h>

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

static int
obj_ilen(const RDB_object *objp, size_t *lenp)
{
    int ret;
    size_t len;

    *lenp = 0;

    switch(objp->kind) {
        case RDB_OB_TUPLE:
        {
            int i;
            RDB_type *tpltyp = objp->typ;

            if (RDB_type_is_scalar(tpltyp))
                tpltyp = tpltyp->var.scalar.arep;

            for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
                RDB_type *attrtyp = tpltyp->var.tuple.attrv[i].typ;
                RDB_object *attrobjp = RDB_tuple_get(objp,
                        tpltyp->var.tuple.attrv[i].name);

                if (attrobjp->typ == NULL)
                    attrobjp->typ = attrtyp;
                if (attrtyp->ireplen == RDB_VARIABLE_LEN)
                    *lenp += sizeof (size_t);
                ret = obj_ilen(attrobjp, &len);
                if (ret != RDB_OK)
                    return ret;
                *lenp += len;
            }

            return RDB_OK;
        }
        case RDB_OB_TABLE:
        {
            RDB_object tpl;
            RDB_qresult *qrp;

            ret = _RDB_table_qresult(objp->var.tbp, NULL, &qrp);
            if (ret != RDB_OK)
                return ret;

            RDB_init_obj(&tpl);

            while ((ret = _RDB_next_tuple(qrp, &tpl, NULL)) == RDB_OK) {
                tpl.typ = objp->var.tbp->typ->var.basetyp;
                ret = obj_ilen(&tpl, &len);
                if (ret != RDB_OK) {
                     RDB_destroy_obj(&tpl);
                    _RDB_drop_qresult(qrp, NULL);
                    return ret;
                }
                *lenp += len;
            }
            RDB_destroy_obj(&tpl);
            if (ret != RDB_NOT_FOUND) {
                _RDB_drop_qresult(qrp, NULL);
                return ret;
            }

            ret = _RDB_drop_qresult(qrp, NULL);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        }
        case RDB_OB_ARRAY:
        {
            RDB_object *elemp;
            int i = 0;

            while ((ret = RDB_array_get((RDB_object *)objp, (RDB_int) i++,
                    &elemp)) == RDB_OK) {
                elemp->typ = RDB_obj_type(objp)->var.basetyp;
                if (elemp->typ->ireplen == RDB_VARIABLE_LEN)
                    *lenp += sizeof (size_t);
                ret = obj_ilen(elemp, &len);
                if (ret != RDB_OK)
                    return ret;
                *lenp += len;
            }
            if (ret != RDB_NOT_FOUND)
                return ret;
            return RDB_OK;
        }            
        default: ;
    }
    *lenp = RDB_obj_type(objp)->ireplen;
    if (*lenp == RDB_VARIABLE_LEN)
        *lenp = objp->var.bin.len;
    return RDB_OK;
}

static enum _RDB_obj_kind
val_kind(const RDB_type *typ)
{
    switch (typ->kind) {
        case RDB_TP_SCALAR:
            if (typ == &RDB_BOOLEAN)
                return RDB_OB_BOOL;
            if (typ == &RDB_INTEGER)
                return RDB_OB_INT;
            if (typ == &RDB_RATIONAL)
                return RDB_OB_RATIONAL;
            if (typ->var.scalar.arep != NULL)
                return val_kind(typ->var.scalar.arep);
            return RDB_OB_BIN;
        case RDB_TP_TUPLE:
            return RDB_OB_TUPLE;
        case RDB_TP_RELATION:
            return RDB_OB_TABLE;
        case RDB_TP_ARRAY:
            return RDB_OB_ARRAY;
    }
    /* Should never be reached */
    abort();
}

static int
len_irep_to_obj(RDB_object *valp, RDB_type *typ, const void *datap)
{
    int ret;
    size_t llen = 0;
    size_t len = typ->ireplen;
    RDB_byte *bp = (RDB_byte *)datap;

    if (len == RDB_VARIABLE_LEN) {
        memcpy(&len, bp, sizeof len);
        llen = sizeof (size_t);
        bp += sizeof (size_t);
    }

    ret = RDB_irep_to_obj(valp, typ, bp, len);
    if (ret != RDB_OK)
        return ret;
    return llen + len;
}

static int
irep_to_tuple(RDB_object *tplp, RDB_type *typ, const void *datap)
{
    int i;
    int ret;
    size_t len = 0;
    const RDB_byte *bp = (RDB_byte *) datap;

    if (RDB_type_is_scalar(typ))
        typ = typ->var.scalar.arep;

    for (i = 0; i < typ->var.tuple.attrc; i++) {
        RDB_object obj;
        size_t l;

        RDB_init_obj(&obj);
        l = len_irep_to_obj(&obj, typ->var.tuple.attrv[i].typ, bp);
        if (l < 0) {
            RDB_destroy_obj(&obj);
            return l;
        }
        bp += l;
        len += l;
        ret = RDB_tuple_set(tplp, typ->var.tuple.attrv[i].name, &obj);
        RDB_destroy_obj(&obj);
        if (ret != RDB_OK)
            return ret;        
    }
    return len;
}

static int
irep_to_table(RDB_table **tbpp, RDB_type *typ, const void *datap, size_t len)
{
    int ret;
    RDB_object tpl;
    RDB_byte *bp = (RDB_byte *)datap;

    if (RDB_type_is_scalar(typ))
        typ = typ->var.scalar.arep;

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

static int
irep_to_array(RDB_object *arrp, RDB_type *typ, const void *datap, size_t len)
{
    int ret;
    int i;
    RDB_object tpl;
    int arrlen = 0;
    RDB_byte *bp = (RDB_byte *)datap;

    RDB_init_obj(&tpl);

    /* Determine array size */
    while (len > 0) {
        int l;

        l = len_irep_to_obj(&tpl, typ->var.basetyp, bp);
        if (l < 0) {
            RDB_destroy_obj(&tpl);
            return l;
        }
        bp += l;
        len -= l;
        arrlen++;
    }

    ret = RDB_set_array_length(arrp, (RDB_int) arrlen);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl);
        return ret;
    }
    
    bp = (RDB_byte *)datap;
    for (i = 0; i < arrlen; i++) {
        int l = len_irep_to_obj(&tpl, typ->var.basetyp, bp);

        ret = RDB_array_set(arrp, i, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            return ret;
        }
        bp += l;
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
        case RDB_OB_INITIAL:
        case RDB_OB_BOOL:
            valp->var.bool_val = *(RDB_bool *) datap;
            break;
        case RDB_OB_INT:
            memcpy(&valp->var.int_val, datap, sizeof (RDB_int));
            break;
        case RDB_OB_RATIONAL:
            memcpy(&valp->var.rational_val, datap, sizeof (RDB_rational));
            break;
        case RDB_OB_BIN:
            valp->var.bin.len = len;
            valp->var.bin.datap = malloc(len);
            if (valp->var.bin.datap == NULL)
                return RDB_NO_MEMORY;
            memcpy(valp->var.bin.datap, datap, len);
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
            if (RDB_type_is_scalar(typ))
                valp->typ = typ;
            return RDB_OK;
        }
        case RDB_OB_ARRAY:
            return irep_to_array(valp, typ, datap, len);
    }
    valp->kind = kind;
    return RDB_OK;
}

typedef struct {
    RDB_byte *bp;
    size_t len;
} irep_info;

static void *
obj_to_irep(void *dstp, const void *srcp, size_t len);

static void *
obj_to_len_irep(void *dstp, const RDB_object *objp, RDB_type *typ)
{
    int ret;
    RDB_byte *bp = dstp;
    size_t len = typ->ireplen;

    if (len == RDB_VARIABLE_LEN) {
        ret = obj_ilen(objp, &len);
        if (ret != RDB_OK)
            return NULL;
        memcpy(bp, &len, sizeof (size_t));
        bp += sizeof (size_t);
    }
    obj_to_irep(bp, objp, len);
    bp += len;

    return bp;
}    

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
            RDB_type *tpltyp = objp->typ;
            int i;

            /* If the type is scalar, use internal rep */
            if (tpltyp->kind == RDB_TP_SCALAR)
                tpltyp = tpltyp->var.scalar.arep;

            for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
                RDB_object *attrp;

                attrp = RDB_tuple_get(objp, tpltyp->var.tuple.attrv[i].name);
                bp = obj_to_len_irep(bp, attrp, tpltyp->var.tuple.attrv[i].typ);
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
                tpl.typ = objp->var.tbp->typ->var.basetyp;
                ret = obj_ilen(&tpl, &l);
                if (ret != RDB_OK)
                    return NULL;
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
        case RDB_OB_ARRAY:
        {
            RDB_object *elemp;
            int ret;
            int i = 0;

            while ((ret = RDB_array_get((RDB_object *) objp, (RDB_int) i++,
                    &elemp)) == RDB_OK) {
                elemp->typ = RDB_obj_type(objp)->var.basetyp;
                bp = obj_to_len_irep(bp, elemp, elemp->typ);
            }
            if (ret != RDB_NOT_FOUND)
                return NULL;
            break;
        }
        default:
            memcpy(bp, objp->var.bin.datap, objp->var.bin.len);
    }
    return dstp;
}

int
_RDB_obj_to_field(RDB_field *fvp, RDB_object *objp)
{
    /* Global tables cannot be converted to field values */
    if (objp->kind == RDB_OB_TABLE && objp->var.tbp->is_persistent)
        return RDB_INVALID_ARGUMENT;

    fvp->datap = objp;
    fvp->copyfp = obj_to_irep;
    return obj_ilen(objp, &fvp->len);
}

int
RDB_obj_equals(const RDB_object *val1p, const RDB_object *val2p,
        RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_object retval;
    RDB_object *argv[2];

    argv[0] = (RDB_object *) val1p;
    argv[1] = (RDB_object *) val2p;
    RDB_init_obj(&retval);
    ret = RDB_call_ro_op("=", 2, argv, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval);
        if (ret == RDB_NOT_FOUND)
            ret = RDB_INTERNAL;
        return ret;
    }
    *resp = RDB_obj_bool(&retval);
    return RDB_destroy_obj(&retval);
}


/* Copy data only, not the type information. Assume non-initialized
   destination. */
int
_RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp)
{
    switch (srcvalp->kind) {
        case RDB_OB_INITIAL:
            break;
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
        case RDB_OB_ARRAY:
            return _RDB_copy_array(dstvalp, srcvalp);
        case RDB_OB_TABLE:
            /* The table itself is not copied, only the pointer */
            dstvalp->kind = srcvalp->kind;
            dstvalp->var.tbp = srcvalp->var.tbp;
            srcvalp->var.tbp->refcount++;
            break;
        case RDB_OB_BIN:
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
        case RDB_OB_INITIAL:
        case RDB_OB_BOOL:
        case RDB_OB_INT:
        case RDB_OB_RATIONAL:
            break;
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

            if (objp->var.arr.elemv != NULL) {
                int i;

                for (i = 0; i < objp->var.arr.elemc; i++)
                    RDB_destroy_obj(&objp->var.arr.elemv[i]);
                free(objp->var.arr.elemv);
            }

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

            /* Delete optimzed table copy (only if it's a virtual table) */
            if (objp->var.arr.tbp->kind != RDB_TB_REAL) {
                ret = RDB_drop_table(objp->var.arr.tbp, NULL);
                if (ret != RDB_OK)
                    return ret;
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
    }
    return RDB_OK;
}

void
RDB_bool_to_obj(RDB_object *valp, RDB_bool v)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_BOOLEAN);

    valp->typ = &RDB_BOOLEAN;
    valp->kind = RDB_OB_BOOL;
    valp->var.bool_val = v;
}

void
RDB_int_to_obj(RDB_object *valp, RDB_int v)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_INTEGER);

    valp->typ = &RDB_INTEGER;
    valp->kind = RDB_OB_INT;
    valp->var.int_val = v;
}

void
RDB_rational_to_obj(RDB_object *valp, RDB_rational v)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_RATIONAL);

    valp->typ = &RDB_RATIONAL;
    valp->kind = RDB_OB_RATIONAL;
    valp->var.rational_val = v;
}

int
RDB_string_to_obj(RDB_object *valp, const char *str)
{
    void *datap;
    int len = strlen(str) + 1;

    if(valp->kind != RDB_OB_INITIAL && valp->typ != &RDB_STRING)
        return RDB_TYPE_MISMATCH;

    if (valp->kind == RDB_OB_INITIAL) {
        datap = malloc(len);
        if (datap == NULL)
            return RDB_NO_MEMORY;
        valp->typ = &RDB_STRING;
        valp->kind = RDB_OB_BIN;
    } else {
        datap = realloc(valp->var.bin.datap, len);
        if (datap == NULL)
            return RDB_NO_MEMORY;
    }
    valp->var.bin.len = len;
    valp->var.bin.datap = datap;

    strcpy(valp->var.bin.datap, str);
    return RDB_OK;
}

int
RDB_obj_comp(const RDB_object *valp, const char *compname, RDB_object *compvalp,
        RDB_transaction *txp)
{
    int ret;
    
    if (!RDB_type_is_scalar(valp->typ) || valp->typ->var.scalar.repc == 0)
        return RDB_INVALID_ARGUMENT;

    if (valp->typ->var.scalar.sysimpl) {
        ret = RDB_destroy_obj(compvalp);
        if (ret != RDB_OK)
            return ret;

        if (valp->typ->var.scalar.repv[0].compc == 1) {
            _RDB_set_obj_type(compvalp,
                    valp->typ->var.scalar.repv[0].compv[0].typ);
            ret = _RDB_copy_obj(compvalp, valp);
        } else {
            RDB_init_obj(compvalp);
            RDB_copy_obj(compvalp, RDB_tuple_get(valp, compname));
        }
    } else {
        /* Getter is implemented by user */
        char *opname;
        RDB_object *argv[1];

        opname = malloc(strlen(valp->typ->name) + strlen(compname) + 6);
        if (opname == NULL)
            return RDB_NO_MEMORY;

        strcpy(opname, valp->typ->name);
        strcat(opname, "_get_");
        strcat(opname, compname);
        argv[0] = (RDB_object *) valp;

        ret = RDB_call_ro_op(opname, 1, argv, txp, compvalp);
        free(opname);
    }
    return ret;
}

int
RDB_obj_set_comp(RDB_object *valp, const char *compname,
        const RDB_object *compvalp, RDB_transaction *txp)
{
    int ret;

    if (valp->typ->var.scalar.sysimpl) {
        /* Setter is implemented by the system */
        if (valp->typ->var.scalar.repv[0].compc == 1) {
            ret = RDB_destroy_obj(valp);
            if (ret != RDB_OK)
                return ret;

            ret = _RDB_copy_obj(valp, compvalp);
        } else {
            ret = RDB_tuple_set(valp, compname, compvalp);
        }
    } else {
        /* Setter is implemented by user */
        char *opname;
        RDB_object *argv[2];

        opname = malloc(strlen(valp->typ->name) + strlen(compname) + 6);
        if (opname == NULL)
            return RDB_NO_MEMORY;

        strcpy(opname, valp->typ->name);
        strcat(opname, "_set_");
        strcat(opname, compname);
        argv[0] = valp;
        argv[1] = (RDB_object *) compvalp;
        
        ret = RDB_call_update_op(opname, 2, argv, txp);
        free(opname);        
    }

    if (ret != RDB_OK)
        return ret;
    
    ret = _RDB_check_type_constraint(valp, txp);
    if (ret != RDB_OK) {
        /* Destroy illegal value */
        RDB_destroy_obj(valp);
        RDB_init_obj(valp);
    }
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
    if (objp->kind != RDB_OB_TABLE)
        return NULL;
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
        if (valp->var.bin.len > 0) {
            valp->var.bin.datap = malloc(valp->var.bin.len);
            if (valp->var.bin.datap == NULL)
                return RDB_NO_MEMORY;
        }
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
RDB_binary_get(const RDB_object *objp, size_t pos, void **pp, size_t len,
        size_t *alenp)
{
    if (pos > objp->var.bin.len)
        return RDB_NOT_FOUND;
    if (alenp != NULL) {
        if (pos + len > objp->var.bin.len)
            *alenp = objp->var.bin.len - pos;
        else
            *alenp = len;
    }
    *pp = (RDB_byte *)objp->var.bin.datap + pos;
    return RDB_OK;
}

size_t
RDB_binary_length(const RDB_object *objp)
{
    return objp->var.bin.len;
}

RDB_type *
RDB_obj_type(const RDB_object *objp)
{
    if (objp->kind == RDB_OB_TABLE
            && (objp->typ == NULL || objp->typ->kind == RDB_TP_RELATION))
        return RDB_table_type(objp->var.tbp);
    return objp->typ;
}

/* Works only for scalar types */
void
_RDB_set_obj_type(RDB_object *objp, RDB_type *typ)
{
    objp->typ = typ;
    objp->kind = val_kind(typ);
    if (objp->kind == RDB_OB_BIN)
        objp->var.bin.datap = NULL;
}

int
RDB_obj_to_string(RDB_object *dstp, const RDB_object *srcp)
{
    char buf[64];
    int ret;

    if (srcp->typ == &RDB_INTEGER) {
        sprintf(buf, "%d", RDB_obj_int(srcp));
        ret = RDB_string_to_obj(dstp, buf);
        if (ret != RDB_OK)
            return ret;
    } else if (srcp->typ == &RDB_BOOLEAN) {
        ret = RDB_string_to_obj(dstp, RDB_obj_bool(srcp) ? "TRUE" : "FALSE");
        if (ret != RDB_OK)
            return ret;
    } else if (srcp->typ == &RDB_RATIONAL) {
        sprintf(buf, "%g", RDB_obj_rational(srcp));
        ret = RDB_string_to_obj(dstp, buf);
        if (ret != RDB_OK)
            return ret;
    } else if (srcp->typ == &RDB_STRING) {
        ret = RDB_string_to_obj(dstp, RDB_obj_string(srcp));
        if (ret != RDB_OK)
            return ret;
    } else {
        return RDB_INVALID_ARGUMENT;
    }
    return RDB_OK;
}
