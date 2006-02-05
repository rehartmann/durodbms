/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/hashtabit.h>
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
        case RDB_OB_FLOAT:
            return &valp->var.float_val;
        case RDB_OB_DOUBLE:
            return &valp->var.double_val;
        default:
            return valp->var.bin.datap;
    }
} 

int
_RDB_table_ilen(RDB_table *tbp, size_t *lenp, RDB_exec_context *ecp)
{
    int ret;
    size_t len;
    RDB_object tpl;
    RDB_qresult *qrp;

    qrp = _RDB_table_qresult(tbp, ecp, NULL);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);

    *lenp = 0;
    while ((ret = _RDB_next_tuple(qrp, &tpl, ecp, NULL)) == RDB_OK) {
        tpl.typ = tbp->typ->var.basetyp;
        ret = _RDB_obj_ilen(&tpl, &len, ecp);
        if (ret != RDB_OK) {
             RDB_destroy_obj(&tpl, ecp);
            _RDB_drop_qresult(qrp, ecp, NULL);
            return ret;
        }
        *lenp += len;
    }
    RDB_destroy_obj(&tpl, ecp);
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
    } else {
        _RDB_drop_qresult(qrp, ecp, NULL);
        return RDB_ERROR;
    }
    return _RDB_drop_qresult(qrp, ecp, NULL);
}

int
_RDB_obj_ilen(const RDB_object *objp, size_t *lenp, RDB_exec_context *ecp)
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
                ret = _RDB_obj_ilen(attrobjp, &len, ecp);
                if (ret != RDB_OK)
                    return ret;
                *lenp += len;
            }

            return RDB_OK;
        }
        case RDB_OB_TABLE:
            return _RDB_table_ilen(objp->var.tbp, lenp, ecp);
        case RDB_OB_ARRAY:
        {
            RDB_object *elemp;
            int i = 0;

            while ((elemp = RDB_array_get((RDB_object *)objp, (RDB_int) i++,
                    ecp)) != NULL) {
                elemp->typ = RDB_obj_type(objp)->var.basetyp;
                if (elemp->typ->ireplen == RDB_VARIABLE_LEN)
                    *lenp += sizeof (size_t);
                ret = _RDB_obj_ilen(elemp, &len, ecp);
                if (ret != RDB_OK)
                    return ret;
                *lenp += len;
            }
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
                return RDB_ERROR;
            RDB_clear_err(ecp);            
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
            if (typ == &RDB_FLOAT)
                return RDB_OB_FLOAT;
            if (typ == &RDB_DOUBLE)
                return RDB_OB_DOUBLE;
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
len_irep_to_obj(RDB_object *valp, RDB_type *typ, const void *datap,
        RDB_exec_context *ecp)
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

    ret = RDB_irep_to_obj(valp, typ, bp, len, ecp);
    if (ret != RDB_OK)
        return ret;
    return llen + len;
}

static int
irep_to_tuple(RDB_object *tplp, RDB_type *typ, const void *datap,
        RDB_exec_context *ecp)
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
        l = len_irep_to_obj(&obj, typ->var.tuple.attrv[i].typ, bp, ecp);
        if (l < 0) {
            RDB_destroy_obj(&obj, ecp);
            return l;
        }
        bp += l;
        len += l;
        ret = RDB_tuple_set(tplp, typ->var.tuple.attrv[i].name, &obj, ecp);
        RDB_destroy_obj(&obj, ecp);
        if (ret != RDB_OK)
            return ret;        
    }
    return len;
}

int
_RDB_irep_to_table(RDB_table **tbpp, RDB_type *typ, const void *datap, size_t len,
        RDB_exec_context *ecp)
{
    int ret;
    RDB_object tpl;
    RDB_byte *bp = (RDB_byte *)datap;

    if (RDB_type_is_scalar(typ))
        typ = typ->var.scalar.arep;

    *tbpp = RDB_create_table(NULL, RDB_FALSE,
                        typ->var.basetyp->var.tuple.attrc,
                        typ->var.basetyp->var.tuple.attrv,
                        0, NULL, ecp, NULL);
    if (*tbpp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    while (len > 0) {
        int l;
        
        l = irep_to_tuple(&tpl, typ->var.basetyp, bp, ecp);
        if (l < 0) {
            RDB_destroy_obj(&tpl, ecp);
            return l;
        }
        ret = _RDB_insert_real(*tbpp, &tpl, ecp, NULL);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        bp += l;
        len -= l;
    }
    RDB_destroy_obj(&tpl, ecp);

    return RDB_OK;
}

static int
irep_to_array(RDB_object *arrp, RDB_type *typ, const void *datap, size_t len,
        RDB_exec_context *ecp)
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

        l = len_irep_to_obj(&tpl, typ->var.basetyp, bp, ecp);
        if (l < 0) {
            RDB_destroy_obj(&tpl, ecp);
            return l;
        }
        bp += l;
        len -= l;
        arrlen++;
    }

    ret = RDB_set_array_length(arrp, (RDB_int) arrlen, ecp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    
    bp = (RDB_byte *)datap;
    for (i = 0; i < arrlen; i++) {
        int l = len_irep_to_obj(&tpl, typ->var.basetyp, bp, ecp);

        ret = RDB_array_set(arrp, i, &tpl, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        bp += l;
    }
    
    RDB_destroy_obj(&tpl, ecp);

    return RDB_OK;
}

int
RDB_irep_to_obj(RDB_object *valp, RDB_type *typ, const void *datap, size_t len,
        RDB_exec_context *ecp)
{
    int ret;
    enum _RDB_obj_kind kind;

    if (valp->kind != RDB_OB_INITIAL) {
        ret = RDB_destroy_obj(valp, ecp);
        if (ret != RDB_OK)
            return ret;
        RDB_init_obj(valp);
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
        case RDB_OB_FLOAT:
            memcpy(&valp->var.float_val, datap, sizeof (RDB_float));
            break;
        case RDB_OB_DOUBLE:
            memcpy(&valp->var.double_val, datap, sizeof (RDB_double));
            break;
        case RDB_OB_BIN:
            valp->var.bin.len = len;
            valp->var.bin.datap = malloc(len);
            if (valp->var.bin.datap == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            memcpy(valp->var.bin.datap, datap, len);
            break;
        case RDB_OB_TUPLE:
            ret = irep_to_tuple(valp, typ, datap, ecp);
            if (ret > 0)
                ret = RDB_OK;
            return ret;
        case RDB_OB_TABLE:
        {
            RDB_table *tbp;
        
            ret = _RDB_irep_to_table(&tbp, typ, datap, len, ecp);
            if (ret != RDB_OK)
                return ret;
            RDB_table_to_obj(valp, tbp, ecp);
            if (RDB_type_is_scalar(typ))
                valp->typ = typ;
            return RDB_OK;
        }
        case RDB_OB_ARRAY:
            return irep_to_array(valp, typ, datap, len, ecp);
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
    _RDB_obj_to_irep(dstp, (const RDB_object *) srcp, len);
    return dstp;
}

static void *
obj_to_len_irep(void *dstp, const RDB_object *objp, RDB_type *typ,
        RDB_exec_context *ecp)
{
    int ret;
    RDB_byte *bp = dstp;
    size_t len = typ->ireplen;

    if (len == RDB_VARIABLE_LEN) {
        ret = _RDB_obj_ilen(objp, &len, ecp);
        if (ret != RDB_OK)
            return NULL;
        memcpy(bp, &len, sizeof (size_t));
        bp += sizeof (size_t);
    }
    obj_to_irep(bp, objp, len);
    bp += len;

    return bp;
}    

void
_RDB_table_to_irep(void *dstp, RDB_table *tbp, size_t len)
{
    RDB_exec_context ec;
    RDB_object tpl;
    RDB_qresult *qrp;
    int ret;
    size_t l;
    RDB_byte *bp = dstp;

    RDB_init_exec_context(&ec);

    qrp = _RDB_table_qresult(tbp, &ec, NULL);
    if (qrp == NULL) {
        RDB_destroy_exec_context(&ec);
        return;
    }

    RDB_init_obj(&tpl);

    while ((ret = _RDB_next_tuple(qrp, &tpl, &ec, NULL)) == RDB_OK) {
        tpl.typ = tbp->typ->var.basetyp;
        ret = _RDB_obj_ilen(&tpl, &l, &ec);
        if (ret != RDB_OK) {
            RDB_destroy_exec_context(&ec);
            return;
        }
        obj_to_irep(bp, &tpl, l);
        bp += l;
    }
    RDB_destroy_obj(&tpl, &ec);
    if (RDB_obj_type(RDB_get_err(&ec)) != &RDB_NOT_FOUND_ERROR) {
        _RDB_drop_qresult(qrp, &ec, NULL);
        RDB_destroy_exec_context(&ec);
        return;
    }
    _RDB_drop_qresult(qrp, &ec, NULL);
    RDB_destroy_exec_context(&ec);
}

void
_RDB_obj_to_irep(void *dstp, const RDB_object *objp, size_t len)
{
    RDB_exec_context ec;
    RDB_byte *bp = dstp;

    switch (objp->kind) {
        case RDB_OB_BOOL:
            *bp = objp->var.bool_val;
            break;
        case RDB_OB_INT:
            memcpy(bp, &objp->var.int_val, sizeof (RDB_int));
            break;
        case RDB_OB_FLOAT:
            memcpy(bp, &objp->var.float_val, sizeof (RDB_float));
            break;
        case RDB_OB_DOUBLE:
            memcpy(bp, &objp->var.double_val, sizeof (RDB_double));
            break;
        case RDB_OB_TUPLE:
        {
            RDB_type *tpltyp = objp->typ;
            int i;

            RDB_init_exec_context(&ec);

            /* If the type is scalar, use internal rep */
            if (tpltyp->kind == RDB_TP_SCALAR)
                tpltyp = tpltyp->var.scalar.arep;

            for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
                RDB_object *attrp;

                attrp = RDB_tuple_get(objp, tpltyp->var.tuple.attrv[i].name);
                bp = obj_to_len_irep(bp, attrp, tpltyp->var.tuple.attrv[i].typ,
                        &ec);
            }
            RDB_destroy_exec_context(&ec);
            break;
        }
        case RDB_OB_TABLE:
            return _RDB_table_to_irep(dstp, objp->var.tbp, len);
        case RDB_OB_ARRAY:
        {
            RDB_object *elemp;
            int i = 0;

            RDB_init_exec_context(&ec);

            while ((elemp = RDB_array_get((RDB_object *) objp, (RDB_int) i++,
                    &ec)) != NULL) {
                elemp->typ = RDB_obj_type(objp)->var.basetyp;
                bp = obj_to_len_irep(bp, elemp, elemp->typ, &ec);
            }
            if (RDB_obj_type(RDB_get_err(&ec)) != &RDB_NOT_FOUND_ERROR) {
                return;
            }
            RDB_destroy_exec_context(&ec);
            break;
        }
        default:
            memcpy(bp, objp->var.bin.datap, objp->var.bin.len);
    }
}

int
_RDB_obj_to_field(RDB_field *fvp, RDB_object *objp, RDB_exec_context *ecp)
{
    /* Global tables cannot be converted to field values */
    if (objp->kind == RDB_OB_TABLE && objp->var.tbp->is_persistent) {
        RDB_raise_invalid_argument(
                "global table cannot be used as field value", ecp);
        return RDB_ERROR;
    }

    fvp->datap = objp;
    fvp->copyfp = obj_to_irep;
    return _RDB_obj_ilen(objp, &fvp->len, ecp);
}

int
RDB_obj_equals(const RDB_object *val1p, const RDB_object *val2p,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_object retval;
    RDB_object *argv[2];

    argv[0] = (RDB_object *) val1p;
    argv[1] = (RDB_object *) val2p;
    RDB_init_obj(&retval);
    ret = RDB_call_ro_op("=", 2, argv, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return RDB_ERROR;
    }
    *resp = RDB_obj_bool(&retval);
    return RDB_destroy_obj(&retval, ecp);
}

/*
 * Copy RDB_object, but not the type information.
 * If the RDB_object wraps a table, it must be a real table.
 */
int
_RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (dstvalp->kind != RDB_OB_INITIAL && srcvalp->kind != dstvalp->kind) {
        RDB_raise_type_mismatch("source type does not match destination type",
                ecp);
        return RDB_ERROR;
    }

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
        case RDB_OB_FLOAT:
            dstvalp->kind = srcvalp->kind;
            dstvalp->var.float_val = srcvalp->var.float_val;
            break;
        case RDB_OB_DOUBLE:
            dstvalp->kind = srcvalp->kind;
            dstvalp->var.double_val = srcvalp->var.double_val;
            break;
        case RDB_OB_TUPLE:
            return _RDB_copy_tuple(dstvalp, srcvalp, ecp);
        case RDB_OB_ARRAY:
            return _RDB_copy_array(dstvalp, srcvalp, ecp);
        case RDB_OB_TABLE:
            /*
             * If the target is newly initialized, then:
             * If the source is a named table, do not copy the table
             * but make the target refer to the source table.
             * If the source is an unnamed virtual table, duplicate
             * the virtual table
             */
            if (dstvalp->kind == RDB_OB_INITIAL) {
                if (srcvalp->var.tbp->name != NULL) {
                    dstvalp->var.tbp = srcvalp->var.tbp;                    
                    dstvalp->kind = RDB_OB_TABLE;
                    return RDB_OK;
                }
                if (srcvalp->var.tbp->kind != RDB_TB_REAL) {
                    dstvalp->var.tbp = _RDB_dup_vtable(srcvalp->var.tbp, ecp);
                    if (dstvalp->var.tbp == NULL) {
                        return RDB_ERROR;
                    }
                    dstvalp->kind = RDB_OB_TABLE;
                    return RDB_OK;
                }
                dstvalp->var.tbp = RDB_create_table(NULL, RDB_FALSE,
                        srcvalp->var.tbp->typ->var.basetyp->var.tuple.attrc,
                        srcvalp->var.tbp->typ->var.basetyp->var.tuple.attrv,
                        0, NULL, ecp, NULL);
                if (dstvalp->var.tbp == NULL)
                    return RDB_ERROR;
                dstvalp->kind = RDB_OB_TABLE;
            } else {
                if (dstvalp->kind != RDB_OB_TABLE) {
                    RDB_raise_type_mismatch("destination must be table", ecp);
                    return RDB_ERROR;
                }
                ret = _RDB_delete_real(dstvalp->var.tbp, NULL, ecp,
                        dstvalp->var.tbp->is_persistent ? txp : NULL);
                if (ret != RDB_OK)
                    return ret;
            }
            if (dstvalp->var.tbp->is_persistent && txp == NULL) {
                RDB_raise_invalid_tx(ecp);
                return RDB_ERROR;
            }
            return _RDB_move_tuples(dstvalp->var.tbp, srcvalp->var.tbp, ecp,
                    srcvalp->var.tbp->is_persistent
                    || dstvalp->var.tbp->is_persistent ? txp : NULL);
        case RDB_OB_BIN:
            if (dstvalp->kind == RDB_OB_BIN)
                free(dstvalp->var.bin.datap);
            else
                dstvalp->kind = srcvalp->kind;
            dstvalp->var.bin.len = srcvalp->var.bin.len;
            dstvalp->var.bin.datap = malloc(srcvalp->var.bin.len);
            if (dstvalp->var.bin.datap == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            memcpy(dstvalp->var.bin.datap, srcvalp->var.bin.datap,
                    srcvalp->var.bin.len);
            break;
    }
    return RDB_OK;
}

int
RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp,
        RDB_exec_context *ecp)
{
    RDB_ma_copy cpy;

    cpy.dstp = dstvalp;
    cpy.srcp = (RDB_object *) srcvalp;

    return RDB_multi_assign(0, NULL, 0, NULL, 0, NULL, 1, &cpy, ecp, NULL)
            != RDB_ERROR ? RDB_OK : RDB_ERROR ;
} 

void
RDB_init_obj(RDB_object *valp)
{
    valp->kind = RDB_OB_INITIAL;
    valp->typ = NULL;
}

int
RDB_destroy_obj(RDB_object *objp, RDB_exec_context *ecp)
{
    switch (objp->kind) {
        case RDB_OB_INITIAL:
        case RDB_OB_BOOL:
        case RDB_OB_INT:
        case RDB_OB_FLOAT:
        case RDB_OB_DOUBLE:
            break;
        case RDB_OB_BIN:
            if (objp->var.bin.len > 0)
                free(objp->var.bin.datap);
            break;
        case RDB_OB_TUPLE:
        {
            RDB_hashtable_iter it;
            tuple_entry *entryp;

            RDB_init_hashtable_iter(&it, (RDB_hashtable *) &objp->var.tpl_tab);
            while ((entryp = RDB_hashtable_next(&it)) != NULL) {
                RDB_destroy_obj(&entryp->obj, ecp);
                free(entryp->key);
                free(entryp);
            }
            RDB_destroy_hashtable_iter(&it);
            RDB_destroy_hashtable(&objp->var.tpl_tab);
            break;
        }
        case RDB_OB_ARRAY:
        {
            int ret = RDB_OK;
            int ret2 = RDB_OK;

            if (objp->var.arr.elemv != NULL) {
                int i;

                for (i = 0; i < objp->var.arr.elemc; i++)
                    RDB_destroy_obj(&objp->var.arr.elemv[i], ecp);
                free(objp->var.arr.elemv);
            }

            if (objp->var.arr.tbp == NULL)
                return RDB_OK;

            if (objp->var.arr.qrp != NULL) {
                ret = _RDB_drop_qresult(objp->var.arr.qrp, ecp,
                        objp->var.arr.txp);
            }

            /* Delete optimzed table copy (only if it's a virtual table) */
            if (objp->var.arr.tbp->kind != RDB_TB_REAL) {
                ret = RDB_drop_table(objp->var.arr.tbp, ecp, NULL);
                if (ret != RDB_OK)
                    return RDB_ERROR;
            }

            if (objp->var.arr.tplp != NULL) {
                ret2 = RDB_destroy_obj(objp->var.arr.tplp, ecp);
                free(objp->var.arr.tplp);
            }
            if (ret != RDB_OK)
                return RDB_ERROR;
            return ret2;
        }
        case RDB_OB_TABLE:
            if (objp->var.tbp != NULL && objp->var.tbp->name == NULL) {
                return RDB_drop_table(objp->var.tbp, ecp, NULL);
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
RDB_float_to_obj(RDB_object *valp, RDB_float v)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_FLOAT);

    valp->typ = &RDB_FLOAT;
    valp->kind = RDB_OB_FLOAT;
    valp->var.float_val = v;
}

void
RDB_double_to_obj(RDB_object *valp, RDB_double v)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_DOUBLE);

    valp->typ = &RDB_DOUBLE;
    valp->kind = RDB_OB_DOUBLE;
    valp->var.double_val = v;
}

int
RDB_string_to_obj(RDB_object *valp, const char *str, RDB_exec_context *ecp)
{
    void *datap;
    int len = strlen(str) + 1;

    if (valp->kind != RDB_OB_INITIAL && valp->typ != &RDB_STRING) {
        RDB_raise_type_mismatch("not a STRING", ecp);
        return RDB_ERROR;
    }

    if (valp->kind == RDB_OB_INITIAL) {
        datap = malloc(len);
        if (datap == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        valp->typ = &RDB_STRING;
        valp->kind = RDB_OB_BIN;
    } else {
        datap = realloc(valp->var.bin.datap, len);
        if (datap == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }
    valp->var.bin.len = len;
    valp->var.bin.datap = datap;

    strcpy(valp->var.bin.datap, str);
    return RDB_OK;
}

int
RDB_obj_comp(const RDB_object *valp, const char *compname, RDB_object *compvalp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (!RDB_type_is_scalar(valp->typ) || valp->typ->var.scalar.repc == 0) {
        RDB_raise_invalid_argument("component not found", ecp);
        return RDB_ERROR;
    }

    if (valp->typ->var.scalar.sysimpl) {
        if (valp->typ->var.scalar.repv[0].compc == 1) {
            /* Actual rep is type of the only component - check component name */
            if (strcmp(compname, valp->typ->var.scalar.repv[0].compv[0].name)
                    != 0) {
                RDB_raise_invalid_argument("component not found", ecp);
                return RDB_ERROR;
            }            
            RDB_type *comptyp = valp->typ->var.scalar.repv[0].compv[0].typ;

            /* If *compvalp carries a value, it must match the type */
            if (compvalp->kind != RDB_OB_INITIAL
                     && (compvalp->typ == NULL
                         || !RDB_type_equals(compvalp->typ, comptyp))) {
                RDB_raise_type_mismatch("invalid component type", ecp);
                return RDB_ERROR;
            }
            ret = _RDB_copy_obj(compvalp, valp, ecp, NULL);
            if (ret != RDB_OK)
                return ret;
            compvalp->typ = comptyp;
        } else {
            /* Actual rep is tuple */
            RDB_object *elemp = RDB_tuple_get(valp, compname);
            if (elemp == NULL) {
                RDB_raise_invalid_argument("component not found", ecp);
                return RDB_ERROR;
            }
            ret = RDB_copy_obj(compvalp, elemp, ecp);
        }
    } else {
        /* Getter is implemented by user */
        char *opname;
        RDB_object *argv[1];

        opname = malloc(strlen(valp->typ->name) + strlen(compname) + 6);
        if (opname == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }

        strcpy(opname, valp->typ->name);
        strcat(opname, "_get_");
        strcat(opname, compname);
        argv[0] = (RDB_object *) valp;

        ret = RDB_call_ro_op(opname, 1, argv, ecp, txp, compvalp);
        free(opname);
    }
    return ret;
}

int
RDB_obj_set_comp(RDB_object *valp, const char *compname,
        const RDB_object *compvalp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    if (valp->typ->var.scalar.sysimpl) {
        /* Setter is implemented by the system */
        if (valp->typ->var.scalar.repv[0].compc == 1) {
            ret = RDB_destroy_obj(valp, ecp);
            if (ret != RDB_OK)
                return ret;

            ret = _RDB_copy_obj(valp, compvalp, ecp, NULL);
        } else {
            ret = RDB_tuple_set(valp, compname, compvalp, ecp);
        }
    } else {
        /* Setter is implemented by user */
        char *opname;
        RDB_object *argv[2];

        opname = malloc(strlen(valp->typ->name) + strlen(compname) + 6);
        if (opname == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }

        strcpy(opname, valp->typ->name);
        strcat(opname, "_set_");
        strcat(opname, compname);
        argv[0] = valp;
        argv[1] = (RDB_object *) compvalp;
        
        ret = RDB_call_update_op(opname, 2, argv, ecp, txp);
        free(opname);        
    }

    if (ret != RDB_OK)
        return ret;
    
    ret = _RDB_check_type_constraint(valp, ecp, txp);
    if (ret != RDB_OK) {
        /* Destroy illegal value */
        RDB_destroy_obj(valp, ecp);
        RDB_init_obj(valp);
    }
    return ret;
}

void
RDB_table_to_obj(RDB_object *objp, RDB_table *tbp, RDB_exec_context *ecp)
{
    RDB_destroy_obj(objp, ecp);
    objp->typ = tbp->typ;
    objp->kind = RDB_OB_TABLE;
    objp->var.tbp = tbp;
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

RDB_float
RDB_obj_float(const RDB_object *valp)
{
    return valp->var.float_val;
}

RDB_double
RDB_obj_double(const RDB_object *valp)
{
    return valp->var.double_val;
}

char *
RDB_obj_string(const RDB_object *valp)
{
    return valp->var.bin.datap;
}

int
RDB_binary_set(RDB_object *valp, size_t pos, const void *srcp, size_t len,
        RDB_exec_context *ecp)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_BINARY);

    /* If the value is newly initialized, allocate memory */
    if (valp->kind == RDB_OB_INITIAL) {
        valp->var.bin.len = pos + len;
        if (valp->var.bin.len > 0) {
            valp->var.bin.datap = malloc(valp->var.bin.len);
            if (valp->var.bin.datap == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
        }
        valp->typ = &RDB_BINARY;
        valp->kind = RDB_OB_BIN;
    }

    /* If the memory block is to small, reallocate */
    if (valp->var.bin.len < pos + len) {
        void *datap;

        if (valp->var.bin.len > 0)
            datap = realloc(valp->var.bin.datap, pos + len);
        else
            datap = malloc(pos + len);
        if (datap == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        valp->var.bin.datap = datap;
        valp->var.bin.len = pos + len;
    }
    
    /* Copy data */
    if (len > 0)
        memcpy(((RDB_byte *)valp->var.bin.datap) + pos, srcp, len);
    return RDB_OK;
}

int
RDB_binary_get(const RDB_object *objp, size_t pos, size_t len,
        RDB_exec_context *ecp, void **pp, size_t *alenp)
{
    if (pos > objp->var.bin.len) {
        RDB_raise_invalid_argument("index out of range", ecp);
        return RDB_ERROR;
    }
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
RDB_obj_to_string(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *ecp)
{
    char buf[64];
    int ret;

    if (srcp->typ == &RDB_INTEGER) {
        sprintf(buf, "%d", RDB_obj_int(srcp));
        ret = RDB_string_to_obj(dstp, buf, ecp);
        if (ret != RDB_OK)
            return ret;
    } else if (srcp->typ == &RDB_BOOLEAN) {
        ret = RDB_string_to_obj(dstp, RDB_obj_bool(srcp) ? "TRUE" : "FALSE",
                ecp);
        if (ret != RDB_OK)
            return ret;
    } else if (srcp->typ == &RDB_FLOAT) {
        sprintf(buf, "%g", (double) RDB_obj_float(srcp));
        ret = RDB_string_to_obj(dstp, buf, ecp);
        if (ret != RDB_OK)
            return ret;
    } else if (srcp->typ == &RDB_DOUBLE) {
        sprintf(buf, "%g", (double) RDB_obj_double(srcp));
        ret = RDB_string_to_obj(dstp, buf, ecp);
        if (ret != RDB_OK)
            return ret;
    } else if (srcp->typ == &RDB_STRING) {
        ret = RDB_string_to_obj(dstp, RDB_obj_string(srcp), ecp);
        if (ret != RDB_OK)
            return ret;
    } else {
        RDB_raise_invalid_argument("type cannot be converted to STRING", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}
