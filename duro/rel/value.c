/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/errors.h>
#include <string.h>

void *
RDB_value_irep(RDB_value *valp, size_t *lenp)
{
    RDB_type *rep = valp->typ;

    if (!RDB_type_is_builtin(rep))
        rep = rep->arep;

    if (lenp != NULL) {
        *lenp = rep->ireplen;
        if (*lenp == RDB_VARIABLE_LEN)
            *lenp = valp->var.bin.len;
    }

    if (rep == &RDB_BOOLEAN) {
        return &valp->var.bool_val;
    } else if (rep == &RDB_INTEGER) {
        return &valp->var.int_val;
    } else if (rep == &RDB_RATIONAL) {
        return &valp->var.rational_val;
    } else {
        return valp->var.bin.datap;
    }
} 

int
RDB_irep_to_value(RDB_value *valp, RDB_type *typ, void *datap, size_t len)
{
    const RDB_type *rep = typ;

    if (!RDB_type_is_builtin(typ))
        rep = typ->arep;

    if (valp->typ != NULL)
        RDB_destroy_value(valp);

    valp->typ = typ;
    if (typ == &RDB_BOOLEAN) {
        valp->var.bool_val = *(RDB_bool *) datap;
    } else if (typ == &RDB_INTEGER) {
        memcpy(&valp->var.int_val, datap, sizeof (RDB_int));
    } else if (typ == &RDB_RATIONAL) {
        memcpy(&valp->var.rational_val, datap, sizeof (RDB_rational));
    } else {
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
    RDB_type *rep = valp1->typ;

    if (!RDB_type_is_builtin(rep))
        rep = rep->arep;

    if (rep == &RDB_BOOLEAN) {
        return (RDB_bool) valp1->var.bool_val == valp2->var.bool_val;
    } else if (rep == &RDB_INTEGER) {
        return (RDB_bool) (valp1->var.int_val == valp2->var.int_val);
    } else if (rep == &RDB_RATIONAL) {
        return (RDB_bool) (valp1->var.rational_val == valp2->var.rational_val);
    } else {
        if (valp1->var.bin.len != valp2->var.bin.len)
            return RDB_FALSE;
        return (RDB_bool) (memcmp(valp1->var.bin.datap, valp2->var.bin.datap,
                valp1->var.bin.len) == 0);
    }
} 

static int
copy_value(RDB_value *dstvalp, const RDB_value *srcvalp)
{
    RDB_type *rep = srcvalp->typ;

    if (!RDB_type_is_builtin(rep))
        rep = rep->arep;

    if (rep == &RDB_BOOLEAN) {
        dstvalp->var.bool_val = srcvalp->var.bool_val;
    } else if (rep == &RDB_INTEGER) {
        dstvalp->var.int_val = srcvalp->var.int_val;
    } else if (rep == &RDB_RATIONAL) {
        dstvalp->var.rational_val = srcvalp->var.rational_val;
    } else {
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
    RDB_type *rep = valp->typ;

    if (rep == NULL)
        return;

    if (!RDB_type_is_builtin(rep))
        rep = rep->arep;

    if (rep->ireplen == RDB_VARIABLE_LEN) {
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
                   RDB_value *compvalp)
{
    RDB_icomp *comp = _RDB_get_icomp(valp->typ, compname);

    if (comp->setterp != NULL) {
        return (*(comp->getterp))(valp, compvalp, valp->typ, compname);
    } else {
        RDB_destroy_value(compvalp);

        compvalp->typ = valp->typ->var.scalar.repv[0].compv[0].typ;
        return copy_value(compvalp, valp);   
    }
}

int
RDB_value_set_comp(RDB_value *valp, const char *compname,
                   const RDB_value *compvalp)
{
    RDB_icomp *comp = _RDB_get_icomp(valp->typ, compname);

    if (comp->setterp != NULL) {
        return (*(comp->setterp))(valp, compvalp, valp->typ, compname);
    } else {
        RDB_destroy_value(valp);

        return copy_value(valp, compvalp);
    }
}

static RDB_bool
check_constraint(RDB_value *valp) {
    int i, j;
    int ret;

    /* Check constraint for each possrep */
    for (i = 0; i < valp->typ->var.scalar.repc; i++) {
        RDB_tuple tpl;
        RDB_bool result;

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
                    return RDB_FALSE; /* !! */
                }
                ret = RDB_tuple_set(&tpl, compname, &comp);
                RDB_destroy_value(&comp);
                if (ret != RDB_OK) {
                    RDB_destroy_tuple(&tpl);
                    return RDB_FALSE; /* !! */
                }
            }
            RDB_evaluate_bool(valp->typ->var.scalar.repv[i].constraintp,
                    &tpl, NULL, &result);
            RDB_destroy_tuple(&tpl);
            if (!result)
                return RDB_FALSE;
        }
    }
    return RDB_TRUE;
}

int
RDB_select_value(RDB_value *valp, RDB_type *typ, const char *repname,
              RDB_value **compv)
{
    RDB_ipossrep *prp;
    int i;
    int ret;

    RDB_destroy_value(valp);

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
    for (i = 0; i < typ->var.scalar.repc
            && strcmp(typ->var.scalar.repv[i].name, repname) != 0;
            i++);
    if (i >= typ->var.scalar.repc)
        return RDB_INVALID_ARGUMENT;
    prp = &typ->var.scalar.repv[i];

    valp->typ = typ;

    if (prp->selectorp != NULL)
        ret = (prp->selectorp)(valp, compv, typ, repname);
    else
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
