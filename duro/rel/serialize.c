/* $Id$ */

#include "serialize.h"
#include "typeimpl.h"
#include <gen/strfns.h>
#include <string.h>

/*
 * Functions for serializing/deserializing - needed for
 * persistent virtual tables (views)
 */

static int
reserve_space(RDB_value *valp, int pos, size_t n)
{
    while (valp->var.bin.len < pos + n) {
        int newlen = valp->var.bin.len * 2;
        void *newdatap = realloc(valp->var.bin.datap, newlen);
        
        if (newdatap == NULL)
            return RDB_NO_MEMORY;
        valp->var.bin.len = newlen;
        valp->var.bin.datap = newdatap;
    }

    return RDB_OK;
}

static int
serialize_str(RDB_value *valp, int *posp, const char *str)
{
    int res;
    size_t len = strlen(str);

    res = reserve_space(valp, *posp, len + sizeof len);
    if (res != RDB_OK)
        return res;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &len, sizeof len);
    *posp += sizeof len;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, str, len);
    *posp += len;
    return RDB_OK;
}

static int
serialize_int(RDB_value *valp, int *posp, RDB_int v)
{
    int res;

    res = reserve_space(valp, *posp, sizeof v);
    if (res != RDB_OK)
        return res;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &v, sizeof v);
    *posp += sizeof v;
    return RDB_OK;
}

static int
serialize_byte(RDB_value *valp, int *posp, RDB_byte b)
{
    int res;

    res = reserve_space(valp, *posp, 1);
    if (res != RDB_OK)
        return res;
    ((RDB_byte *) valp->var.bin.datap)[(*posp)++] = b;
    return RDB_OK;
}

static int
serialize_type(RDB_value *valp, int *posp, const RDB_type *typ)
{
    if (typ->name == NULL)
        return RDB_NOT_SUPPORTED;
    return serialize_str(valp, posp, typ->name);
}

static int
serialize_value(RDB_value *valp, int *posp, const RDB_value *argvalp)
{
    size_t len;
    void *datap;
    int res;
    
    res = serialize_type(valp, posp, argvalp->typ);
    if (res != RDB_OK)
        return res;

    datap = RDB_value_irep((RDB_value *)argvalp, &len);
    res = reserve_space(valp, *posp, len + sizeof len);
    if (res != RDB_OK)
        return res;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &len, sizeof len);
    *posp += sizeof len;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, datap, len);
    *posp += len;
    return RDB_OK;
}

static int
serialize_expr(RDB_value *valp, int *posp, const RDB_expression *exprp)
{
    int res = serialize_byte(valp, posp, (RDB_byte)exprp->kind);
    if (res != RDB_OK)
        return res;

    switch(exprp->kind) {
        case RDB_CONST:
            return serialize_value(valp, posp, &exprp->var.const_val);
        case RDB_ATTR:
            res = serialize_type(valp, posp, exprp->var.attr.typ);
            if (res != RDB_OK)
                return res;
            return serialize_str(valp, posp, exprp->var.attr.name);
        case RDB_OP_NOT:
        case RDB_OP_REL_IS_EMPTY:
            return serialize_expr(valp, posp, exprp->var.op.arg1);
        case RDB_OP_EQ:
        case RDB_OP_NEQ:
        case RDB_OP_LT:
        case RDB_OP_GT:
        case RDB_OP_LET:
        case RDB_OP_GET:
        case RDB_OP_AND:
        case RDB_OP_OR:
        case RDB_OP_ADD:
            res = serialize_expr(valp, posp, exprp->var.op.arg1);
            if (res != RDB_OK)
                return res;
            return serialize_expr(valp, posp, exprp->var.op.arg2);
        case RDB_TABLE:
            return _RDB_serialize_table(valp, posp, exprp->var.tbp);
    }
    abort();
}

static int
serialize_project(RDB_value *valp, int *posp, RDB_table *tbp)
{
    RDB_type *tuptyp = tbp->typ->complex.basetyp;
    int i;
    int res;

    res = _RDB_serialize_table(valp, posp, tbp->var.project.tbp);
    if (res != RDB_OK)
        return res;

    res = serialize_int(valp, posp, tuptyp->complex.tuple.attrc);
    if (res != RDB_OK)
        return res;
    for (i = 0; i < tuptyp->complex.tuple.attrc; i++) {
        res = serialize_str(valp, posp, tuptyp->complex.tuple.attrv[i].name);
        if (res != RDB_OK)
            return res;
    }
    return RDB_OK;
}      

static int
serialize_extend(RDB_value *valp, int *posp, RDB_table *tbp)
{
    int i;
    int res;

    res = _RDB_serialize_table(valp, posp, tbp->var.extend.tbp);
    if (res != RDB_OK)
        return res;

    res = serialize_int(valp, posp, tbp->var.extend.attrc);
    if (res != RDB_OK)
        return res;
    for (i = 0; i < tbp->var.extend.attrc; i++) {
        res = serialize_str(valp, posp, tbp->var.extend.attrv[i].name);
        if (res != RDB_OK)
            return res;
        res = serialize_expr(valp, posp, tbp->var.extend.attrv[i].value);
        if (res != RDB_OK)
            return res;
    }
    return RDB_OK;
}      

int
_RDB_serialize_table(RDB_value *valp, int *posp, RDB_table *tbp)
{
    int res = serialize_byte(valp, posp, (RDB_byte)tbp->kind);

    if (res != RDB_OK)
        return res;

    switch (tbp->kind) {
        case RDB_TB_STORED:
            if (tbp->name == NULL)
                return RDB_ILLEGAL_ARG;
            return serialize_str(valp, posp, tbp->name);
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            res = _RDB_serialize_table(valp, posp, tbp->var.select.tbp);
            if (res != RDB_OK)
                return res;
            return serialize_expr(valp, posp, tbp->var.select.exprp);
        case RDB_TB_UNION:
            res = _RDB_serialize_table(valp, posp, tbp->var._union.tbp1);
            if (res != RDB_OK)
                return res;
            res = _RDB_serialize_table(valp, posp, tbp->var._union.tbp2);
            if (res != RDB_OK)
                return res;
            return RDB_OK;
        case RDB_TB_MINUS:
            res = _RDB_serialize_table(valp, posp, tbp->var.minus.tbp1);
            if (res != RDB_OK)
                return res;
            res = _RDB_serialize_table(valp, posp, tbp->var.minus.tbp2);
            if (res != RDB_OK)
                return res;
            return RDB_OK;
        case RDB_TB_INTERSECT:
            res = _RDB_serialize_table(valp, posp, tbp->var.intersect.tbp1);
            if (res != RDB_OK)
                return res;
            res = _RDB_serialize_table(valp, posp, tbp->var.intersect.tbp2);
            if (res != RDB_OK)
                return res;
            return RDB_OK;
        case RDB_TB_JOIN:
            res = _RDB_serialize_table(valp, posp, tbp->var.join.tbp1);
            if (res != RDB_OK)
                return res;
            res = _RDB_serialize_table(valp, posp, tbp->var.join.tbp2);
            if (res != RDB_OK)
                return res;
            return RDB_OK;
        case RDB_TB_EXTEND:
            return serialize_extend(valp, posp, tbp);
        case RDB_TB_PROJECT:
            return serialize_project(valp, posp, tbp);
    }
    abort();
}

static int
deserialize_str(RDB_value *valp, int *posp, char **strp)
{
    size_t len;
    
    if (*posp + sizeof (len) > valp->var.bin.len)
        return RDB_INTERNAL;
    memcpy (&len, ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof len);
    *posp += sizeof len;
    if (*posp + len > valp->var.bin.len)
        return RDB_INTERNAL;
    *strp = malloc(len + 1);
    if (*strp == NULL)
        return RDB_NO_MEMORY;
    memcpy(*strp, ((RDB_byte *)valp->var.bin.datap) + *posp, len);
    (*strp)[len] = '\0';
    *posp += len;
    return RDB_OK;
}

static int
deserialize_int(RDB_value *valp, int *posp, RDB_int *vp)
{
    if (*posp + sizeof (RDB_int) > valp->var.bin.len)
        return RDB_INTERNAL;
    memcpy (vp, ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof (RDB_int));
    *posp += sizeof (RDB_int);
    return RDB_OK;
}

static int
deserialize_byte(RDB_value *valp, int *posp, RDB_byte *bp)
{
    if (*posp + 1 > valp->var.bin.len)
        return RDB_INTERNAL;

    *bp = ((RDB_byte *)valp->var.bin.datap)[(*posp)++];
    return RDB_OK;
}

static int
deserialize_type(RDB_value *valp, int *posp, RDB_database *dbp,
                 RDB_type **typp)
{
    char *namp;
    int res;

    res = deserialize_str(valp, posp, &namp);
    if (res != RDB_OK)
        return res;
    res = RDB_get_type(dbp, namp, typp);
    free(namp);
    return res;
}

static int
deserialize_value(RDB_value *valp, int *posp, RDB_database *dbp,
                      RDB_value *argvalp)
{
    RDB_type *typ;
    size_t len;
    int res;

    res = deserialize_type(valp, posp, dbp, &typ);
    if (res != RDB_OK)
        return res;

    if (*posp + sizeof len > valp->var.bin.len)
        return RDB_INTERNAL;
    memcpy (&len, ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof len);
    *posp += sizeof len;

    if (*posp + len > valp->var.bin.len)
        return RDB_INTERNAL;
    res = RDB_irep_to_value(argvalp, typ,
            ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof len);
    *posp += len;

    return RDB_OK;
}

static int
deserialize_expr(RDB_value *valp, int *posp, RDB_database *dbp,
                 RDB_expression **exprpp)
{
    RDB_byte b;
    RDB_expression *expr1p, *expr2p;
    int res;

    res = deserialize_byte(valp, posp, &b);
    if (res != RDB_OK)
        return res;
    switch (b) {
        case RDB_CONST:
            {
               RDB_value val;
               RDB_expression *exprp;

               RDB_init_value(&val);
               res = deserialize_value(valp, posp, dbp, &val);
               if (res != RDB_OK) {
                   RDB_deinit_value(&val);
                   return res;
               }
               *exprpp = RDB_value_const(&val);
               RDB_deinit_value(&val);
               if (exprp == NULL)
                   return RDB_NO_MEMORY;
            }
            break;
        case RDB_ATTR:
            {
                RDB_type *typ;
                char *attrnamp;
            
                res = deserialize_type(valp, posp, dbp, &typ);
                if (res != RDB_OK)
                    return res;

                res = deserialize_str(valp, posp, &attrnamp);
                if (res != RDB_OK)
                    return res;

                *exprpp = RDB_expr_attr(attrnamp, typ);
                free(attrnamp);
                if (*exprpp == NULL)
                    return RDB_NO_MEMORY;
            }
            break;
        case RDB_OP_NOT:
            res = deserialize_expr(valp, posp, dbp, &expr1p);
            if (res != RDB_OK)
                return res;
            *exprpp = RDB_not(expr1p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_REL_IS_EMPTY:
            res = deserialize_expr(valp, posp, dbp, &expr1p);
            if (res != RDB_OK)
                return res;
            *exprpp = RDB_rel_is_empty(expr1p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_EQ:
            res = deserialize_expr(valp, posp, dbp, &expr1p);
            if (res != RDB_OK)
                return res;
            res = deserialize_expr(valp, posp, dbp, &expr2p);
            if (res != RDB_OK)
                return res;
            *exprpp = RDB_eq(expr1p, expr2p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_NEQ:
            res = deserialize_expr(valp, posp, dbp, &expr1p);
            if (res != RDB_OK)
                return res;
            res = deserialize_expr(valp, posp, dbp, &expr2p);
            if (res != RDB_OK)
                return res;
            *exprpp = RDB_neq(expr1p, expr2p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_LT:
            res = deserialize_expr(valp, posp, dbp, &expr1p);
            if (res != RDB_OK)
                return res;
            res = deserialize_expr(valp, posp, dbp, &expr2p);
            if (res != RDB_OK)
                return res;
            *exprpp = RDB_lt(expr1p, expr2p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_GT:
            res = deserialize_expr(valp, posp, dbp, &expr1p);
            if (res != RDB_OK)
                return res;
            res = deserialize_expr(valp, posp, dbp, &expr2p);
            if (res != RDB_OK)
                return res;
            *exprpp = RDB_gt(expr1p, expr2p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_LET:
            res = deserialize_expr(valp, posp, dbp, &expr1p);
            if (res != RDB_OK)
                return res;
            res = deserialize_expr(valp, posp, dbp, &expr2p);
            if (res != RDB_OK)
                return res;
            *exprpp = RDB_let(expr1p, expr2p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_GET:
            res = deserialize_expr(valp, posp, dbp, &expr1p);
            if (res != RDB_OK)
                return res;
            res = deserialize_expr(valp, posp, dbp, &expr2p);
            if (res != RDB_OK)
                return res;
            *exprpp = RDB_get(expr1p, expr2p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_AND:
            res = deserialize_expr(valp, posp, dbp, &expr1p);
            if (res != RDB_OK)
                return res;
            res = deserialize_expr(valp, posp, dbp, &expr2p);
            if (res != RDB_OK)
                return res;
            *exprpp = RDB_get(expr1p, expr2p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_OR:
            res = deserialize_expr(valp, posp, dbp, &expr1p);
            if (res != RDB_OK)
                return res;
            res = deserialize_expr(valp, posp, dbp, &expr2p);
            if (res != RDB_OK)
                return res;
            *exprpp = RDB_or(expr1p, expr2p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_ADD:
            res = deserialize_expr(valp, posp, dbp, &expr1p);
            if (res != RDB_OK)
                return res;
            res = deserialize_expr(valp, posp, dbp, &expr2p);
            if (res != RDB_OK)
                return res;
            *exprpp = RDB_add(expr1p, expr2p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_TABLE:
            {
                RDB_table *tbp;

                res = _RDB_deserialize_table(valp, posp, dbp, &tbp);
                if (res != RDB_OK)
                    return res;
                *exprpp = RDB_rel_table(tbp);
                if (exprpp == NULL)
                    return RDB_NO_MEMORY;
            }
            break;
    }
    return RDB_OK;
}

int
deserialize_project(RDB_value *valp, int *posp, RDB_database *dbp,
                    RDB_table **tbpp)
{
    RDB_table *tbp;
    RDB_int ac;
    char **av;
    int i;
    int res;

    res = _RDB_deserialize_table(valp, posp, dbp, &tbp);
    if (res != RDB_OK)
        return res;
    res = deserialize_int(valp, posp, &ac);
    av = malloc(ac * sizeof(char *));
    if (av == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < ac; i++)
       av[i] = NULL;
    for (i = 0; i < ac; i++) {
        res = deserialize_str(valp, posp, av + i);
        if (res != RDB_OK)
            goto error;
    }
    res = RDB_project(tbp, ac, av, tbpp);
    if (res != RDB_OK)
        goto error;
    RDB_free_strvec(ac, av);
    return RDB_OK;
error:
    RDB_free_strvec(ac, av);
    return res;
}

int
deserialize_extend(RDB_value *valp, int *posp, RDB_database *dbp,
                    RDB_table **tbpp)
{
    RDB_table *tbp;
    RDB_int ac;
    RDB_virtual_attr *av;
    int i;
    int res;

    res = _RDB_deserialize_table(valp, posp, dbp, &tbp);
    if (res != RDB_OK)
        return res;
    res = deserialize_int(valp, posp, &ac);
    av = malloc(ac * sizeof(RDB_virtual_attr));
    if (av == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < ac; i++) {
        av[i].name = NULL;
        av[i].value = NULL;
    }
    for (i = 0; i < ac; i++) {
        res = deserialize_str(valp, posp, &av[i].name);
        if (res != RDB_OK)
            goto error;
        res = deserialize_expr(valp, posp, dbp, &av[i].value);
        if (res != RDB_OK)
            goto error;
    }
    res = RDB_extend(tbp, ac, av, tbpp);
    if (res != RDB_OK)
        goto error;
    for (i = 0; i < ac; i++) {
        free(av[i].name);
        if (av[i].value != NULL)
            RDB_drop_expr(av[i].value);
    }
    free(av);
    return RDB_OK;
error:
    for (i = 0; i < ac; i++) {
        free(av[i].name);
        if (av[i].value != NULL)
            RDB_drop_expr(av[i].value);
    }
    free(av);
    return res;
}

int
_RDB_deserialize_table(RDB_value *valp, int *posp, RDB_database *dbp,
                       RDB_table **tbpp)
{
    int res;
    RDB_byte b;
    char *namp;
    RDB_table *tb1p, *tb2p;
    RDB_expression *exprp;

    res = deserialize_byte(valp, posp, &b);
    if (res != RDB_OK)
        return res;
    switch (b) {
        case RDB_TB_STORED:
            res = deserialize_str(valp, posp, &namp);
            if (res != RDB_OK)
                return res;
            res = RDB_get_table(dbp, namp, tbpp);
            free(namp);
            return res;
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            res = _RDB_deserialize_table(valp, posp, dbp, &tb1p);
            if (res != RDB_OK)
                return res;
            res = deserialize_expr(valp, posp, dbp, &exprp);
            if (res != RDB_OK)
                goto error;
            return RDB_select(tb1p, exprp, tbpp);
            break;
        case RDB_TB_UNION:
            res = _RDB_deserialize_table(valp, posp, dbp, &tb1p);
            if (res != RDB_OK)
                return res;
            res = _RDB_deserialize_table(valp, posp, dbp, &tb2p);
            if (res != RDB_OK)
                return res;
            return RDB_union(tb1p, tb2p, tbpp);
        case RDB_TB_MINUS:
            res = _RDB_deserialize_table(valp, posp, dbp, &tb1p);
            if (res != RDB_OK)
                return res;
            res = _RDB_deserialize_table(valp, posp, dbp, &tb2p);
            if (res != RDB_OK)
                return res;
            return RDB_minus(tb1p, tb2p, tbpp);
        case RDB_TB_INTERSECT:
            res = _RDB_deserialize_table(valp, posp, dbp, &tb1p);
            if (res != RDB_OK)
                return res;
            res = _RDB_deserialize_table(valp, posp, dbp, &tb2p);
            if (res != RDB_OK)
                return res;
            return RDB_union(tb1p, tb2p, tbpp);
        case RDB_TB_JOIN:
            res = _RDB_deserialize_table(valp, posp, dbp, &tb1p);
            if (res != RDB_OK)
                return res;
            res = _RDB_deserialize_table(valp, posp, dbp, &tb2p);
            if (res != RDB_OK)
                return res;
            return RDB_join(tb1p, tb2p, tbpp);
        case RDB_TB_EXTEND:
            return deserialize_extend(valp, posp, dbp, tbpp);
        case RDB_TB_PROJECT:
            return deserialize_project(valp, posp, dbp, tbpp);
    }
    abort();
error:
    return res;
}
