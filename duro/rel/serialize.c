/* $Id$ */

#include "serialize.h"
#include "typeimpl.h"
#include "internal.h"
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
    int ret;
    size_t len = strlen(str);

    ret = reserve_space(valp, *posp, len + sizeof len);
    if (ret != RDB_OK)
        return ret;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &len, sizeof len);
    *posp += sizeof len;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, str, len);
    *posp += len;
    return RDB_OK;
}

static int
serialize_int(RDB_value *valp, int *posp, RDB_int v)
{
    int ret;

    ret = reserve_space(valp, *posp, sizeof v);
    if (ret != RDB_OK)
        return ret;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &v, sizeof v);
    *posp += sizeof v;
    return RDB_OK;
}

static int
serialize_byte(RDB_value *valp, int *posp, RDB_byte b)
{
    int ret;

    ret = reserve_space(valp, *posp, 1);
    if (ret != RDB_OK)
        return ret;
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
    int ret;
    
    ret = serialize_type(valp, posp, argvalp->typ);
    if (ret != RDB_OK)
        return ret;

    datap = RDB_value_irep((RDB_value *)argvalp, &len);
    ret = reserve_space(valp, *posp, len + sizeof len);
    if (ret != RDB_OK)
        return ret;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, &len, sizeof len);
    *posp += sizeof len;
    memcpy(((RDB_byte *) valp->var.bin.datap) + *posp, datap, len);
    *posp += len;
    return RDB_OK;
}

static int
_RDB_serialize_table(RDB_value *valp, int *posp, RDB_table *tbp);

static int
serialize_expr(RDB_value *valp, int *posp, const RDB_expression *exprp)
{
    int ret = serialize_byte(valp, posp, (RDB_byte)exprp->kind);
    if (ret != RDB_OK)
        return ret;

    switch(exprp->kind) {
        case RDB_CONST:
            return serialize_value(valp, posp, &exprp->var.const_val);
        case RDB_ATTR:
            ret = serialize_type(valp, posp, exprp->var.attr.typ);
            if (ret != RDB_OK)
                return ret;
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
        case RDB_OP_REGMATCH:
            ret = serialize_expr(valp, posp, exprp->var.op.arg1);
            if (ret != RDB_OK)
                return ret;
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
    int ret;

    ret = _RDB_serialize_table(valp, posp, tbp->var.project.tbp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tuptyp->complex.tuple.attrc);
    if (ret != RDB_OK)
        return ret;
    for (i = 0; i < tuptyp->complex.tuple.attrc; i++) {
        ret = serialize_str(valp, posp, tuptyp->complex.tuple.attrv[i].name);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}      

static int
serialize_extend(RDB_value *valp, int *posp, RDB_table *tbp)
{
    int i;
    int ret;

    ret = _RDB_serialize_table(valp, posp, tbp->var.extend.tbp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.extend.attrc);
    if (ret != RDB_OK)
        return ret;
    for (i = 0; i < tbp->var.extend.attrc; i++) {
        ret = serialize_str(valp, posp, tbp->var.extend.attrv[i].name);
        if (ret != RDB_OK)
            return ret;
        ret = serialize_expr(valp, posp, tbp->var.extend.attrv[i].exp);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}      

static int
serialize_summarize(RDB_value *valp, int *posp, RDB_table *tbp)
{
    int i;
    int ret;

    ret = _RDB_serialize_table(valp, posp, tbp->var.summarize.tb1p);
    if (ret != RDB_OK)
        return ret;

    ret = _RDB_serialize_table(valp, posp, tbp->var.summarize.tb1p);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.summarize.addc);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < tbp->var.summarize.addc; i++) {
        ret = serialize_byte(valp, posp,
                             (RDB_byte)tbp->var.summarize.addv[i].op);
        if (ret != RDB_OK)
            return ret;

        ret = serialize_expr(valp, posp, tbp->var.summarize.addv[i].exp);
        if (ret != RDB_OK)
            return ret;

        ret = serialize_str(valp, posp, tbp->var.summarize.addv[i].name);
        if (ret != RDB_OK)
            return ret;
    }
    return RDB_OK;
}

static int
serialize_rename(RDB_value *valp, int *posp, RDB_table *tbp)
{
    int i;
    int ret;

    ret = _RDB_serialize_table(valp, posp, tbp->var.rename.tbp);
    if (ret != RDB_OK)
        return ret;

    ret = serialize_int(valp, posp, tbp->var.rename.renc);
    if (ret != RDB_OK)
        return ret;

    for (i = 0; i < tbp->var.summarize.addc; i++) {
        ret = serialize_str(valp, posp, tbp->var.rename.renv[i].from);
        if (ret != RDB_OK)
            return ret;

        ret = serialize_str(valp, posp, tbp->var.rename.renv[i].to);
        if (ret != RDB_OK)
            return ret;
    }

    return RDB_OK;
}

static int
_RDB_serialize_table(RDB_value *valp, int *posp, RDB_table *tbp)
{
    int ret = serialize_byte(valp, posp, (RDB_byte)tbp->kind);

    if (ret != RDB_OK)
        return ret;

    switch (tbp->kind) {
        case RDB_TB_STORED:
            if (tbp->name == NULL)
                return RDB_ILLEGAL_ARG;
            return serialize_str(valp, posp, tbp->name);
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            ret = _RDB_serialize_table(valp, posp, tbp->var.select.tbp);
            if (ret != RDB_OK)
                return ret;
            return serialize_expr(valp, posp, tbp->var.select.exprp);
        case RDB_TB_UNION:
            ret = _RDB_serialize_table(valp, posp, tbp->var._union.tbp1);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_serialize_table(valp, posp, tbp->var._union.tbp2);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_MINUS:
            ret = _RDB_serialize_table(valp, posp, tbp->var.minus.tbp1);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_serialize_table(valp, posp, tbp->var.minus.tbp2);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_INTERSECT:
            ret = _RDB_serialize_table(valp, posp, tbp->var.intersect.tbp1);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_serialize_table(valp, posp, tbp->var.intersect.tbp2);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_JOIN:
            ret = _RDB_serialize_table(valp, posp, tbp->var.join.tbp1);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_serialize_table(valp, posp, tbp->var.join.tbp2);
            if (ret != RDB_OK)
                return ret;
            return RDB_OK;
        case RDB_TB_EXTEND:
            return serialize_extend(valp, posp, tbp);
        case RDB_TB_PROJECT:
            return serialize_project(valp, posp, tbp);
        case RDB_TB_SUMMARIZE:
            return serialize_summarize(valp, posp, tbp);
        case RDB_TB_RENAME:
            return serialize_rename(valp, posp, tbp);
    }
    abort();
}

enum {
    RDB_BUF_INITLEN = 256
};

int
_RDB_vtable_to_value(RDB_table *tbp, RDB_value *valp)
{
    int pos;
    int ret;

    RDB_destroy_value(valp);
    valp->typ = &RDB_BINARY;
    valp->var.bin.len = RDB_BUF_INITLEN;
    valp->var.bin.datap = malloc(RDB_BUF_INITLEN);
    if (valp->var.bin.datap == NULL) {
        return RDB_NO_MEMORY;
    }
    pos = 0;
    ret = _RDB_serialize_table(valp, &pos, tbp);
    if (ret != RDB_OK)
        return ret;

    valp->var.bin.len = pos; /* Only store actual length */
    return RDB_OK;
}

int
_RDB_expr_to_value(const RDB_expression *exp, RDB_value *valp)
{
    int pos;
    int ret;

    RDB_destroy_value(valp);
    valp->typ = &RDB_BINARY;
    valp->var.bin.len = RDB_BUF_INITLEN;
    valp->var.bin.datap = malloc(RDB_BUF_INITLEN);
    if (valp->var.bin.datap == NULL) {
        return RDB_NO_MEMORY;
    }
    pos = 0;
    ret = serialize_expr(valp, &pos, exp);
    if (ret != RDB_OK)
        return ret;

    valp->var.bin.len = pos; /* Only store actual length */
    return RDB_OK;
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
    int ret;

    ret = deserialize_str(valp, posp, &namp);
    if (ret != RDB_OK)
        return ret;
    ret = RDB_get_type(dbp, namp, typp);
    free(namp);
    return ret;
}

static int
deserialize_value(RDB_value *valp, int *posp, RDB_database *dbp,
                  RDB_value *argvalp)
{
    RDB_type *typ;
    size_t len;
    int ret;

    ret = deserialize_type(valp, posp, dbp, &typ);
    if (ret != RDB_OK)
        return ret;

    if (*posp + sizeof len > valp->var.bin.len)
        return RDB_INTERNAL;
    memcpy (&len, ((RDB_byte *)valp->var.bin.datap) + *posp, sizeof len);
    *posp += sizeof len;

    if (*posp + len > valp->var.bin.len)
        return RDB_INTERNAL;
    ret = RDB_irep_to_value(argvalp, typ,
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
    int ret;

    ret = deserialize_byte(valp, posp, &b);
    if (ret != RDB_OK)
        return ret;
    switch (b) {
        case RDB_CONST:
            {
               RDB_value val;
               RDB_expression *exprp;

               RDB_init_value(&val);
               ret = deserialize_value(valp, posp, dbp, &val);
               if (ret != RDB_OK) {
                   RDB_destroy_value(&val);
                   return ret;
               }
               *exprpp = RDB_value_const(&val);
               RDB_destroy_value(&val);
               if (exprp == NULL)
                   return RDB_NO_MEMORY;
            }
            break;
        case RDB_ATTR:
            {
                RDB_type *typ;
                char *attrnamp;
            
                ret = deserialize_type(valp, posp, dbp, &typ);
                if (ret != RDB_OK)
                    return ret;

                ret = deserialize_str(valp, posp, &attrnamp);
                if (ret != RDB_OK)
                    return ret;

                *exprpp = RDB_expr_attr(attrnamp, typ);
                free(attrnamp);
                if (*exprpp == NULL)
                    return RDB_NO_MEMORY;
            }
            break;
        case RDB_OP_NOT:
            ret = deserialize_expr(valp, posp, dbp, &expr1p);
            if (ret != RDB_OK)
                return ret;
            *exprpp = RDB_not(expr1p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_REL_IS_EMPTY:
            ret = deserialize_expr(valp, posp, dbp, &expr1p);
            if (ret != RDB_OK)
                return ret;
            *exprpp = RDB_rel_is_empty(expr1p);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_OP_EQ:
        case RDB_OP_NEQ:
        case RDB_OP_LT:
        case RDB_OP_GT:
        case RDB_OP_LET:
        case RDB_OP_GET:
        case RDB_OP_AND:
        case RDB_OP_OR:
        case RDB_OP_ADD:
        case RDB_OP_REGMATCH:
            ret = deserialize_expr(valp, posp, dbp, &expr1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_expr(valp, posp, dbp, &expr2p);
            if (ret != RDB_OK)
                return ret;
            *exprpp = _RDB_create_binexpr(expr1p, expr2p, (enum _RDB_expr_kind)b);
            if (*exprpp == NULL)
                return RDB_NO_MEMORY;
            break;
        case RDB_TABLE:
            {
                RDB_table *tbp;

                ret = _RDB_deserialize_table(valp, posp, dbp, &tbp);
                if (ret != RDB_OK)
                    return ret;
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
    int ret;

    ret = _RDB_deserialize_table(valp, posp, dbp, &tbp);
    if (ret != RDB_OK)
        return ret;
    ret = deserialize_int(valp, posp, &ac);
    av = malloc(ac * sizeof(char *));
    if (av == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < ac; i++)
       av[i] = NULL;
    for (i = 0; i < ac; i++) {
        ret = deserialize_str(valp, posp, av + i);
        if (ret != RDB_OK)
            goto error;
    }
    ret = RDB_project(tbp, ac, av, tbpp);
    if (ret != RDB_OK)
        goto error;
    RDB_free_strvec(ac, av);
    return RDB_OK;
error:
    RDB_free_strvec(ac, av);
    return ret;
}

int
deserialize_extend(RDB_value *valp, int *posp, RDB_database *dbp,
                    RDB_table **tbpp)
{
    RDB_table *tbp;
    RDB_int ac;
    RDB_virtual_attr *av;
    int i;
    int ret;

    ret = _RDB_deserialize_table(valp, posp, dbp, &tbp);
    if (ret != RDB_OK)
        return ret;
    ret = deserialize_int(valp, posp, &ac);
    av = malloc(ac * sizeof(RDB_virtual_attr));
    if (av == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < ac; i++) {
        av[i].name = NULL;
    }
    for (i = 0; i < ac; i++) {
        ret = deserialize_str(valp, posp, &av[i].name);
        if (ret != RDB_OK)
            goto error;
        ret = deserialize_expr(valp, posp, dbp, &av[i].exp);
        if (ret != RDB_OK)
            goto error;
    }
    ret = RDB_extend(tbp, ac, av, tbpp);
    if (ret != RDB_OK)
        goto error;
    for (i = 0; i < ac; i++) {
        free(av[i].name);
    }
    free(av);
    return RDB_OK;
error:
    for (i = 0; i < ac; i++) {
        free(av[i].name);
    }
    free(av);
    return ret;
}

int
deserialize_summarize(RDB_value *valp, int *posp, RDB_database *dbp,
                    RDB_table **tbpp)
{
    return RDB_NOT_SUPPORTED;
}

int
deserialize_rename(RDB_value *valp, int *posp, RDB_database *dbp,
                    RDB_table **tbpp)
{
    return RDB_NOT_SUPPORTED;
}

int
_RDB_deserialize_table(RDB_value *valp, int *posp, RDB_database *dbp,
                       RDB_table **tbpp)
{
    int ret;
    RDB_byte b;
    char *namp;
    RDB_table *tb1p, *tb2p;
    RDB_expression *exprp;

    ret = deserialize_byte(valp, posp, &b);
    if (ret != RDB_OK)
        return ret;
    switch ((enum _RDB_tb_kind) b) {
        case RDB_TB_STORED:
            ret = deserialize_str(valp, posp, &namp);
            if (ret != RDB_OK)
                return ret;
            ret = RDB_get_table(dbp, namp, tbpp);
            free(namp);
            return ret;
        case RDB_TB_SELECT:
        case RDB_TB_SELECT_PINDEX:
            ret = _RDB_deserialize_table(valp, posp, dbp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = deserialize_expr(valp, posp, dbp, &exprp);
            if (ret != RDB_OK)
                goto error;
            return RDB_select(tb1p, exprp, tbpp);
            break;
        case RDB_TB_UNION:
            ret = _RDB_deserialize_table(valp, posp, dbp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_deserialize_table(valp, posp, dbp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_union(tb1p, tb2p, tbpp);
        case RDB_TB_MINUS:
            ret = _RDB_deserialize_table(valp, posp, dbp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_deserialize_table(valp, posp, dbp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_minus(tb1p, tb2p, tbpp);
        case RDB_TB_INTERSECT:
            ret = _RDB_deserialize_table(valp, posp, dbp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_deserialize_table(valp, posp, dbp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_union(tb1p, tb2p, tbpp);
        case RDB_TB_JOIN:
            ret = _RDB_deserialize_table(valp, posp, dbp, &tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = _RDB_deserialize_table(valp, posp, dbp, &tb2p);
            if (ret != RDB_OK)
                return ret;
            return RDB_join(tb1p, tb2p, tbpp);
        case RDB_TB_EXTEND:
            return deserialize_extend(valp, posp, dbp, tbpp);
        case RDB_TB_PROJECT:
            return deserialize_project(valp, posp, dbp, tbpp);
        case RDB_TB_SUMMARIZE:
            return deserialize_summarize(valp, posp, dbp, tbpp);
        case RDB_TB_RENAME:
            return deserialize_rename(valp, posp, dbp, tbpp);
    }
    abort();
error:
    return ret;
}
