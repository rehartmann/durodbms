/* $Id$ */

#include "rdb.h"
#include "internal.h"
#include <string.h>
#include <malloc.h>

#define RDB_TUPLE_CAPACITY 37

/*
 * A tuple is implemented using a static hash table, taking advantage of
 * the fact that removing attributes is not supported.
 */

static void
init_tuple(RDB_object *tp)
{
    RDB_init_hashmap(&tp->var.tpl_map, RDB_TUPLE_CAPACITY);
    tp->kind = _RDB_TUPLE;
}

int
RDB_tuple_set(RDB_object *tp, const char *namp, const RDB_object *valp)
{
    RDB_object newval;
    RDB_object *oldvalp;
    int res;

    if (tp->kind == _RDB_INITIAL)
        init_tuple(tp);

    /* delete old value */
    oldvalp = (RDB_object *) RDB_hashmap_get(&tp->var.tpl_map, namp, NULL);
    if (oldvalp != NULL) {
        RDB_destroy_obj(oldvalp);
    }

    /* insert new value */
    RDB_init_obj(&newval);
    res = RDB_copy_obj(&newval, valp);
    if (res != RDB_OK)
        return res;
    return RDB_hashmap_put(&tp->var.tpl_map, namp, &newval, sizeof (RDB_object));
}

int
RDB_tuple_set_bool(RDB_object *tp, const char *namp, RDB_bool val)
{
    RDB_object value;

    if (tp->kind == _RDB_INITIAL)
        init_tuple(tp);

    RDB_init_obj(&value);
    RDB_obj_set_bool(&value, val);

    return RDB_hashmap_put(&tp->var.tpl_map, namp, &value, sizeof(value));
} 

int
RDB_tuple_set_int(RDB_object *tp, const char *namp, RDB_int val)
{
    RDB_object value;

    if (tp->kind == _RDB_INITIAL)
        init_tuple(tp);

    RDB_init_obj(&value);
    RDB_obj_set_int(&value, val);

    return RDB_hashmap_put(&tp->var.tpl_map, namp, &value, sizeof(value));
}

int
RDB_tuple_set_rational(RDB_object *tp, const char *namp, RDB_rational val)
{
    RDB_object value;

    if (tp->kind == _RDB_INITIAL)
        init_tuple(tp);

    RDB_init_obj(&value);
    RDB_obj_set_rational(&value, val);

    return RDB_hashmap_put(&tp->var.tpl_map, namp, &value, sizeof(value));
}

int
RDB_tuple_set_string(RDB_object *tp, const char *namp, const char *str)
{
    RDB_object value;
    int res;

    if (tp->kind == _RDB_INITIAL)
        init_tuple(tp);

    RDB_init_obj(&value);
    res = RDB_obj_set_string(&value, str);
    if (res != RDB_OK) {
        RDB_destroy_obj(&value);
        return res;
    }
    res = RDB_hashmap_put(&tp->var.tpl_map, namp, &value, sizeof(value));
    /* Must not destroy value because datap is not copied */
    return res;
}

RDB_object *
RDB_tuple_get(const RDB_object *tp, const char *namp)
{
    return (RDB_object *) RDB_hashmap_get(&tp->var.tpl_map, namp, NULL);
}

RDB_bool
RDB_tuple_get_bool(const RDB_object *tp, const char *namp)
{
    return ((RDB_object *) RDB_hashmap_get(&tp->var.tpl_map, namp, NULL))->var.bool_val;
}

RDB_int
RDB_tuple_get_int(const RDB_object *tp, const char *namp)
{
    return ((RDB_object *) RDB_hashmap_get(&tp->var.tpl_map, namp, NULL))->var.int_val;
}

RDB_rational
RDB_tuple_get_rational(const RDB_object *tp, const char *namp)
{
    return ((RDB_object *) RDB_hashmap_get(&tp->var.tpl_map, namp, NULL))->var.rational_val;
}

char *
RDB_tuple_get_string(const RDB_object *tp, const char *namp)
{
    return ((RDB_object *) RDB_hashmap_get(&tp->var.tpl_map, namp, NULL))
            ->var.bin.datap;
}

RDB_int
RDB_tuple_size(const RDB_object *tplp)
{
    return (RDB_int) RDB_hashmap_size(&tplp->var.tpl_map);
}

void
RDB_tuple_attr_names(const RDB_object *tplp, char **namev)
{
    RDB_hashmap_keys(&tplp->var.tpl_map, namev);
}

int
RDB_extend_tuple(RDB_object *tup, int attrc, RDB_virtual_attr attrv[],
                RDB_transaction *txp)
{
    int i;
    int res;
    RDB_object val;
    
    for (i = 0; i < attrc; i++) {
        res = RDB_evaluate(attrv[i].exp, tup, txp, &val);
        if (res != RDB_OK)
            return res;
        RDB_tuple_set(tup, attrv[i].name, &val);
        RDB_destroy_obj(&val);
    }
    return RDB_OK;
}

struct _RDB_rename_attr_info {
    int renc;
    RDB_renaming *renv;
    const RDB_object *srctup;
    RDB_object *dsttup;
};

static void
rename_attr(RDB_hashmap *hp, const char *attrname, void *arg)
{
    struct _RDB_rename_attr_info *infop = (struct _RDB_rename_attr_info *)arg;
    int ai = _RDB_find_rename_from(infop->renc, infop->renv, attrname);

    if (ai >= 0) {
        RDB_tuple_set(infop->dsttup, infop->renv[ai].to,
                      RDB_tuple_get(infop->srctup, infop->renv[ai].from));
    } else {
        RDB_tuple_set(infop->dsttup, attrname,
                          RDB_tuple_get(infop->srctup, attrname));
    }
}

int
RDB_rename_tuple(const RDB_object *tup, int renc, RDB_renaming renv[],
                 RDB_object *restup)
{
    struct _RDB_rename_attr_info info;

    info.renc = renc;
    info.renv = renv;
    info.srctup = tup;
    info.dsttup = restup;

    /* Copy attributes to tup */
    RDB_hashmap_apply((RDB_hashmap *)&tup->var.tpl_map, &rename_attr, &info);

    return RDB_OK;
}

struct _RDB_copy_attr_info {
    const RDB_object *srctup;
    RDB_object *dsttup;
};

static void
copy_attr(RDB_hashmap *hp, const char *attrname, void *arg)
{
    struct _RDB_copy_attr_info *infop = (struct _RDB_copy_attr_info *)arg;

    RDB_tuple_set(infop->dsttup, attrname, RDB_tuple_get(infop->srctup, attrname));
}

/* Copy all attributes from one tuple to another. */
int
_RDB_copy_tuple(RDB_object *dstp, const RDB_object *srcp)
{
    struct _RDB_copy_attr_info info;

    info.srctup = srcp;
    info.dsttup = dstp;

    /* Copy attributes to tup */
    RDB_hashmap_apply((RDB_hashmap *)&srcp->var.tpl_map, &copy_attr, &info);
    
    return RDB_OK;
}

static void
copy_nexattr(RDB_hashmap *hp, const char *attrname, void *arg)
{
    struct _RDB_copy_attr_info *infop = (struct _RDB_copy_attr_info *)arg;

    if (RDB_tuple_get(infop->dsttup, attrname) == NULL) {
        RDB_tuple_set(infop->dsttup, attrname,
                RDB_tuple_get(infop->srctup, attrname));
    }
}

/* Copy only those attributes which do not exist in the destination tuple */
static int
copy_remaining(RDB_object *dstp, const RDB_object *srcp)
{
    struct _RDB_copy_attr_info info;

    info.srctup = srcp;
    info.dsttup = dstp;

    /* Copy attributes to tup */
    RDB_hashmap_apply((RDB_hashmap *)&srcp->var.tpl_map, &copy_nexattr, &info);
    
    return RDB_OK;
}

int
RDB_wrap_tuple(const RDB_object *tplp, int wrapc, RDB_wrapping wrapv[],
               RDB_object *restplp)
{
    int i, j;
    int ret;
    RDB_object tpl;

    RDB_init_obj(&tpl);

    /* Wrap attributes */
    for (i = 0; i < wrapc; i++) {
        for (j = 0; j < wrapv[i].attrc; j++) {
            ret = RDB_tuple_set(&tpl, wrapv[i].attrv[j],
                    RDB_tuple_get(tplp, wrapv[i].attrv[j]));
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl);
                return ret;
            }
        }
        RDB_tuple_set(restplp, wrapv[i].attrname, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            return ret;
        }
    }
    RDB_destroy_obj(&tpl);

    /* Copy remaining attributes */
    copy_remaining(restplp, tplp);
    
    return RDB_OK;
}

int
RDB_unwrap_tuple(const RDB_object *tplp, int attrc, const char *attrv[],
        RDB_object *restplp)
{
    int i;
    int ret;
    
    for (i = 0; i < attrc; i++) {
        RDB_object *wtplp = RDB_tuple_get(tplp, attrv[i]);

        ret = _RDB_copy_tuple(restplp, wtplp);
        if (ret != RDB_OK)
            return ret;
    }
    
    /* Copy remaining attributes */
    copy_remaining(restplp, tplp);

    return RDB_OK;
}
