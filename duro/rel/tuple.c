#include "rdb.h"
#include "internal.h"
#include <string.h>
#include <malloc.h>

/* $Id$ */

#define RDB_TUPLE_CAPACITY 37

/*
 * RDB_tuple is implemented using a static hash table, taking advantage of
 * the fact that removing attributes is not supported.
 */

void
RDB_init_tuple(RDB_tuple *tp)
{
    RDB_init_hashmap(&tp->map, RDB_TUPLE_CAPACITY);
}

static void
destroy_obj(RDB_hashmap *hp, const char *key, void *arg) {
    RDB_destroy_obj((RDB_object *) RDB_hashmap_get(hp, key, NULL));
}

int
RDB_destroy_tuple(RDB_tuple *tp)
{
    RDB_hashmap_apply(&tp->map, destroy_obj, NULL);

    RDB_destroy_hashmap(&tp->map);

    return RDB_OK;
}

int
RDB_tuple_set(RDB_tuple *tp, const char *namp, const RDB_object *valp)
{
    RDB_object newval;
    RDB_object *oldvalp;
    int res;

    /* delete old value */
    oldvalp = (RDB_object *) RDB_hashmap_get(&tp->map, namp, NULL);
    if (oldvalp != NULL) {
        RDB_destroy_obj(oldvalp);
    }

    /* insert new value */
    RDB_init_obj(&newval);
    res = RDB_copy_obj(&newval, valp);
    if (res != RDB_OK)
        return res;
    return RDB_hashmap_put(&tp->map, namp, &newval, sizeof (RDB_object));
}

int
RDB_tuple_set_bool(RDB_tuple *tp, const char *namp, RDB_bool val)
{
    RDB_object value;

    RDB_init_obj(&value);
    RDB_obj_set_bool(&value, val);

    return RDB_hashmap_put(&tp->map, namp, &value, sizeof(value));
} 

int
RDB_tuple_set_int(RDB_tuple *tp, const char *namp, RDB_int val)
{
    RDB_object value;

    RDB_init_obj(&value);
    RDB_obj_set_int(&value, val);

    return RDB_hashmap_put(&tp->map, namp, &value, sizeof(value));
}

int
RDB_tuple_set_rational(RDB_tuple *tp, const char *namp, RDB_rational val)
{
    RDB_object value;

    RDB_init_obj(&value);
    RDB_obj_set_rational(&value, val);

    return RDB_hashmap_put(&tp->map, namp, &value, sizeof(value));
}

int
RDB_tuple_set_string(RDB_tuple *tp, const char *namp, const char *str)
{
    RDB_object value;
    int res;

    RDB_init_obj(&value);
    res = RDB_obj_set_string(&value, str);
    if (res != RDB_OK) {
        RDB_destroy_obj(&value);
        return res;
    }
    res = RDB_hashmap_put(&tp->map, namp, &value, sizeof(value));
    /* Must not destroy value because datap is not copied */
    return res;
}

RDB_object *
RDB_tuple_get(const RDB_tuple *tp, const char *namp)
{
    return (RDB_object *) RDB_hashmap_get(&tp->map, namp, NULL);
}

RDB_bool
RDB_tuple_get_bool(const RDB_tuple *tp, const char *namp)
{
    return ((RDB_object *) RDB_hashmap_get(&tp->map, namp, NULL))->var.bool_val;
}

RDB_int
RDB_tuple_get_int(const RDB_tuple *tp, const char *namp)
{
    return ((RDB_object *) RDB_hashmap_get(&tp->map, namp, NULL))->var.int_val;
}

RDB_rational
RDB_tuple_get_rational(const RDB_tuple *tp, const char *namp)
{
    return ((RDB_object *) RDB_hashmap_get(&tp->map, namp, NULL))->var.rational_val;
}

char *
RDB_tuple_get_string(const RDB_tuple *tp, const char *namp)
{
    return ((RDB_object *) RDB_hashmap_get(&tp->map, namp, NULL))
            ->var.bin.datap;
}

RDB_int
RDB_tuple_size(const RDB_tuple *tplp)
{
    return (RDB_int) RDB_hashmap_size(&tplp->map);
}

void
RDB_tuple_attr_names(const RDB_tuple *tplp, char **namev)
{
    RDB_hashmap_keys(&tplp->map, namev);
}

int
RDB_extend_tuple(RDB_tuple *tup, int attrc, RDB_virtual_attr attrv[],
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
    const RDB_tuple *srctup;
    RDB_tuple *dsttup;
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
RDB_rename_tuple(const RDB_tuple *tup, int renc, RDB_renaming renv[],
                 RDB_tuple *restup)
{
    struct _RDB_rename_attr_info info;

    info.renc = renc;
    info.renv = renv;
    info.srctup = tup;
    info.dsttup = restup;

    /* Copy attributes to tup */
    RDB_hashmap_apply((RDB_hashmap *)&tup->map, &rename_attr, &info);

    return RDB_OK;
}
