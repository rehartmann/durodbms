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
destroy_value(RDB_hashmap *hp, const char *key, void *arg) {
    RDB_destroy_value((RDB_value *) RDB_hashmap_get(hp, key, NULL));
}

void
RDB_destroy_tuple(RDB_tuple *tp)
{
    RDB_hashmap_apply(&tp->map, destroy_value, NULL);

    RDB_destroy_hashmap(&tp->map);
}

int
RDB_tuple_set(RDB_tuple *tp, const char *namp, const RDB_value *valp)
{
    RDB_value newval;
    RDB_value *oldvalp;
    int res;

    /* delete old value */
    oldvalp = (RDB_value *) RDB_hashmap_get(&tp->map, namp, NULL);
    if (oldvalp != NULL) {
        RDB_destroy_value(oldvalp);
    }

    /* insert new value */
    RDB_init_value(&newval);
    res = RDB_copy_value(&newval, valp);
    if (res != RDB_OK)
        return res;
    return RDB_hashmap_put(&tp->map, namp, &newval, sizeof (RDB_value));
}

int
RDB_tuple_set_bool(RDB_tuple *tp, const char *namp, RDB_bool val)
{
    RDB_value value;

    RDB_init_value(&value);
    RDB_value_set_bool(&value, val);

    return RDB_hashmap_put(&tp->map, namp, &value, sizeof(value));
} 

int
RDB_tuple_set_int(RDB_tuple *tp, const char *namp, RDB_int val)
{
    RDB_value value;

    RDB_init_value(&value);
    RDB_value_set_int(&value, val);

    return RDB_hashmap_put(&tp->map, namp, &value, sizeof(value));
}

int
RDB_tuple_set_rational(RDB_tuple *tp, const char *namp, RDB_rational val)
{
    RDB_value value;

    RDB_init_value(&value);
    RDB_value_set_rational(&value, val);

    return RDB_hashmap_put(&tp->map, namp, &value, sizeof(value));
}

int
RDB_tuple_set_string(RDB_tuple *tp, const char *namp, const char *str)
{
    RDB_value value;
    int res;

    RDB_init_value(&value);
    res = RDB_value_set_string(&value, str);
    if (res != RDB_OK) {
        RDB_destroy_value(&value);
        return res;
    }
    res = RDB_hashmap_put(&tp->map, namp, &value, sizeof(value));
    /* Must not destroy value because datap is not copied */
    return res;
}

RDB_value *
RDB_tuple_get(const RDB_tuple *tp, const char *namp)
{
    return (RDB_value *) RDB_hashmap_get(&tp->map, namp, NULL);
}

RDB_bool
RDB_tuple_get_bool(const RDB_tuple *tp, const char *namp)
{
    return ((RDB_value *) RDB_hashmap_get(&tp->map, namp, NULL))->var.bool_val;
}

RDB_int
RDB_tuple_get_int(const RDB_tuple *tp, const char *namp)
{
    return ((RDB_value *) RDB_hashmap_get(&tp->map, namp, NULL))->var.int_val;
}

RDB_rational
RDB_tuple_get_rational(const RDB_tuple *tp, const char *namp)
{
    return ((RDB_value *) RDB_hashmap_get(&tp->map, namp, NULL))->var.rational_val;
}

char *
RDB_tuple_get_string(const RDB_tuple *tp, const char *namp)
{
    return ((RDB_value *) RDB_hashmap_get(&tp->map, namp, NULL))
            ->var.bin.datap;
}

int
RDB_tuple_extend(RDB_tuple *tup, int attrc, RDB_virtual_attr attrv[],
                RDB_transaction *txp)
{
    int i;
    int res;
    RDB_value val;
    
    for (i = 0; i < attrc; i++) {
        res = RDB_evaluate(attrv[i].exp, tup, txp, &val);
        if (res != RDB_OK)
            return res;
        RDB_tuple_set(tup, attrv[i].name, &val);
        RDB_destroy_value(&val);
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
RDB_tuple_rename(const RDB_tuple *tup, int renc, RDB_renaming renv[],
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
