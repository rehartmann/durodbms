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
destroy_value(RDB_hashmap *hp, const char *key) {
    RDB_destroy_value((RDB_value *) RDB_hashmap_get(hp, key, NULL));
}

void
RDB_destroy_tuple(RDB_tuple *tp)
{
    RDB_hashmap_apply(&tp->map, destroy_value);

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

    value.typ = &RDB_BOOLEAN;
    value.var.bool_val = val;

    return RDB_hashmap_put(&tp->map, namp, &value, sizeof(value));
} 

int
RDB_tuple_set_int(RDB_tuple *tp, const char *namp, RDB_int val)
{
    RDB_value value;

    value.typ = &RDB_INTEGER;
    value.var.int_val = val;

    return RDB_hashmap_put(&tp->map, namp, &value, sizeof(value));
}

int
RDB_tuple_set_rational(RDB_tuple *tp, const char *namp, RDB_rational val)
{
    RDB_value value;

    value.typ = &RDB_RATIONAL;
    value.var.rational_val = val;

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
