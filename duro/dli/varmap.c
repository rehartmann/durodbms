/*
 * varmap.c
 *
 *  Created on: 16.01.2016
 *      Author: rene
 */

#include "varmap.h"

#include <stdlib.h>
#include <string.h>

#include <gen/hashtabit.h>
#include <gen/strfns.h>
#include <obj/object.h>
#include <obj/excontext.h>

static unsigned
hash_varentry(const void *entryp, void *arg)
{
    return RDB_hash_str(((Duro_var_entry *) entryp)->name);
}

static RDB_bool
varentry_equals(const void *e1p, const void *e2p, void *arg)
{
    return (RDB_bool) (strcmp(((Duro_var_entry *) e1p)->name,
            ((Duro_var_entry *) e2p)->name) == 0);
}

void
Duro_init_varmap(Duro_varmap *varmap, int capacity) {
    RDB_init_hashtable(&varmap->hashtab, capacity, &hash_varentry,
            &varentry_equals);
}

void
Duro_destroy_varmap(Duro_varmap *varmap)
{
    Duro_var_entry *entryp;
    RDB_hashtable_iter hiter;

    RDB_init_hashtable_iter(&hiter, &varmap->hashtab);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        free(entryp->name);
        free(entryp);
    }
    RDB_destroy_hashtable_iter(&hiter);

    RDB_destroy_hashtable(&varmap->hashtab);
}

int
Duro_varmap_put(Duro_varmap *varmap, const char *name,
        RDB_object *varp, RDB_bool isconst, RDB_exec_context *ecp)
{
    int ret;
    Duro_var_entry entry;
    Duro_var_entry *entryp;

    entry.name = (char *) name;
    entryp = RDB_hashtable_get(&varmap->hashtab, &entry, NULL);
    if (entryp == NULL) {
        entryp = RDB_alloc(sizeof (Duro_var_entry), ecp);
        if (entryp == NULL) {
            return RDB_ERROR;
        }
        entryp->name = RDB_dup_str(name);
        if (entryp->name == NULL) {
            RDB_free(entryp);
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        ret = RDB_hashtable_put(&varmap->hashtab, entryp, NULL);
        if (ret != RDB_OK) {
            free(entryp->name);
            RDB_free(entryp);
            RDB_errno_to_error(ret, ecp);
            return RDB_ERROR;
        }
    }
    entryp->varp = varp;
    entryp->is_const = isconst;
    return RDB_OK;
}

Duro_var_entry *
Duro_varmap_get(const Duro_varmap *varmap, const char *name)
{
    Duro_var_entry entry;

    entry.name = (char *) name;
    return RDB_hashtable_get(&varmap->hashtab, &entry, NULL);
}
