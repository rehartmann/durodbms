/* $Id$ */

#include "rdb.h"
#include <gen/strfns.h>
#include <string.h>

RDB_type RDB_BOOLEAN;
RDB_type RDB_INTEGER;
RDB_type RDB_RATIONAL;
RDB_type RDB_STRING;
RDB_type RDB_BINARY;

void _RDB_init_builtin_types(void)
{
    RDB_BOOLEAN.kind = RDB_TP_BOOLEAN;
    RDB_BOOLEAN.name = "BOOLEAN";
    RDB_STRING.kind = RDB_TP_STRING;
    RDB_STRING.name = "STRING";
    RDB_INTEGER.kind = RDB_TP_INTEGER;
    RDB_INTEGER.name = "INTEGER";
    RDB_RATIONAL.kind = RDB_TP_RATIONAL;
    RDB_RATIONAL.name = "RATIONAL";
    RDB_BINARY.kind = RDB_TP_BINARY;
    RDB_BINARY.name = "BINARY";
}

RDB_bool
RDB_type_is_numeric(const RDB_type *typ) {
    return (RDB_bool)(typ->kind == RDB_TP_INTEGER
                      || typ->kind == RDB_TP_RATIONAL);
}

RDB_type *
RDB_create_tuple_type(int attrc, RDB_attr attrv[])
{
    RDB_type *tbtyp;
    int i;
    
    if ((tbtyp = malloc(sizeof(RDB_type))) == NULL)
        return NULL;
    tbtyp->name = NULL;
    tbtyp->kind = RDB_TP_TUPLE;
    if ((tbtyp->complex.tuple.attrv = malloc(sizeof(RDB_attr) * attrc))
            == NULL) {
        free(tbtyp);
        return NULL;
    }
    for (i = 0; i < attrc; i++) {
        tbtyp->complex.tuple.attrv[i].type = attrv[i].type;
        tbtyp->complex.tuple.attrv[i].name = RDB_dup_str(attrv[i].name);
    }   
    tbtyp->complex.tuple.attrc = attrc;
    
    return tbtyp;
}

RDB_type *
RDB_create_relation_type(int attrc, RDB_attr attrv[])
{
    RDB_type *typ = malloc(sizeof (RDB_type));
    
    if (typ == NULL)
        return NULL;
    
    typ->name = NULL;
    typ->kind = RDB_TP_RELATION;
    if ((typ->complex.basetyp = RDB_create_tuple_type(attrc, attrv))
            == NULL) {
        free(typ);
        return NULL;
    }
    return typ;
}

RDB_bool
RDB_is_builtin_type(const RDB_type *typ)
{
    switch(typ->kind) {
        case RDB_TP_BOOLEAN:
        case RDB_TP_INTEGER:
        case RDB_TP_RATIONAL:
        case RDB_TP_STRING:
        case RDB_TP_BINARY:
            return RDB_TRUE;
        default: ;
    }
    return RDB_FALSE;
}

void
RDB_drop_type(RDB_type *typ)
{
    int i;

    if (RDB_is_builtin_type(typ))
        return;

    if (typ->name != NULL) {
        /* delete type from database */
        /* persistent type are not yet implemented */
        free(typ->name);
    }
    switch(typ->kind) {
        case RDB_TP_TUPLE:
            for (i = 0; i < typ->complex.tuple.attrc; i++) {
                RDB_type *attrtyp = typ->complex.tuple.attrv[i].type;
            
                free(typ->complex.tuple.attrv[i].name);
                if (!RDB_is_builtin_type(attrtyp))
                    RDB_drop_type(attrtyp);
            }
            free(typ->complex.tuple.attrv);
            break;
        case RDB_TP_RELATION:
            RDB_drop_type(typ->complex.basetyp);
            break;
        default:
            abort();
    }
    free(typ);
}

RDB_bool
RDB_type_equals(const RDB_type *typ1, const RDB_type *typ2)
{
    if (typ1 == typ2)
        return RDB_TRUE;
    if (typ1->kind != typ2->kind)
        return RDB_FALSE;
    
    /* If the two types both have a name, they are equal iff their name
     * is equal */
    if (typ1->name != NULL && typ2->name != NULL)
         return (RDB_bool) (strcmp(typ1->name, typ2->name) == 0);
    
    switch (typ1->kind) {
        case RDB_TP_RELATION:
            return RDB_type_equals(typ1->complex.basetyp, typ2->complex.basetyp);
        case RDB_TP_TUPLE:
            {
                int i, j;
                int attrcnt = typ1->complex.tuple.attrc;

                if (attrcnt != typ2->complex.tuple.attrc)
                    return RDB_FALSE;
                    
                /* check if all attributes of typ1 also appear in typ2 */
                for (i = 0; i < attrcnt; i++) {
                    for (j = 0; j < attrcnt; j++) {
                        if (RDB_type_equals(typ1->complex.tuple.attrv[i].type,
                                typ2->complex.tuple.attrv[j].type)
                                && (strcmp(typ1->complex.tuple.attrv[i].name,
                                typ2->complex.tuple.attrv[j].name) == 0))
                            break;
                    }
                    if (j >= attrcnt) {
                        /* not found */
                        return RDB_FALSE;
                    }
                }
                return RDB_TRUE;
            }
        default:
            ;
    }
    abort();
}  

char *
RDB_type_name(const RDB_type *typ)
{
    return typ->name;
}

RDB_type *
RDB_extend_tuple_type(const RDB_type *typ, int attrc, RDB_attr attrv[])
{
    int i;
    RDB_type *newtyp = malloc(sizeof (RDB_type));
    
    if (newtyp == NULL)
        return NULL;
    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->complex.tuple.attrc = typ->complex.tuple.attrc + attrc;
    newtyp->complex.tuple.attrv = malloc(sizeof (RDB_attr)
            * (newtyp->complex.tuple.attrc));
    if (newtyp->complex.tuple.attrv == NULL) {
        free(newtyp);
        return NULL;
    }
    for (i = 0; i < typ->complex.tuple.attrc; i++) {
        newtyp->complex.tuple.attrv[i].name = NULL;
    }
    for (i = 0; i < typ->complex.tuple.attrc; i++) {
        newtyp->complex.tuple.attrv[i].name =
                RDB_dup_str(typ->complex.tuple.attrv[i].name);
        if (newtyp->complex.tuple.attrv[i].name == NULL)
            goto error;
        newtyp->complex.tuple.attrv[i].type = typ->complex.tuple.attrv[i].type;
    }
    for (i = 0; i < attrc; i++) {
        newtyp->complex.tuple.attrv[typ->complex.tuple.attrc + i].name =
                RDB_dup_str(attrv[i].name);
        newtyp->complex.tuple.attrv[typ->complex.tuple.attrc + i].type =
                attrv[i].type;
    }
    return newtyp;    

error:
    for (i = 0; i < typ->complex.tuple.attrc; i++)
        free(newtyp->complex.tuple.attrv[i].name);
    free(newtyp->complex.tuple.attrv);
    free(newtyp);
    return NULL;
}

RDB_type *
RDB_extend_relation_type(const RDB_type *typ, int attrc, RDB_attr attrv[])
{
    RDB_type *restyp;

    restyp = malloc(sizeof (RDB_type));
    if (restyp == NULL) {
        return NULL;
    }
    restyp->name = NULL;
    restyp->kind = RDB_TP_RELATION;
    restyp->complex.basetyp = RDB_extend_tuple_type(
            typ->complex.basetyp, attrc, attrv);
    return restyp;
}

int
RDB_join_tuple_types(const RDB_type *typ1, const RDB_type *typ2, RDB_type **newtypp)
{
    RDB_type *newtyp;
    int attrc;
    int i, j;
    int res;
    
    /* Create new tuple type */
    newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL)
        return RDB_NO_MEMORY;

    *newtypp = newtyp;
    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    
    /* calculate new # of attributes as the sum of the # of attributes
     * of both types.
     * That often will be too high; in this case it is reduced later.
     */
    newtyp->complex.tuple.attrc = typ1->complex.tuple.attrc
            + typ2->complex.tuple.attrc;
    newtyp->complex.tuple.attrv = malloc(sizeof (RDB_attr)
            * newtyp->complex.tuple.attrc);

    for (i = 0; i < typ1->complex.tuple.attrc; i++)
        newtyp->complex.tuple.attrv[i].name = NULL;

    /* copy attributes from first tuple type */
    for (i = 0; i < typ1->complex.tuple.attrc; i++) {
        newtyp->complex.tuple.attrv[i].name = RDB_dup_str(
                typ1->complex.tuple.attrv[i].name);
        newtyp->complex.tuple.attrv[i].type = 
                typ1->complex.tuple.attrv[i].type;
    }
    attrc = typ1->complex.tuple.attrc;

    /* add attributes from second tuple type */
    for (i = 0; i < typ2->complex.tuple.attrc; i++) {
        for (j = 0; j < typ1->complex.tuple.attrc; j++) {
            if (strcmp(typ2->complex.tuple.attrv[i].name,
                    typ1->complex.tuple.attrv[j].name) == 0) {
                /* If two attributes match by name, they must be of
                   the same type */
                if (!RDB_type_equals(typ2->complex.tuple.attrv[i].type,
                        typ1->complex.tuple.attrv[j].type)) {
                    res = RDB_TYPE_MISMATCH;
                    goto error;
                }
                break;
            }
        }
        if (j >= typ1->complex.tuple.attrc) {
            /* attribute not found, so add it to result type */
            newtyp->complex.tuple.attrv[attrc].name = RDB_dup_str(
                    typ2->complex.tuple.attrv[i].name);
            newtyp->complex.tuple.attrv[attrc++].type =
                    typ2->complex.tuple.attrv[i].type;
        }
    }

    /* adjust array size, if necessary */    
    if (attrc < newtyp->complex.tuple.attrc) {
        newtyp->complex.tuple.attrc = attrc;
        newtyp->complex.tuple.attrv = realloc(newtyp->complex.tuple.attrv,
                sizeof(RDB_attr) * attrc);
    }
    return RDB_OK;

error:
    for (i = 0; i < typ1->complex.tuple.attrc; i++)
        free(newtyp->complex.tuple.attrv[i].name);

    free(newtyp);
    return res;
}

/* Return the type of the attribute with name attrname in the tuple
   type pointed to by tutyp. */
RDB_type *_RDB_tuple_attr_type(const RDB_type *tuptyp, const char *attrname)
{
    int i;
    
    for (i = 0; i < tuptyp->complex.tuple.attrc; i++) {
        if (strcmp(tuptyp->complex.tuple.attrv[i].name, attrname) == 0)
            return tuptyp->complex.tuple.attrv[i].type;
    }
    /* not found */
    return NULL;
}

int
RDB_project_relation_type(const RDB_type *typ, int attrc, char *attrv[],
                          RDB_type **newtypp)
{
    RDB_type *reltyp;
    RDB_type *tuptyp = NULL;
    int res;
    int i;

    reltyp = malloc(sizeof (RDB_type));
    if (reltyp == NULL) {
        return RDB_NO_MEMORY;
    }

    /* Create tuple type */
    tuptyp = malloc(sizeof (RDB_type));
    if (tuptyp == NULL) {
        res = RDB_NO_MEMORY;
        goto error;
    }
    tuptyp->name = NULL;
    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->complex.tuple.attrc = attrc;
    tuptyp->complex.tuple.attrv = malloc(attrc * sizeof (RDB_attr));
    for (i = 0; i < attrc; i++)
        tuptyp->complex.tuple.attrv[i].name = NULL;

    for (i = 0; i < attrc; i++) {
        char *attrname;
        RDB_type *attrtyp;
    
        attrname  = RDB_dup_str(attrv[i]);
        if (attrname == NULL) {
            res = RDB_NO_MEMORY;
            goto error;
        }
        tuptyp->complex.tuple.attrv[i].name = attrname;

        attrtyp = _RDB_tuple_attr_type(typ->complex.basetyp, attrname);
        if (attrtyp == NULL) {
            res = RDB_ILLEGAL_ARG;
            goto error;
        }
        tuptyp->complex.tuple.attrv[i].type = attrtyp;
        tuptyp->complex.tuple.attrv[i].default_value = NULL;
    }

    reltyp->name = NULL;
    reltyp->kind = RDB_TP_RELATION;
    reltyp->complex.basetyp = tuptyp;

    *newtypp = reltyp;
    return RDB_OK;

error:
    if (tuptyp != NULL) {
      for (i = 0; i < attrc; i++)
          free(tuptyp->complex.tuple.attrv[i].name);
    }
        
    free(reltyp);

    return res;
}
