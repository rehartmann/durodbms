/*
 * Functions to convert a DuroDBMS object to JSON.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "json.h"
#include "rdb.h"

#include <stdio.h>

static int
append_string_json(RDB_object *strobjp, const char *str,
        RDB_exec_context *ecp)
{
    char buf[7];
    const char *cp = str;

    if (RDB_append_char(strobjp, '\"', ecp) != RDB_OK)
        return RDB_ERROR;
    while (*cp != '\0') {
        if (((unsigned int) *cp) <= 0x1f) {
            /* Write control characters as \uxxxx */
            sprintf(buf, "\\u%04x",(unsigned int) *cp);
            if (RDB_append_string(strobjp, buf, ecp) != RDB_OK)
                return RDB_ERROR;
        } else {
            /* Escape backslash and double quotation mark */
            if (*cp == '\\' || *cp == '\"') {
                if (RDB_append_char(strobjp, '\\', ecp) != RDB_OK)
                    return RDB_ERROR;
            }
            if (RDB_append_char(strobjp, *cp, ecp) != RDB_OK)
                return RDB_ERROR;
        }
        ++cp;
    }
    return RDB_append_char(strobjp, '\"', ecp);
}

static int
append_obj_json(RDB_object *, const RDB_object *, RDB_exec_context *, RDB_transaction *);

static int
append_array_json(RDB_object *strobjp, const RDB_object *arrp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_int i;
    RDB_object *elemp;
    RDB_int len = (int) RDB_array_length(arrp, ecp);
    if (len < 0)
        return RDB_ERROR;

    if (RDB_append_char(strobjp, '[', ecp) != RDB_OK)
        return RDB_ERROR;
    for (i = 0; i < len; i++) {
        elemp = RDB_array_get(arrp, i, ecp);
        if (elemp == NULL)
            return RDB_ERROR;
        if (append_obj_json(strobjp, elemp, ecp, txp) != RDB_OK)
            return RDB_ERROR;
        if (i < len - 1) {
            if (RDB_append_char(strobjp, ',', ecp) != RDB_OK)
                return RDB_ERROR;
        }
    }
    return RDB_append_char(strobjp, ']', ecp);
}

int
append_datetime_json(RDB_object *strobjp, const RDB_object *dt,
        RDB_exec_context *ecp)
{
    struct tm tm;
    char buf[22];

    RDB_datetime_to_tm(&tm, dt);
    strftime(buf, sizeof(buf), "\"%Y-%m-%dT%H:%M:%S\"", &tm);
    if (RDB_append_string(strobjp, buf, ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_OK;
}

int
append_tuple_json(RDB_object *strobjp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int tsize = RDB_tuple_size(tplp);
    char **namev = RDB_alloc(tsize * sizeof(char *), ecp);
    if (namev == NULL)
        return RDB_ERROR;

    RDB_tuple_attr_names(tplp, namev);

    if (RDB_append_char(strobjp, '{', ecp) != RDB_OK)
        goto error;
    for (i = 0; i < tsize; i++) {
        if (RDB_append_char(strobjp, '\"', ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(strobjp, namev[i], ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(strobjp, "\":", ecp) != RDB_OK)
            goto error;

        if (append_obj_json(strobjp, RDB_tuple_get(tplp, namev[i]), ecp, txp) != RDB_OK)
            goto error;

        if (i < tsize - 1) {
            if (RDB_append_char(strobjp, ',', ecp) != RDB_OK) {
                goto error;
            }
        }
    }
    if (RDB_append_char(strobjp, '}', ecp) != RDB_OK)
        goto error;

    free(namev);
    return RDB_OK;

error:
    free(namev);
    return RDB_ERROR;
}

int
append_possrep_json(RDB_object *strobjp, const RDB_object *objp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int prc;
    int i;
    RDB_object compval;
    RDB_possrep *possrep = RDB_type_possreps(RDB_obj_type(objp), &prc);

    if (RDB_append_char(strobjp, '{', ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_append_string(strobjp, "\"@type\":\"", ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(strobjp, RDB_type_name(RDB_obj_type(objp)), ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(strobjp, "\",", ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    for (i = 0; i < possrep->compc; i++) {
        if (RDB_append_char(strobjp, '"', ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_append_string(strobjp, possrep->compv[i].name, ecp) != RDB_OK)
            return RDB_ERROR;
        if (RDB_append_string(strobjp, "\":", ecp) != RDB_OK)
            return RDB_ERROR;

        RDB_init_obj(&compval);

        if (RDB_obj_property(objp, possrep->compv[i].name, &compval, NULL, ecp, txp) != RDB_OK) {
            RDB_destroy_obj(&compval, ecp);
            return RDB_ERROR;
        }

        if (append_obj_json(strobjp, &compval, ecp, txp) != RDB_OK) {
            RDB_destroy_obj(&compval, ecp);
            return RDB_ERROR;
        }

        if (i < possrep->compc - 1) {
            if (RDB_append_char(strobjp, ',', ecp) != RDB_OK) {
                RDB_destroy_obj(&compval, ecp);
                return RDB_ERROR;
            }
        }

        RDB_destroy_obj(&compval, ecp);
    }

    if (RDB_append_char(strobjp, '}', ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}

static int
append_obj_json(RDB_object *strobjp, const RDB_object *objp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    char buf[64];
    RDB_type *typ = RDB_obj_type(objp);

    if (typ == &RDB_STRING) {
        return append_string_json(strobjp, RDB_obj_string(objp), ecp);
    }
    if (typ == &RDB_INTEGER) {
        sprintf(buf, "%d", (int) RDB_obj_int(objp));
        return RDB_append_string(strobjp, buf, ecp);
    }
    if (typ == &RDB_FLOAT) {
        sprintf(buf, "%g", (double) RDB_obj_float(objp));
        return RDB_append_string(strobjp, buf, ecp);
    }
    if (typ == &RDB_BOOLEAN) {
        return RDB_append_string(strobjp, RDB_obj_bool(objp) ? "true" : "false",
                ecp);
    }
    if (typ == &RDB_DATETIME) {
        return append_datetime_json(strobjp, objp, ecp);
    }
    if (RDB_is_tuple(objp)) {
        return append_tuple_json(strobjp, objp, ecp, txp);
    }
    if (RDB_is_array(objp)) {
        return append_array_json (strobjp, objp, ecp, txp);
    }
    if (typ != NULL && RDB_type_has_possreps(typ)) {
        return append_possrep_json(strobjp, objp, ecp, txp);
    }
    RDB_raise_invalid_argument("unsupported type", ecp);
    return RDB_ERROR;
}

/**
 * Converts a RDB_object to JSON. Tables are not supported.
 */
int
RDB_obj_to_json(RDB_object *strobjp, const RDB_object *objp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    if (RDB_string_to_obj(strobjp, "", ecp) != RDB_OK)
        return RDB_ERROR;
    return append_obj_json(strobjp, objp, ecp, txp);
}
