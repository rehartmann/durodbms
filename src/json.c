/*
 * Functions to convert a DuroDBMS tuple to JSON.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "json.h"

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
append_obj_json(RDB_object *, RDB_object *, RDB_exec_context *);

static int
append_array_json(RDB_object *strobjp, RDB_object *arrp, RDB_exec_context *ecp)
{
    RDB_int i;
    RDB_object *elemp;
    RDB_int len = (int) RDB_array_length(arrp, ecp);
    if (len < 0)
        return RDB_ERROR;

    if (RDB_append_string(strobjp, "[ ", ecp) != RDB_OK)
        return RDB_ERROR;
    for (i = 0; i < len; i++) {
        elemp = RDB_array_get(arrp, i, ecp);
        if (elemp == NULL)
            return RDB_ERROR;
        if (append_obj_json(strobjp, elemp, ecp) != RDB_OK)
            return RDB_ERROR;
        if (i < len - 1) {
            if (RDB_append_string(strobjp, ", ", ecp) != RDB_OK)
                return RDB_ERROR;
        }
    }
    return RDB_append_string(strobjp, " ]", ecp);
}

int
append_tuple_json(RDB_object *strobjp, RDB_object *tplp, RDB_exec_context *ecp)
{
    int i;
    int tsize = RDB_tuple_size(tplp);
    char **namev = RDB_alloc(tsize * sizeof(char *), ecp);
    if (namev == NULL)
        return RDB_ERROR;

    RDB_tuple_attr_names(tplp, namev);

    if (RDB_append_string(strobjp, "{\n", ecp) != RDB_OK)
        goto error;
    for (i = 0; i < tsize; i++) {
        if (RDB_append_char(strobjp, '\"', ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(strobjp, namev[i], ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(strobjp, "\": ", ecp) != RDB_OK)
            goto error;

        if (append_obj_json(strobjp, RDB_tuple_get(tplp, namev[i]), ecp) != RDB_OK)
            goto error;

        if (RDB_append_string(strobjp, i < tsize - 1 ? ",\n" : "\n", ecp)
                != RDB_OK) {
            goto error;
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

static int
append_obj_json(RDB_object *strobjp, RDB_object *objp, RDB_exec_context *ecp)
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
        sprintf(buf, "%e", (double) RDB_obj_float(objp));
        return RDB_append_string(strobjp, buf, ecp);
    }
    if (typ == &RDB_BOOLEAN) {
        return RDB_append_string(strobjp, RDB_obj_bool(objp) ? "true" : "false",
                ecp);
    }
    if (RDB_is_tuple(objp)) {
        return append_tuple_json(strobjp, objp, ecp);
    }
    if (RDB_is_array(objp)) {
        return append_array_json (strobjp, objp, ecp);
    }
    RDB_raise_invalid_argument("unsupported type", ecp);
    return RDB_ERROR;
}

int
Dr_obj_to_json(RDB_object *strobjp, RDB_object *tplp, RDB_exec_context *ecp)
{
    if (RDB_string_to_obj(strobjp, "", ecp) != RDB_OK)
        return RDB_ERROR;
    return append_obj_json(strobjp, tplp, ecp);
}
