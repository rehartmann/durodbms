/*
 * Record map functions implemented using PostgreSQL
 * 
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "pgrecmap.h"
#include <rec/recmapimpl.h>
#include <rec/envimpl.h>
#include <rec/dbdefs.h>
#include <pgrec/pgcursor.h>
#include <obj/excontext.h>
#include <obj/object.h>
#include <gen/strfns.h>
#include <arpa/inet.h>

static RDB_recmap *
new_pg_recmap(const char *name, RDB_environment *envp,
        int fieldc, const RDB_field_info fieldinfov[],
        int keyfieldc, int flags, RDB_exec_context *ecp)
{
    RDB_recmap *rmp = RDB_new_recmap(name, NULL, envp, fieldc, fieldinfov,
            keyfieldc, flags, ecp);
    if (rmp == NULL) {
        return NULL;
    }

    rmp->close_recmap_fn = RDB_close_pg_recmap;
    rmp->delete_recmap_fn = &RDB_delete_pg_recmap;
    rmp->insert_rec_fn = &RDB_insert_pg_rec;
    rmp->update_rec_fn = &RDB_update_pg_rec;
    rmp->delete_rec_fn = &RDB_delete_pg_rec;
    rmp->get_fields_fn = &RDB_get_pg_fields;
    rmp->contains_rec_fn = &RDB_contains_pg_rec;
    rmp->recmap_est_size_fn = &RDB_pg_recmap_est_size;
    rmp->cursor_fn = &RDB_pg_recmap_cursor;

    rmp->fieldcount = fieldc;
    return rmp;
}

RDB_recmap *
RDB_create_pg_recmap(const char *name, const char *filename,
        RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[], int keyfieldc,
        const RDB_compare_field cmpv[], int flags,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_object command;
    int i;
    PGresult *res = NULL;
    RDB_recmap *rmp = new_pg_recmap(name, envp, fieldc, fieldinfov, keyfieldc,
            flags, ecp);
    if (rmp == NULL)
        return NULL;

    RDB_init_obj(&command);
    if (RDB_string_to_obj(&command, "CREATE TABLE ", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, name, ecp) != RDB_OK)
        goto error;
    if (RDB_append_char(&command, '(', ecp) != RDB_OK)
        goto error;

    for (i = 0; i < fieldc; i++) {
        if (RDB_append_string(&command, "d_", ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&command, fieldinfov[i].attrname, ecp) != RDB_OK)
            goto error;
        if (RDB_append_char(&command, ' ', ecp) != RDB_OK)
            goto error;
        if (RDB_FTYPE_CHAR & fieldinfov[i].flags) {
            if (fieldinfov[i].len != RDB_VARIABLE_LEN) {
                RDB_raise_invalid_argument("fixed-size character types are not supported", ecp);
                goto error;
            }
            if (RDB_append_string(&command, "text", ecp) != RDB_OK)
                goto error;
        } else {
            if (fieldinfov[i].len == sizeof(RDB_int) && (RDB_FTYPE_INTEGER & fieldinfov[i].flags)) {
                if (RDB_append_string(&command, "integer", ecp) != RDB_OK)
                    goto error;
            } else if (fieldinfov[i].len == sizeof(RDB_float) && (RDB_FTYPE_FLOAT & fieldinfov[i].flags)) {
                if (RDB_append_string(&command, "double precision", ecp) != RDB_OK)
                    goto error;
            } else if (RDB_FTYPE_BOOLEAN & fieldinfov[i].flags) {
                if (RDB_append_string(&command, "boolean", ecp) != RDB_OK)
                    goto error;
            } else {
                if (RDB_append_string(&command, "bytea", ecp) != RDB_OK)
                    goto error;
            }
        }
        if (RDB_append_string(&command, " NOT NULL", ecp) != RDB_OK)
            goto error;
        if (i < fieldc - 1) {
            if (RDB_append_char(&command, ',', ecp) != RDB_OK)
                goto error;
        }
    }
    if (keyfieldc > 0) {
        if (RDB_append_string(&command, ", PRIMARY KEY(", ecp) != RDB_OK)
            goto error;
        for (i = 0; i < keyfieldc; i++) {
            if (RDB_append_string(&command, "d_", ecp) != RDB_OK)
                goto error;
            if (RDB_append_string(&command, fieldinfov[i].attrname, ecp) != RDB_OK)
                goto error;
            if (i < keyfieldc - 1) {
                if (RDB_append_char(&command, ',', ecp) != RDB_OK)
                    goto error;
            }
        }
        if (RDB_append_char(&command, ')', ecp) != RDB_OK)
            goto error;
    }
    if (RDB_append_char(&command, ')', ecp) != RDB_OK)
        goto error;

    res = PQexec(envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_raise_system(PQerrorMessage(envp->env.pgconn), ecp);
        PQclear(res);
        goto error;
    }
    PQclear(res);
    RDB_destroy_obj(&command, ecp);
    return rmp;

error:
    RDB_free(rmp->namp);
    RDB_free(rmp->filenamp);
    RDB_free(rmp->fieldinfos);
    RDB_free(rmp);
    RDB_destroy_obj(&command, ecp);
    return NULL;
}

RDB_recmap *
RDB_open_pg_recmap(const char *name, const char *filename,
       RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[],
       int keyfieldc, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    PGresult *res;
    char *resval;

    /* Check if the table exists */
    res = PQexecParams(envp->env.pgconn,
            "SELECT EXISTS(SELECT tablename FROM pg_tables WHERE tablename=$1)",
            1, NULL, &name, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        RDB_raise_system(PQerrorMessage(envp->env.pgconn), ecp);
        PQclear(res);
        return NULL;
    }
    resval = PQgetvalue(res, 0, 0);
    if (resval[0] == 'f') {
        PQclear(res);
        RDB_raise_not_found(name, ecp);
        return NULL;
    }
    PQclear(res);

    return new_pg_recmap(name, envp, fieldc, fieldinfov, keyfieldc,
            0, ecp);
}

int
RDB_close_pg_recmap(RDB_recmap *rmp, RDB_exec_context *ecp)
{
    RDB_free(rmp->namp);
    RDB_free(rmp->filenamp);
    RDB_free(rmp->fieldinfos);
    RDB_free(rmp);
    return RDB_OK;
}

int
RDB_delete_pg_recmap(RDB_recmap *rmp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_raise_not_supported("RDB_delete_pg_recmap", ecp);
    return RDB_ERROR;
}

void *
RDB_field_to_pg(RDB_field *field, RDB_field_info *fieldinfo, int *formatp,
        RDB_exec_context *ecp)
{
    void *valuep = RDB_alloc(field->len, ecp);
    if (valuep == NULL)
        return NULL;

    (*field->copyfp)(valuep, field->datap, field->len);
    if (RDB_FTYPE_INTEGER & fieldinfo->flags)
        *((uint32_t *)valuep) = htonl(*((uint32_t *)valuep));
    *formatp = RDB_FTYPE_CHAR & fieldinfo->flags ? 0 : 1;
    return valuep;
}

int
RDB_insert_pg_rec(RDB_recmap *rmp, RDB_field flds[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_object command;
    int i;
    char parambuf[12];
    PGresult *res = NULL;
    int *lenv = NULL;
    void **valuev = NULL;
    int *formatv = NULL;

    RDB_init_obj(&command);

    lenv = RDB_alloc(rmp->fieldcount * sizeof(int), ecp);
    if (lenv == NULL)
        goto error;
    valuev = RDB_alloc(rmp->fieldcount * sizeof(void*), ecp);
    if (valuev == NULL)
        goto error;
    for (i = 0; i < rmp->fieldcount; i++) {
        valuev[i] = NULL;
    }
    formatv = RDB_alloc(rmp->fieldcount * sizeof(int), ecp);
    if (formatv == NULL)
        goto error;

    if (RDB_string_to_obj(&command, "INSERT INTO ", ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_append_string(&command, rmp->namp, ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_append_string(&command, " VALUES(", ecp) != RDB_OK) {
        goto error;
    }
    for (i = 0; i < rmp->fieldcount; i++) {
        sprintf(parambuf, "$%d", i + 1);
        if (RDB_append_string(&command, parambuf, ecp) != RDB_OK) {
            goto error;
        }
        if ( i < rmp->fieldcount - 1) {
            if (RDB_append_char(&command, ',', ecp) != RDB_OK) {
                goto error;
            }
        }
        lenv[i] = (int) flds[i].len;
        valuev[i] = RDB_field_to_pg(&flds[i], &rmp->fieldinfos[i], &formatv[i], ecp);
        if (valuev[i] == NULL)
            goto error;
    }
    if (RDB_append_char(&command, ')', ecp) != RDB_OK) {
        goto error;
    }

    res = PQexecParams(rmp->envp->env.pgconn, RDB_obj_string(&command),
            rmp->fieldcount, NULL, (const char * const *) valuev, lenv,
            formatv, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_raise_system(PQerrorMessage(rmp->envp->env.pgconn), ecp);
        PQclear(res);
        goto error;
    }

    PQclear(res);
    RDB_free(lenv);
    for (i = 0; i < rmp->fieldcount; i++) {
        RDB_free(valuev[i]);
    }
    RDB_free(formatv);
    RDB_free(valuev);
    RDB_destroy_obj(&command, ecp);
    return RDB_OK;

error:
    if (lenv != NULL)
        RDB_free(lenv);
    if (formatv != NULL)
        RDB_free(formatv);
    if (valuev != NULL) {
        for (i = 0; i < rmp->fieldcount; i++) {
            if (valuev[i] != NULL)
                RDB_free(valuev[i]);
        }
        RDB_free(valuev);
    }
    RDB_destroy_obj(&command, ecp);
    return RDB_ERROR;
}

int
RDB_update_pg_rec(RDB_recmap *rmp, RDB_field keyv[],
               int fieldc, const RDB_field fieldv[], RDB_rec_transaction *rtxp,
               RDB_exec_context *ecp)
{
    RDB_raise_not_supported("RDB_update_pg_rec", ecp);
    return RDB_ERROR;
}

int
RDB_delete_pg_rec(RDB_recmap *rmp, RDB_field keyv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_raise_not_supported("RDB_delete_pg_rec", ecp);
    return RDB_ERROR;
}

int
RDB_get_pg_fields(RDB_recmap *rmp, RDB_field keyv[], int fieldc,
        RDB_rec_transaction *rtxp, RDB_field retfieldv[], RDB_exec_context *ecp)
{
    static PGresult *res = NULL;
    static RDB_int *intres = NULL;
    int i;
    RDB_object command;
    char numbuf[14];
    int *lenv = NULL;
    void **valuev = NULL;
    int *formatv = NULL;

    RDB_init_obj(&command);

    lenv = RDB_alloc(rmp->keyfieldcount * sizeof(int), ecp);
    if (lenv == NULL)
        goto error;
    valuev = RDB_alloc(rmp->keyfieldcount * sizeof(void*), ecp);
    if (valuev == NULL)
        goto error;
    for (i = 0; i < rmp->keyfieldcount; i++) {
        valuev[i] = NULL;
    }
    formatv = RDB_alloc(rmp->keyfieldcount * sizeof(int), ecp);
    if (formatv == NULL)
        goto error;

    if (RDB_string_to_obj(&command, "SELECT ", ecp) != RDB_OK)
        goto error;
    for (i = 0; i < fieldc; i++) {
        if (RDB_append_string(&command, "d_", ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&command,
                rmp->fieldinfos[retfieldv[i].no].attrname, ecp) != RDB_OK) {
            goto error;
        }
        if (i < fieldc - 1) {
            if (RDB_append_char(&command, ',', ecp) != RDB_OK)
                goto error;
        }
    }
    if (RDB_append_string(&command, " FROM ", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, rmp->namp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, " WHERE ", ecp) != RDB_OK)
        goto error;

    for (i = 0; i < rmp->keyfieldcount; i++) {
        if (RDB_append_string(&command, "d_", ecp))
            goto error;
        if (RDB_append_string(&command, rmp->fieldinfos[i].attrname, ecp))
            goto error;
        sprintf(numbuf, "=$%d", i + 1);
        if (RDB_append_string(&command, numbuf, ecp) != RDB_OK)
            goto error;
        if (i < rmp->keyfieldcount - 1) {
            if (RDB_append_string(&command, " AND ", ecp) != RDB_OK)
                goto error;
        }
        lenv[i] = (int) keyv[i].len;
        valuev[i] = RDB_field_to_pg(&keyv[i], &rmp->fieldinfos[i], &formatv[i], ecp);
        if (valuev[i] == NULL)
            goto error;
    }
    printf("command: %s\n", RDB_obj_string(&command));
    if (res != NULL)
        PQclear(res);
    res = PQexecParams(rmp->envp->env.pgconn, RDB_obj_string(&command),
            rmp->keyfieldcount, NULL, (const char * const *) valuev, lenv,
            formatv, 1);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        RDB_raise_system(PQerrorMessage(rmp->envp->env.pgconn), ecp);
        PQclear(res);
        res = NULL;
        goto error;
    }
    if (PQntuples(res) == 0) {
        RDB_raise_not_found("no data", ecp);
        goto error;
    }

    /* Read integer fields and adjust byte order */
    if (intres != NULL) {
        RDB_free(intres);
    }
    intres = RDB_alloc(sizeof(RDB_int) * fieldc, ecp);
    if (intres == NULL)
        goto error;
    for (i = 0; i < fieldc; i++) {
        retfieldv[i].datap = PQgetvalue(res, 0, i);
        retfieldv[i].len = (size_t) PQgetlength(res, 0, i);
        if (RDB_FTYPE_INTEGER & rmp->fieldinfos[retfieldv[i].no].flags) {
            intres[i] = ntohl(*((uint32_t *)retfieldv[i].datap));
            retfieldv[i].datap = &intres[i];
        }
    }

    for (i = 0; i < rmp->keyfieldcount; i++) {
        RDB_free(valuev[i]);
    }
    RDB_free(valuev);
    RDB_free(lenv);
    RDB_free(formatv);
    RDB_destroy_obj(&command, ecp);
    return RDB_OK;

error:
    if (valuev != NULL) {
        for (i = 0; i < rmp->keyfieldcount; i++) {
            if (valuev[i] != NULL)
                RDB_free(valuev[i]);
        }
        RDB_free(valuev);
    }
    RDB_free(lenv);
    RDB_free(formatv);
    RDB_destroy_obj(&command, ecp);
    return RDB_ERROR;
}

int
RDB_contains_pg_rec(RDB_recmap *rmp, RDB_field flds[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_raise_not_supported("RDB_contains_pg_rec", ecp);
    return RDB_ERROR;
}

int
RDB_pg_recmap_est_size(RDB_recmap *rmp, RDB_rec_transaction *rtxp, unsigned *sz,
        RDB_exec_context *ecp)
{
    *sz = 0;
    return RDB_OK;
}