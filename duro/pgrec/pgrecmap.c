/*
 * Record map functions implemented using PostgreSQL
 * 
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "pgrecmap.h"
#include "pgcursor.h"
#include "pgenv.h"
#include "pgtx.h"
#include "pgindex.h"
#include <rec/recmapimpl.h>
#include <rec/envimpl.h>
#include <rec/dbdefs.h>
#include <obj/excontext.h>
#include <obj/object.h>
#include <gen/types.h>
#include <gen/strfns.h>
#include <arpa/inet.h>
#include <string.h>

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
    rmp->create_index_fn = &RDB_create_pg_index;
    rmp->open_index_fn = NULL;

    rmp->fieldcount = fieldc;
    return rmp;
}

RDB_recmap *
RDB_create_pg_recmap(const char *name,
        RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[], int keyfieldc,
        int cmpc, const RDB_compare_field cmpv[], int flags,
        int keyc, const RDB_string_vec *keyv,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_object command;
    int i;
    PGresult *res = NULL;
    RDB_recmap *rmp;

    if (cmpc != 0) {
        RDB_raise_not_supported("comparison function not supported", ecp);
        return NULL;
    }
    if (RDB_ORDERED & flags) {
        RDB_raise_not_supported("RDB_ORDERED", ecp);
        return NULL;
    }
    if (!(RDB_UNIQUE & flags)) {
        RDB_raise_not_supported("RDB_UNIQUE is required", ecp);
        return NULL;
    }

    rmp = new_pg_recmap(name, envp, fieldc, fieldinfov, keyfieldc,
            flags, ecp);
    if (rmp == NULL)
        return NULL;

    RDB_init_obj(&command);
    if (RDB_string_to_obj(&command, "CREATE TABLE \"", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, name, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, "\"(", ecp) != RDB_OK)
        goto error;

    for (i = 0; i < fieldc; i++) {
        if (RDB_append_char(&command, '"', ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&command, fieldinfov[i].attrname, ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&command, "\" ", ecp) != RDB_OK)
            goto error;
        if (RDB_FTYPE_SERIAL & fieldinfov[i].flags) {
            if (RDB_append_string(&command, "serial", ecp) != RDB_OK)
                goto error;
        } else {
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
        }
        if (i < fieldc - 1) {
            if (RDB_append_char(&command, ',', ecp) != RDB_OK)
                goto error;
        }
    }
    if (keyfieldc > 0) {
        if (RDB_append_string(&command, ", PRIMARY KEY(", ecp) != RDB_OK)
            goto error;
        for (i = 0; i < keyfieldc; i++) {
            if (RDB_append_char(&command, '"', ecp) != RDB_OK)
                goto error;
            if (RDB_append_string(&command, fieldinfov[i].attrname, ecp) != RDB_OK)
                goto error;
            if (RDB_append_char(&command, '"', ecp) != RDB_OK)
                goto error;
            if (i < keyfieldc - 1) {
                if (RDB_append_char(&command, ',', ecp) != RDB_OK)
                    goto error;
            }
        }
        if (RDB_append_string(&command, ") DEFERRABLE INITIALLY IMMEDIATE", ecp)
                != RDB_OK) {
            goto error;
        }
    }
    for (i = 0; i < keyc; i++) {
        int j;
        if (RDB_append_string(&command, ", UNIQUE(", ecp) != RDB_OK)
            goto error;
        for (j = 0; j < keyv[i].strc; j++) {
            if (RDB_append_char(&command, '"', ecp) != RDB_OK)
                goto error;
            if (RDB_append_string(&command, keyv[i].strv[j], ecp) != RDB_OK)
                goto error;
            if (RDB_append_char(&command, '"', ecp) != RDB_OK)
                goto error;
            if (j < keyv[i].strc - 1) {
                if (RDB_append_char(&command, ',', ecp) != RDB_OK)
                    goto error;
            }
        }
        if (RDB_append_string(&command, ") DEFERRABLE INITIALLY IMMEDIATE", ecp)
                != RDB_OK) {
            goto error;
        }
    }
    if (fieldc == 0) {
        if (RDB_append_string(&command, "\"$dummy\" INTEGER NOT NULL DEFAULT 0 PRIMARY KEY", ecp) != RDB_OK)
            goto error;
    }
    if (RDB_append_char(&command, ')', ecp) != RDB_OK)
        goto error;

    if (RDB_env_trace(envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    res = PQexec(envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(envp, res, ecp);
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
RDB_open_pg_recmap(const char *name,
       RDB_environment *envp, int fieldc, const RDB_field_info fieldinfov[],
       int keyfieldc, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    PGresult *res;
    int resval;

    /* Check if the table exists */
    res = PQexecParams(envp->env.pgconn,
            "SELECT EXISTS(SELECT tablename FROM pg_tables WHERE tablename=$1)",
            1, NULL, &name, NULL, NULL, 1);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        RDB_pgresult_to_error(envp, res, ecp);
        PQclear(res);
        return NULL;
    }
    resval = *PQgetvalue(res, 0, 0);
    PQclear(res);
    if (!resval) {
        RDB_raise_not_found(name, ecp);
        return NULL;
    }

    return new_pg_recmap(name, envp, fieldc, fieldinfov, keyfieldc, 0, ecp);
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
    RDB_object command;
    PGresult *res;

    RDB_init_obj(&command);
    if (RDB_string_to_obj(&command, "DROP TABLE \"", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, rmp->namp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_char(&command, '"', ecp) != RDB_OK)
        goto error;

    if (RDB_env_trace(rmp->envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    res = PQexec(rmp->envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(rmp->envp, res, ecp);
        PQclear(res);
        goto error;
    }
    PQclear(res);
    RDB_free(rmp->namp);
    RDB_free(rmp->filenamp);
    RDB_free(rmp->fieldinfos);
    RDB_free(rmp);
    RDB_destroy_obj(&command, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&command, ecp);
    return RDB_ERROR;
}

static void
hton(void *p, size_t len)
{
    uint32_t v = 1;
    if ((*(uint8_t*)&v) == 1) {
        int i;
        uint8_t *cp = p;
        uint8_t h;
        for (i = 0; i < len / 2; i++) {
            h = cp[len - 1 - i];
            cp[len - 1 - i] = cp[i];
            cp[i] = h;
        }
    }
}

void
RDB_ntoh(void *dstp, const void *srcp, size_t len)
{
    uint32_t v = 1;
    if ((*(uint8_t*)&v) == 1) {
        int i;
        const uint8_t *srccp = srcp;
        uint8_t *dstcp = dstp;
        for (i = 0; i < len; i++) {
            dstcp[i] = srccp[len - 1 - i];
        }
    } else {
        memcpy(dstp, srcp, len);
    }
}

void *
RDB_field_to_pg(RDB_field *field, RDB_field_info *fieldinfo, int *formatp,
        RDB_exec_context *ecp)
{
    void *valuep = RDB_alloc(field->len, ecp);
    if (valuep == NULL)
        return NULL;

    (*field->copyfp)(valuep, field->datap, field->len);
    if (RDB_FTYPE_INTEGER & fieldinfo->flags) {
        *((uint32_t *)valuep) = htonl(*((uint32_t *)valuep));
    } else if (RDB_FTYPE_FLOAT & fieldinfo->flags) {
        hton(valuep, sizeof(RDB_float));
    }
    *formatp = RDB_FTYPE_CHAR & fieldinfo->flags ? 0 : 1;
    return valuep;
}

static int
insert_pg_rec(RDB_recmap *rmp, RDB_field flds[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_object command;
    int i;
    char parambuf[12];
    PGresult *res = NULL;
    int *lenv = NULL;
    void **valuev = NULL;
    int *formatv = NULL;
    int valuec = 0;

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

    if (RDB_string_to_obj(&command, "INSERT INTO \"", ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_append_string(&command, rmp->namp, ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_append_char(&command, '"', ecp) != RDB_OK) {
        goto error;
    }
    if (rmp->fieldcount > 0) {
        if (RDB_append_string(&command, " VALUES(", ecp) != RDB_OK) {
            goto error;
        }
        for (i = 0; i < rmp->fieldcount; i++) {
            if (RDB_FTYPE_SERIAL & rmp->fieldinfos[i].flags) {
                if (RDB_append_string(&command, "DEFAULT", ecp) != RDB_OK) {
                    goto error;
                }
            } else {
                sprintf(parambuf, "$%d", valuec + 1);
                if (RDB_append_string(&command, parambuf, ecp) != RDB_OK) {
                    goto error;
                }
            }
            if ( i < rmp->fieldcount - 1) {
                if (RDB_append_char(&command, ',', ecp) != RDB_OK) {
                    goto error;
                }
            }
            if (!(RDB_FTYPE_SERIAL & rmp->fieldinfos[i].flags)) {
                lenv[valuec] = (int) flds[i].len;
                valuev[valuec] = RDB_field_to_pg(&flds[i], &rmp->fieldinfos[i], &formatv[valuec], ecp);
                if (valuev[valuec] == NULL)
                    goto error;
                valuec++;
            }
        }
        if (RDB_append_char(&command, ')', ecp) != RDB_OK) {
            goto error;
        }
    } else {
        if (RDB_append_string(&command, " DEFAULT VALUES", ecp) != RDB_OK) {
            goto error;
        }
    }

    if (RDB_env_trace(rmp->envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    res = PQexecParams(rmp->envp->env.pgconn, RDB_obj_string(&command),
            valuec, NULL, (const char * const *) valuev, lenv,
            formatv, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(rmp->envp, res, ecp);
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
RDB_insert_pg_rec(RDB_recmap *rmp, RDB_field flds[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    /*
     * Do insert in a subtransaction, so the transaction remains valid
     * after a key violation
     */
    RDB_rec_transaction *chrtxp = RDB_pg_begin_tx(rmp->envp, rtxp, ecp);
    if (chrtxp == NULL)
        return RDB_ERROR;

    if (insert_pg_rec(rmp, flds, chrtxp, ecp) != RDB_OK) {
        RDB_pg_abort(chrtxp, ecp);
        return RDB_ERROR;
    }
    return RDB_pg_commit(chrtxp, ecp);
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
RDB_delete_pg_rec(RDB_recmap *rmp, int fieldc, RDB_field fieldv[], RDB_rec_transaction *rtxp,
        RDB_exec_context *ecp)
{
    RDB_object command;
    int i;
    char parambuf[14];
    PGresult *res = NULL;
    int *lenv = NULL;
    void **valuev = NULL;
    int *formatv = NULL;

    RDB_init_obj(&command);

    lenv = RDB_alloc(fieldc * sizeof(int), ecp);
    if (lenv == NULL)
        goto error;
    valuev = RDB_alloc(fieldc * sizeof(void*), ecp);
    if (valuev == NULL)
        goto error;
    for (i = 0; i < fieldc; i++) {
        valuev[i] = NULL;
    }
    formatv = RDB_alloc(fieldc * sizeof(int), ecp);
    if (formatv == NULL)
        goto error;

    if (RDB_string_to_obj(&command, "DELETE FROM \"", ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_append_string(&command, rmp->namp, ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_append_char(&command, '"', ecp) != RDB_OK) {
        goto error;
    }
    if (fieldc > 0) {
        if (RDB_append_string(&command, " WHERE ", ecp) != RDB_OK) {
            goto error;
        }
        for (i = 0; i < fieldc; i++) {
            if (RDB_append_string(&command, rmp->fieldinfos[i].attrname, ecp) != RDB_OK) {
                goto error;
            }
            sprintf(parambuf, "=$%d", i + 1);
            if (RDB_append_string(&command, parambuf, ecp) != RDB_OK) {
                goto error;
            }
            if (i < fieldc - 1) {
                if (RDB_append_string(&command, " AND ", ecp) != RDB_OK) {
                    goto error;
                }
            }
            lenv[i] = (int) fieldv[i].len;
            valuev[i] = RDB_field_to_pg(&fieldv[i], &rmp->fieldinfos[i], &formatv[i], ecp);
            if (valuev[i] == NULL)
                goto error;
        }
    }

    if (RDB_env_trace(rmp->envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    res = PQexecParams(rmp->envp->env.pgconn, RDB_obj_string(&command),
            fieldc, NULL, (const char * const *) valuev, lenv,
            formatv, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(rmp->envp, res, ecp);
        goto error;
    }
    if (atoi(PQcmdTuples(res)) == 0) {
        PQclear(res);
        RDB_raise_not_found("tuple not found", ecp);
        goto error;
    }
    PQclear(res);

    RDB_free(lenv);
    for (i = 0; i < fieldc; i++) {
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
        for (i = 0; i < fieldc; i++) {
            if (valuev[i] != NULL)
                RDB_free(valuev[i]);
        }
        RDB_free(valuev);
    }
    RDB_destroy_obj(&command, ecp);
    return RDB_ERROR;
}

union num {
    RDB_int i;
    RDB_float r;
};

int
RDB_get_pg_fields(RDB_recmap *rmp, RDB_field keyv[], int fieldc,
        RDB_rec_transaction *rtxp, RDB_field retfieldv[], RDB_exec_context *ecp)
{
    static PGresult *res = NULL;
    static union num *numres = NULL;
    int i;
    RDB_object command;
    char numbuf[16];
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
        if (RDB_append_char(&command, '"', ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&command,
                rmp->fieldinfos[retfieldv[i].no].attrname, ecp) != RDB_OK) {
            goto error;
        }
        if (RDB_append_char(&command, '"', ecp) != RDB_OK)
            goto error;
        if (i < fieldc - 1) {
            if (RDB_append_char(&command, ',', ecp) != RDB_OK)
                goto error;
        }
    }
    if (fieldc == 0) {
        if (RDB_append_string(&command, "*", ecp) != RDB_OK)
            goto error;
    }
    if (RDB_append_string(&command, " FROM \"", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, rmp->namp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, "\" WHERE ", ecp) != RDB_OK)
        goto error;

    for (i = 0; i < rmp->keyfieldcount; i++) {
        if (RDB_append_char(&command, '"', ecp))
            goto error;
        if (RDB_append_string(&command, rmp->fieldinfos[i].attrname, ecp))
            goto error;
        sprintf(numbuf, "\"=$%d", i + 1);
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
    if (res != NULL)
        PQclear(res);
    if (RDB_env_trace(rmp->envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    res = PQexecParams(rmp->envp->env.pgconn, RDB_obj_string(&command),
            rmp->keyfieldcount, NULL, (const char * const *) valuev, lenv,
            formatv, 1);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        RDB_pgresult_to_error(rmp->envp, res, ecp);
        PQclear(res);
        res = NULL;
        goto error;
    }
    if (PQntuples(res) == 0) {
        RDB_raise_not_found("no data", ecp);
        goto error;
    }

    /* Read integer fields and adjust byte order */
    if (numres != NULL) {
        RDB_free(numres);
    }
    numres = RDB_alloc(sizeof(union num) * fieldc, ecp);
    if (numres == NULL)
        goto error;
    for (i = 0; i < fieldc; i++) {
        retfieldv[i].datap = PQgetvalue(res, 0, i);
        retfieldv[i].len = (size_t) PQgetlength(res, 0, i);
        if (RDB_FTYPE_INTEGER & rmp->fieldinfos[retfieldv[i].no].flags) {
            numres[i].i = ntohl(*((uint32_t *)retfieldv[i].datap));
            retfieldv[i].datap = &numres[i].i;
        } else if (RDB_FTYPE_FLOAT & rmp->fieldinfos[retfieldv[i].no].flags) {
            RDB_ntoh(&numres[i], retfieldv[i].datap, sizeof(RDB_float));
            retfieldv[i].datap = &numres[i];
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
    RDB_object command;
    char parambuf[16];
    PGresult *res = NULL;
    int *lenv = NULL;
    void **valuev = NULL;
    int *formatv = NULL;
    int i;
    char *resvalp;

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

    if (RDB_string_to_obj(&command, "SELECT EXISTS(SELECT 1 FROM \"", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, rmp->namp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, "\" WHERE ", ecp) != RDB_OK)
        goto error;

    for (i = 0; i < rmp->fieldcount; i++) {
        if (RDB_append_char(&command, '"', ecp) != RDB_OK) {
            goto error;
        }
        if (RDB_append_string(&command, rmp->fieldinfos[i].attrname, ecp) != RDB_OK) {
            goto error;
        }

        sprintf(parambuf, "\"=$%d", i + 1);
        if (RDB_append_string(&command, parambuf, ecp) != RDB_OK) {
            goto error;
        }
        if (i < rmp->fieldcount - 1) {
            if (RDB_append_string(&command, " AND ", ecp) != RDB_OK) {
                goto error;
            }
        }
        lenv[i] = (int) flds[i].len;
        valuev[i] = RDB_field_to_pg(&flds[i], &rmp->fieldinfos[i], &formatv[i], ecp);
        if (valuev[i] == NULL)
            goto error;
    }
    if (rmp->fieldcount == 0) {
        if (RDB_append_string(&command, "\"$dummy\"=0", ecp) != RDB_OK) {
            goto error;
        }
    }
    if (RDB_append_char(&command, ')', ecp) != RDB_OK)
        goto error;
    if (RDB_env_trace(rmp->envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    res = PQexecParams(rmp->envp->env.pgconn, RDB_obj_string(&command),
            rmp->fieldcount, NULL, (const char * const *) valuev, lenv,
            formatv, 1);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        RDB_pgresult_to_error(rmp->envp, res, ecp);
        PQclear(res);
        goto error;
    }
    resvalp = PQgetvalue(res, 0, 0);
    if (resvalp == NULL) {
        PQclear(res);
        RDB_raise_internal("no result from SELECT EXISTS", ecp);
        goto error;
    }
    if (*resvalp == 0) {
        /* Raise not_found to indicate that the recmap does not contain the tuple */
        PQclear(res);
        RDB_raise_not_found("", ecp);
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
RDB_pg_recmap_est_size(RDB_recmap *rmp, RDB_rec_transaction *rtxp, unsigned *sz,
        RDB_exec_context *ecp)
{
    *sz = 0;
    return RDB_OK;
}
