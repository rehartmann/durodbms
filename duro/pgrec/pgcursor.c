/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "pgcursor.h"
#include "pgenv.h"
#include "pgrecmap.h"
#include <rec/cursorimpl.h>
#include <rec/envimpl.h>
#include <rec/recmapimpl.h>
#include <rec/indeximpl.h>
#include <obj/excontext.h>
#include <string.h>
#include <arpa/inet.h>

unsigned next_cur_id = 0;

/*
 * Allocate and initialize a RDB_cursor structure.
 */
static RDB_cursor *
new_pg_cursor(RDB_environment *envp, RDB_recmap *rmp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_cursor *curp;

    curp = RDB_alloc(sizeof(RDB_cursor), ecp);
    if (curp == NULL)
        return NULL;

    curp->recmapp = rmp;
    curp->envp = envp;
    curp->tx = rtxp;
    curp->destroy_fn = &RDB_destroy_pg_cursor;
    curp->get_fn = &RDB_pg_cursor_get;
    curp->first_fn = &RDB_pg_cursor_first;
    curp->next_fn = &RDB_pg_cursor_next;
    curp->prev_fn = &RDB_pg_cursor_prev;
    curp->set_fn = &RDB_pg_cursor_set;
    curp->delete_fn = &RDB_pg_cursor_delete;
    curp->seek_fn = NULL;

    curp->cur.pg.current_row = NULL;

    return curp;
}

RDB_cursor *
RDB_pg_recmap_cursor(RDB_recmap *rmp, RDB_bool wr,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_object query;
    RDB_cursor *curp;

    RDB_init_obj(&query);

    if (RDB_string_to_obj(&query, "SELECT * FROM \"", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&query, rmp->namp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_char(&query, '"', ecp) != RDB_OK)
        goto error;
    curp = RDB_pg_query_cursor(rmp->envp, RDB_obj_string(&query), wr, rtxp, ecp);
    if (curp == NULL)
        goto error;

    RDB_destroy_obj(&query, ecp);
    curp->recmapp = rmp;
    return curp;

error:
    RDB_destroy_obj(&query, ecp);
    return NULL;
}

RDB_cursor *
RDB_pg_query_cursor(RDB_environment *envp, const char *query, RDB_bool wr,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_object command;
    char idbuf[12];
    RDB_cursor *curp;
    PGresult *res;

    RDB_init_obj(&command);
    curp = new_pg_cursor(envp, NULL, rtxp, ecp);
    if (curp == NULL)
        goto error;

    curp->cur.pg.id = next_cur_id++;
    if (RDB_string_to_obj(&command, "DECLARE c", ecp) != RDB_OK)
        goto error;
    sprintf(idbuf, "%u", curp->cur.pg.id);
    if (RDB_append_string(&command, idbuf, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, " CURSOR FOR ", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, query, ecp) != RDB_OK)
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
    return curp;

error:
    if (curp != NULL) {
        RDB_free(curp);
    }
    RDB_destroy_obj(&command, ecp);
    return NULL;
}

int
RDB_destroy_pg_cursor(RDB_cursor *curp, RDB_exec_context *ecp)
{
    RDB_object command;
    char idbuf[12];
    PGresult *res;

    RDB_init_obj(&command);
    if (RDB_string_to_obj(&command, "CLOSE c", ecp) != RDB_OK)
        goto error;
    sprintf(idbuf, "%u", curp->cur.pg.id);
    if (RDB_append_string(&command, idbuf, ecp) != RDB_OK)
        goto error;

    if (RDB_env_trace(curp->envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    res = PQexec(curp->envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(curp->envp, res, ecp);
        PQclear(res);
        goto error;
    }
    PQclear(res);
    if (curp->cur.pg.current_row != NULL) {
        PQclear(curp->cur.pg.current_row);
    }
    RDB_free(curp);
    RDB_destroy_obj(&command, ecp);
    return RDB_OK;

error:
    if (curp->cur.pg.current_row != NULL) {
        PQclear(curp->cur.pg.current_row);
    }
    RDB_free(curp);
    RDB_destroy_obj(&command, ecp);
    return RDB_ERROR;
}

static int
pg_cursor_get(RDB_cursor *curp, int fno, void **datapp, size_t *lenp,
        int flags, RDB_exec_context *ecp)
{
    static union {
        RDB_int i;
        RDB_float f;
    } fieldval;

    if (curp->cur.pg.current_row == NULL) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }
    *datapp = PQgetvalue(curp->cur.pg.current_row, 0, fno);
    if (*datapp == NULL) {
        RDB_raise_not_found("no field data", ecp);
        return RDB_ERROR;
    }
    *lenp = (size_t) PQgetlength(curp->cur.pg.current_row, 0, fno);
    if (RDB_FTYPE_INTEGER & flags) {
        fieldval.i = ntohl(*((uint32_t *)*datapp));
        *datapp = &fieldval;
    } else if (RDB_FTYPE_FLOAT & flags) {
        RDB_ntoh(&fieldval, *datapp, sizeof(RDB_float));
        *datapp = &fieldval;
    }
    return RDB_OK;
}

/*
 * Read the value of field fno from the current record.
 * Character data is returned without a trailing null byte.
 */
int
RDB_pg_cursor_get(RDB_cursor *curp, int fno, void **datapp, size_t *lenp,
        RDB_exec_context *ecp)
{
    if (curp->recmapp == NULL) {
        RDB_raise_invalid_argument("access by field number only supported by recmap cursors", ecp);
        return RDB_ERROR;
    }
    return pg_cursor_get(curp, fno, datapp, lenp, curp->recmapp->fieldinfos[fno].flags, ecp);
}

int
RDB_pg_cursor_get_by_name(RDB_cursor *curp, const char *attrname, void **datapp,
        size_t *lenp, int flags, RDB_exec_context *ecp)
{
    RDB_object colname;
    int colnum;
    if (curp->cur.pg.current_row == NULL) {
        RDB_raise_not_found("", ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&colname);
    if (RDB_string_to_obj(&colname, "\"", ecp) != RDB_OK) {
        RDB_destroy_obj(&colname, ecp);
        return RDB_ERROR;
    }
    if (RDB_append_string(&colname, attrname, ecp) != RDB_OK) {
        RDB_destroy_obj(&colname, ecp);
        return RDB_ERROR;
    }
    if (RDB_append_char(&colname, '"', ecp) != RDB_OK) {
        RDB_destroy_obj(&colname, ecp);
        return RDB_ERROR;
    }
    colnum = PQfnumber(curp->cur.pg.current_row, RDB_obj_string(&colname));
    RDB_destroy_obj(&colname, ecp);
    if (colnum == -1) {
        RDB_raise_not_found(attrname, ecp);
        return RDB_ERROR;
    }
    return pg_cursor_get(curp, colnum, datapp, lenp, flags, ecp);
}

static int
exec_fetch(RDB_cursor *curp, const char *curcmd, RDB_exec_context *ecp)
{
    RDB_object command;
    char idbuf[12];
    ExecStatusType execstatus;

    RDB_init_obj(&command);
    if (RDB_string_to_obj(&command, curcmd, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, " c", ecp) != RDB_OK)
        goto error;
    sprintf(idbuf, "%u", curp->cur.pg.id);
    if (RDB_append_string(&command, idbuf, ecp) != RDB_OK)
        goto error;

    if (RDB_env_trace(curp->envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    if (curp->cur.pg.current_row != NULL)
        PQclear(curp->cur.pg.current_row);
    curp->cur.pg.current_row = PQexecParams(curp->envp->env.pgconn,
            RDB_obj_string(&command), 0, NULL, NULL, NULL, NULL, 1);
    execstatus = PQresultStatus(curp->cur.pg.current_row);
    if (execstatus != PGRES_TUPLES_OK && execstatus != PGRES_SINGLE_TUPLE) {
        RDB_pgresult_to_error(curp->envp, curp->cur.pg.current_row, ecp);
        goto error;
    }
    if (PQntuples(curp->cur.pg.current_row) == 0) {
        RDB_raise_not_found(PQerrorMessage(curp->envp->env.pgconn), ecp);
        goto error;
    }
    RDB_destroy_obj(&command, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&command, ecp);
    return RDB_ERROR;
}

/*
 * Move the cursor to the first record.
 * If there is no first record, DB_NOTFOUND is returned.
 */
int
RDB_pg_cursor_first(RDB_cursor *curp, RDB_exec_context *ecp)
{
    return exec_fetch(curp, "FETCH FIRST", ecp);
}

int
RDB_pg_cursor_next(RDB_cursor *curp, int flags, RDB_exec_context *ecp)
{
    return exec_fetch(curp, "FETCH", ecp);
}

int
RDB_pg_cursor_prev(RDB_cursor *curp, RDB_exec_context *ecp)
{
    return exec_fetch(curp, "FETCH PRIOR", ecp);
}

int
RDB_pg_cursor_set(RDB_cursor *curp, int fieldc, RDB_field fields[],
        RDB_exec_context *ecp)
{
    RDB_object command;
    char numbuf[14];
    int i;
    int *lenv = NULL;
    void **valuev = NULL;
    int *formatv = NULL;
    PGresult *res;

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

    if (RDB_string_to_obj(&command, "UPDATE \"", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, curp->recmapp->namp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, "\" SET ", ecp) != RDB_OK)
        goto error;

    for (i = 0; i < fieldc; i++) {
        if (RDB_append_char(&command, '"', ecp))
            goto error;
        if (RDB_append_string(&command,
                curp->recmapp->fieldinfos[fields[i].no].attrname, ecp)) {
            goto error;
        }
        sprintf(numbuf, "\"=$%u", i + 1);
        if (RDB_append_string(&command, numbuf, ecp) != RDB_OK)
            goto error;
        if (i < fieldc - 1) {
            if (RDB_append_char(&command, ',', ecp) != RDB_OK)
                goto error;
        }
        lenv[i] = fields[i].len;
        valuev[i] = RDB_field_to_pg(&fields[i], &curp->recmapp->fieldinfos[fields[i].no],
                &formatv[i], ecp);
        if (valuev[i] == NULL)
            goto error;
    }

    if (RDB_append_string(&command, " WHERE CURRENT OF c", ecp) != RDB_OK)
        goto error;
    sprintf(numbuf, "%u", curp->cur.pg.id);
    if (RDB_append_string(&command, numbuf, ecp) != RDB_OK)
        goto error;

    if (RDB_env_trace(curp->envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    res = PQexecParams(curp->envp->env.pgconn, RDB_obj_string(&command),
            fieldc, NULL, (const char * const *) valuev, lenv,
            formatv, 1);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(curp->envp, res, ecp);
        PQclear(res);
        goto error;
    }

    PQclear(res);
    RDB_free(lenv);
    for (i = 0; i < fieldc; i++) {
        RDB_free(valuev[i]);
    }
    RDB_free(valuev);
    RDB_free(formatv);
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

int
RDB_pg_cursor_delete(RDB_cursor *curp, RDB_exec_context *ecp)
{
    RDB_object command;
    char numbuf[14];
    PGresult *res;

    RDB_init_obj(&command);
    if (RDB_string_to_obj(&command, "DELETE FROM \"", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, curp->recmapp->namp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, "\" WHERE CURRENT OF c", ecp) != RDB_OK)
        goto error;
    sprintf(numbuf, "%u", curp->cur.pg.id);
    if (RDB_append_string(&command, numbuf, ecp) != RDB_OK)
        goto error;

    if (RDB_env_trace(curp->envp) > 0) {
        fprintf(stderr, "Sending SQL: %s\n", RDB_obj_string(&command));
    }
    res = PQexec(curp->envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_pgresult_to_error(curp->envp, res, ecp);
        PQclear(res);
        goto error;
    }
    PQclear(res);
    RDB_destroy_obj(&command, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&command, ecp);
    return RDB_ERROR;
}
