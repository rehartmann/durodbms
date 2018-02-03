/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "pgcursor.h"
#include "pgrecmap.h"
#include <rec/cursorimpl.h>
#include <rec/envimpl.h>
#include <rec/recmapimpl.h>
#include <rec/indeximpl.h>
#include <obj/excontext.h>
#include <string.h>
#include <arpa/inet.h>

int last_cur_id = 0;

/*
 * Allocate and initialize a RDB_cursor structure.
 */
static RDB_cursor *
new_pg_cursor(RDB_recmap *rmp, RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_cursor *curp;

    curp = RDB_alloc(sizeof(RDB_cursor), ecp);
    if (curp == NULL)
        return NULL;

    curp->recmapp = rmp;
    curp->tx = rtxp;
    curp->destroy_fn = &RDB_destroy_pg_cursor;
    curp->get_fn = &RDB_pg_cursor_get;
    curp->first_fn = &RDB_pg_cursor_first;
    curp->next_fn = &RDB_pg_cursor_next;
    curp->prev_fn = &RDB_pg_cursor_prev;
    curp->set_fn = &RDB_pg_cursor_set;
    curp->delete_fn = &RDB_pg_cursor_delete;

    curp->cur.pg.current_row = NULL;

    return curp;
}

RDB_cursor *
RDB_pg_recmap_cursor(RDB_recmap *rmp, RDB_bool wr,
        RDB_rec_transaction *rtxp, RDB_exec_context *ecp)
{
    RDB_object command;
    char idbuf[12];
    RDB_cursor *curp;
    PGresult *res;

    RDB_init_obj(&command);
    curp = new_pg_cursor(rmp, rtxp, ecp);
    if (curp == NULL)
        goto error;

    curp->cur.pg.id = last_cur_id++;
    if (RDB_string_to_obj(&command, "DECLARE c", ecp) != RDB_OK)
        goto error;
    sprintf(idbuf, "%d", curp->cur.pg.id);
    if (RDB_append_string(&command, idbuf, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, " CURSOR FOR SELECT * FROM ", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, rmp->namp, ecp) != RDB_OK)
        goto error;

    res = PQexec(rmp->envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_raise_system(PQerrorMessage(rmp->envp->env.pgconn), ecp);
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
    sprintf(idbuf, "%d", curp->cur.pg.id);
    if (RDB_append_string(&command, idbuf, ecp) != RDB_OK)
        goto error;

    res = PQexec(curp->recmapp->envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_raise_system(PQerrorMessage(curp->recmapp->envp->env.pgconn), ecp);
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

/*
 * Read the value of field fno from the current record.
 * Character data is returned without a trailing null byte.
 */
int
RDB_pg_cursor_get(RDB_cursor *curp, int fno, void **datapp, size_t *lenp,
        RDB_exec_context *ecp)
{
    static RDB_int fieldval;

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
    if (RDB_FTYPE_INTEGER & curp->recmapp->fieldinfos[fno].flags) {
        fieldval = ntohl(*((uint32_t *)*datapp));
        *datapp = &fieldval;
    }
    return RDB_OK;
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
    sprintf(idbuf, "%d", curp->cur.pg.id);
    if (RDB_append_string(&command, idbuf, ecp) != RDB_OK)
        goto error;

    if (curp->cur.pg.current_row != NULL)
        PQclear(curp->cur.pg.current_row);
    curp->cur.pg.current_row = PQexecParams(curp->recmapp->envp->env.pgconn,
            RDB_obj_string(&command), 0, NULL, NULL, NULL, NULL, 1);
    execstatus = PQresultStatus(curp->cur.pg.current_row);
    if (execstatus != PGRES_TUPLES_OK && execstatus != PGRES_SINGLE_TUPLE) {
        RDB_raise_system(PQerrorMessage(curp->recmapp->envp->env.pgconn), ecp);
        goto error;
    }
    if (PQntuples(curp->cur.pg.current_row) == 0) {
        RDB_raise_not_found(PQerrorMessage(curp->recmapp->envp->env.pgconn), ecp);
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

    if (RDB_string_to_obj(&command, "UPDATE ", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, curp->recmapp->namp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, " SET ", ecp) != RDB_OK)
        goto error;

    for (i = 0; i < fieldc; i++) {
        if (RDB_append_string(&command, "d_", ecp))
            goto error;
        if (RDB_append_string(&command,
                curp->recmapp->fieldinfos[fields[i].no].attrname, ecp)) {
            goto error;
        }
        sprintf(numbuf, "=$%d", i + 1);
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
    sprintf(numbuf, "%d", curp->cur.pg.id);
    if (RDB_append_string(&command, numbuf, ecp) != RDB_OK)
        goto error;

    res = PQexecParams(curp->recmapp->envp->env.pgconn, RDB_obj_string(&command),
            fieldc, NULL, (const char * const *) valuev, lenv,
            formatv, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_raise_system(PQerrorMessage(curp->recmapp->envp->env.pgconn), ecp);
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

    if (RDB_string_to_obj(&command, "DELETE FROM ", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, curp->recmapp->namp, ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&command, " WHERE CURRENT OF c", ecp) != RDB_OK)
        goto error;
    sprintf(numbuf, "%d", curp->cur.pg.id);
    if (RDB_append_string(&command, numbuf, ecp) != RDB_OK)
        goto error;

    res = PQexec(curp->recmapp->envp->env.pgconn, RDB_obj_string(&command));
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        RDB_raise_system(PQerrorMessage(curp->recmapp->envp->env.pgconn), ecp);
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
