/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "stable.h"
#include "typeimpl.h"
#include "serialize.h"
#include "catalog.h"
#include <gen/hashmapit.h>
#include <gen/hashtabit.h>
#include <gen/strfns.h>
#include <string.h>
#include <ctype.h>

/** @defgroup db Database functions
 * \#include <rel/rdb.h>
 * @{
 */

/**
 * Return a pointer to the name of the database
specified by <var>dbp</var>.

@returns

The name of the database.
*/
char *
RDB_db_name(RDB_database *dbp)
{
    return dbp->name;
}

/**
RDB_db_env returns a pointer to the database environment of the database
specified by <var>dbp</var>.

@returns

A pointer to the database environment.
*/
RDB_environment *
RDB_db_env(RDB_database *dbp)
{
    return dbp->dbrootp->envp;
}

static void
free_dbroot(RDB_dbroot *dbrootp, RDB_exec_context *ecp)
{
    RDB_constraint *constrp, *nextconstrp;

    RDB_destroy_hashmap(&dbrootp->typemap);

    /*
     * Destroy constraints
     */
    constrp = dbrootp->first_constrp;
    while (constrp != NULL) {
        nextconstrp = constrp->nextp;
        RDB_free(constrp->name);
        RDB_del_expr(constrp->exp, ecp);
        RDB_free(constrp);
        constrp = nextconstrp;
    }

    RDB_destroy_op_map(&dbrootp->ro_opmap);

    RDB_destroy_op_map(&dbrootp->upd_opmap);

    RDB_free(dbrootp);
}

static int
close_table(RDB_object *tbp, RDB_environment *envp, RDB_exec_context *ecp)
{
    RDB_dbroot *dbrootp;
    RDB_database *dbp;

    if (tbp->val.tb.stp != NULL) {
        if (RDB_close_stored_table(tbp->val.tb.stp, ecp) != RDB_OK)
            return RDB_ERROR;
        tbp->val.tb.stp = NULL;
    }

    /*
     * Remove table from all RDB_databases in list
     */
    dbrootp = (RDB_dbroot *) RDB_env_xdata(envp);
    for (dbp = dbrootp->first_dbp; dbp != NULL; dbp = dbp->nextdbp) {
        RDB_object *foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->val.tb.name);
        if (foundtbp != NULL) {
            RDB_hashmap_put(&dbp->tbmap, tbp->val.tb.name, NULL);
        }
    }

    return RDB_free_obj(tbp, ecp);
}

static int
rm_db(RDB_database *dbp, RDB_exec_context *ecp)
{
    /* Remove database from list */
    if (dbp->dbrootp->first_dbp == dbp) {
        dbp->dbrootp->first_dbp = dbp->nextdbp;
    } else {
        RDB_database *hdbp = dbp->dbrootp->first_dbp;
        while (hdbp != NULL && hdbp->nextdbp != dbp) {
            hdbp = hdbp->nextdbp;
        }
        if (hdbp == NULL) {
            RDB_raise_invalid_argument("invalid database", ecp);
            return RDB_ERROR;
        }
        hdbp->nextdbp = dbp->nextdbp;
    }
    return RDB_OK;
}

static void
free_db(RDB_database *dbp)
{
    RDB_destroy_hashmap(&dbp->tbmap);
    RDB_free(dbp->name);
    RDB_free(dbp);
}

/* Check if there is a table this table depends on */
static RDB_bool
table_refs(RDB_database *dbp, RDB_object *tbp)
{
    RDB_hashmap_iter it;
    RDB_object *rtbp;
    char *keyp;
    void *datap;

    RDB_init_hashmap_iter(&it, &dbp->tbmap);

    do {
        datap = RDB_hashmap_next(&it, &keyp);
        if (keyp == NULL) {
            RDB_destroy_hashmap_iter(&it);
            return RDB_FALSE;
        }

        rtbp = datap != NULL ? (RDB_object *) datap : NULL;
    } while (rtbp == NULL || (tbp == rtbp) || !RDB_table_refers(rtbp, tbp));
    RDB_destroy_hashmap_iter(&it);
    return RDB_TRUE;
}

/* Get a user table which no other table depends on */
static RDB_object *
find_del_table(RDB_database *dbp)
{
    RDB_hashmap_iter it;
    RDB_object *tbp;
    void *datap;
    char *keyp;

    RDB_init_hashmap_iter(&it, &dbp->tbmap);

    do {
        datap = RDB_hashmap_next(&it, &keyp);
        if (keyp == NULL) {
            RDB_destroy_hashmap_iter(&it);
            return NULL;
        }

        tbp = (RDB_object *) datap;
    } while (tbp == NULL || !tbp->val.tb.is_user || table_refs(dbp, tbp));
    RDB_destroy_hashmap_iter(&it);
    return tbp;
}

/*
 * Close all user tables
 */
static int
RDB_close_user_tables(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    
    while ((tbp = find_del_table(dbp)) != NULL) {
        if (close_table(tbp, dbp->dbrootp->envp, ecp) != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}    

static int
release_db(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;

    ret = RDB_close_user_tables(dbp, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    ret = rm_db(dbp, ecp);
    if (ret != RDB_OK)
        return ret;
    
    free_db(dbp);
    return ret;
}

static void
close_systables(RDB_dbroot *dbrootp, RDB_exec_context *ecp)
{
    close_table(dbrootp->rtables_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->table_attr_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->table_attr_defvals_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->vtables_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->table_recmap_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->dbtables_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->keys_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->types_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->possrepcomps_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->ro_ops_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->upd_ops_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->indexes_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->constraints_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->version_info_tbp, dbrootp->envp, ecp);
}

/* cleanup function to close all DBs and tables */
static void
cleanup_env(RDB_environment *envp)
{
    RDB_dbroot *dbrootp = (RDB_dbroot *) RDB_env_xdata(envp);
    RDB_database *dbp;
    RDB_database *nextdbp;
    RDB_exec_context ec;

    if (dbrootp == NULL)
        return;

    RDB_init_exec_context(&ec);

    dbp = dbrootp->first_dbp;

    while (dbp != NULL) {
        nextdbp = dbp->nextdbp;
        release_db(dbp, &ec);
        dbp = nextdbp;
    }
    close_systables(dbrootp, &ec);
    free_dbroot(dbrootp, &ec);

    RDB_destroy_exec_context(&ec);

    lt_dlexit();
}

static RDB_dbroot *
new_dbroot(RDB_environment *envp, RDB_exec_context *ecp)
{
    RDB_dbroot *dbrootp = RDB_alloc(sizeof (RDB_dbroot), ecp);

    if (dbrootp == NULL) {
        return NULL;
    }
    
    dbrootp->envp = envp;
    RDB_init_hashmap(&dbrootp->typemap, RDB_DFL_MAP_CAPACITY);
    RDB_init_op_map(&dbrootp->ro_opmap);
    RDB_init_op_map(&dbrootp->upd_opmap);

    dbrootp->first_dbp = NULL;
    dbrootp->first_constrp = NULL;
    dbrootp->constraints_read = RDB_FALSE;

    return dbrootp;
}

/*
 * Allocate and initialize the RDB_database structure
 */
static RDB_database *
new_db(const char *name, RDB_exec_context *ecp)
{
    RDB_database *dbp;

    /* Allocate structure */
    dbp = RDB_alloc(sizeof (RDB_database), ecp);
    if (dbp == NULL)
        return NULL;

    /* Set name */
    dbp->name = RDB_dup_str(name);
    if (dbp->name == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    /* Initialize structure */

    RDB_init_hashmap(&dbp->tbmap, RDB_DFL_MAP_CAPACITY);

    return dbp;

error:
    RDB_free(dbp->name);
    RDB_free(dbp);
    return NULL;
}

static int
assoc_systables(RDB_dbroot *dbrootp, RDB_database *dbp, RDB_exec_context *ecp)
{
    if (RDB_assoc_table_db(dbrootp->table_attr_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->table_attr_defvals_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->rtables_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->vtables_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->table_recmap_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->dbtables_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->keys_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->types_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->possrepcomps_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->ro_ops_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->upd_ops_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->indexes_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->constraints_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->version_info_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_OK;
}

/*
 * Create database from dbroot
 */
static RDB_database *
create_db(const char *name, RDB_dbroot *dbrootp,
                       RDB_exec_context *ecp)
{
    RDB_transaction tx;
    int ret;
    RDB_database *dbp = new_db(name, ecp);
    if (dbp == NULL) {
        return NULL;
    }

    /*
     * Create DB in catalog
     */

    dbp->dbrootp = dbrootp;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_cat_create_db(ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        goto error;
    }

    ret = RDB_commit(ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    if (assoc_systables(dbrootp, dbp, ecp) != RDB_OK)
        goto error;

    /* Insert database into list */
    dbp->nextdbp = dbrootp->first_dbp;
    dbrootp->first_dbp = dbp;

    return dbp;

error:
    free_db(dbp);
    return NULL;
}

static RDB_database *
get_db(const char *name, RDB_dbroot *dbrootp, RDB_exec_context *ecp)
{
    RDB_database *dbp = NULL;
    RDB_transaction tx;
    RDB_object tpl;
    RDB_bool b;
    int ret;

    /* Search the DB list for the database */
    for (dbp = dbrootp->first_dbp; dbp != NULL; dbp = dbp->nextdbp) {
        if (strcmp(dbp->name, name) == 0) {
            return dbp;
        }
    }

    /*
     * Not found, read database from catalog
     */

    tx.dbp = NULL;
    ret = RDB_begin_tx_env(ecp, &tx, dbrootp->envp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    /*
     * Check if the database exists by checking if the DBTABLES contains
     * sys_rtables for this database.
     */

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "tablename", "sys_rtables", ecp);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        goto error;
    }
    ret = RDB_tuple_set_string(&tpl, "dbname", name, ecp);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        goto error;
    }

    dbp = new_db(name, ecp);
    if (dbp == NULL) {
        goto error;
    }

    ret = RDB_table_contains(dbrootp->dbtables_tbp, &tpl, ecp, &tx, &b);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        RDB_rollback(ecp, &tx);
        goto error;
    }
    if (!b) {
        RDB_destroy_obj(&tpl, ecp);
        RDB_rollback(ecp, &tx);
        RDB_raise_not_found("database not found", ecp);
        goto error;
    }
    RDB_destroy_obj(&tpl, ecp);

    ret = RDB_commit(ecp, &tx);
    if (ret != RDB_OK)
        return NULL;

    if (assoc_systables(dbrootp, dbp, ecp) != RDB_OK)
        goto error;
    dbp->dbrootp = dbrootp;

    /* Insert database into list */
    dbp->nextdbp = dbrootp->first_dbp;
    dbrootp->first_dbp = dbp;

    return dbp;

error:
    if (dbp != NULL) {
        free_db(dbp);
    }
    return NULL;
}

/*
 * Create and initialize RDB_dbroot structure.
 */
static RDB_dbroot *
create_dbroot(RDB_environment *envp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_dbroot *dbrootp;
    RDB_database sdb; /* Incomplete database, needed for built-in ops */
    int ret;

    /* Set cleanup function */
    RDB_set_env_closefn(envp, cleanup_env);

    dbrootp = new_dbroot(envp, ecp);
    if (dbrootp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    ret = RDB_begin_tx_env(ecp, &tx, envp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    sdb.name = NULL;
    tx.dbp = &sdb;
    sdb.dbrootp = dbrootp;
    ret = RDB_open_systables(dbrootp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        goto error;
    }

    ret = RDB_commit(ecp, &tx);
    if (ret != RDB_OK)
        goto error;

    RDB_env_set_xdata(envp, dbrootp);

    return dbrootp;

error:
    free_dbroot(dbrootp, ecp);
    return NULL;
}

static RDB_dbroot *
get_dbroot(RDB_environment *envp, RDB_exec_context *ecp)
{
    RDB_dbroot *dbrootp = (RDB_dbroot *) RDB_env_xdata(envp);

    RDB_bool crdbroot = RDB_FALSE;

    if (dbrootp == NULL) {
        /*
         * No dbroot found, initialize builtin types and libltdl
         * and create RDB_dbroot structure
         */

        if (RDB_init_builtin_types(ecp) != RDB_OK) {
            goto error;
        }
        if (lt_dlinit() != 0) {
            RDB_raise_system("lt_dlinit() failed.", ecp);
            return NULL;
        }
        dbrootp = create_dbroot(envp, ecp);
        if (dbrootp == NULL) {
            goto error;
        }
        crdbroot = RDB_TRUE;
    }
    return dbrootp;

error:
    if (crdbroot) {
        close_systables(dbrootp, ecp);
        free_dbroot(dbrootp, ecp);
        RDB_env_set_xdata(envp, NULL);
        lt_dlexit();
    }

    return NULL;
}

/**
RDB_create_db_from_env creates a database from a database environment.
If an error occurs, an error value is left in *<var>ecp</var>.

@returns

A pointer to the newly created database, or NULL if an error occurred.

@par Errors:

<dl>
<dt>element_exists_error
<dd>A database with the name <var>name</var> already exixts.
<dt>version_mismatch_error
<dd>The version number stored in the catalog does not match
the version of the library.
</dl>

The call may also fail for a @ref system-errors "system error".
*/
RDB_database *
RDB_create_db_from_env(const char *name, RDB_environment *envp,
                       RDB_exec_context *ecp)
{
    RDB_dbroot *dbrootp;

    if (!RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid database name", ecp);
        return NULL;
    }

    /* Get dbroot, create it if it does not exist */
    dbrootp = get_dbroot(envp, ecp);
    if (dbrootp == NULL)
        return NULL;

    return create_db(name, dbrootp, ecp);
}

/**
RDB_get_db_from_env obtains a pointer to the database with name
<var>name</var> in the environment specified by <var>envp</var>.
If an error occurs, an error value is left in *<var>ecp</var>.

@returns

On success, a pointer to the database is returned. If an error occurred, NULL is
returned.

@par Errors:

<dl>
<dt>not_found_error
<dd>A database with the name <var>name</var> could not be found.
<dt>version_mismatch_error
<dd>The version number stored in the catalog does not match
the version of the library.
</dl>

The call may also fail for a @ref system-errors "system error".
*/
RDB_database *
RDB_get_db_from_env(const char *name, RDB_environment *envp,
                    RDB_exec_context *ecp)
{
    /* Get dbroot from env */
    RDB_dbroot *dbrootp = get_dbroot(envp, ecp);
    if (dbrootp == NULL)
        return NULL;

    return get_db(name, dbrootp, ecp);
}

static RDB_object *
user_tables_vt(RDB_database *dbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *vtbp;
    RDB_expression *ex1p = NULL;
    RDB_expression *ex2p = NULL;
    RDB_expression *ex3p = NULL;
    RDB_expression *ex4p = NULL;

    ex1p = RDB_table_ref(dbp->dbrootp->rtables_tbp, ecp);
    if (ex1p == NULL)
        goto error;
    ex2p = RDB_var_ref("is_user", ecp);
    if (ex2p == NULL) {
        goto error;
    }
    ex3p = RDB_ro_op("where", ecp);
    if (ex3p == NULL) {
        goto error;
    }
    RDB_add_arg(ex3p, ex1p);
    RDB_add_arg(ex3p, ex2p);

    ex2p = NULL;
    ex1p = RDB_var_ref("dbname", ecp);
    if (ex1p == NULL) {
        goto error;
    }
    ex2p = RDB_string_to_expr(dbp->name, ecp);
    if (ex2p == NULL) {
        goto error;
    }
    ex4p = RDB_eq(ex1p, ex2p, ecp);
    if (ex4p == NULL) {
        goto error;
    }
    ex2p = NULL;
    ex1p = RDB_table_ref(dbp->dbrootp->dbtables_tbp, ecp);
    if (ex1p == NULL)
        goto error;
    ex2p = RDB_ro_op("where", ecp);
    if (ex2p == NULL)
        goto error;
    RDB_add_arg(ex2p, ex1p);
    RDB_add_arg(ex2p, ex4p);
    ex4p = NULL;
    ex1p = RDB_ro_op("join", ecp);
    if (ex1p == NULL)
        goto error;
    RDB_add_arg(ex1p, ex2p);
    RDB_add_arg(ex1p, ex3p);
    ex2p = NULL;
    ex3p = NULL;

    vtbp = RDB_expr_to_vtable(ex1p, ecp, txp);
    if (vtbp == NULL)
        goto error;
    return vtbp;

error:
    if (ex1p != NULL)
        RDB_del_expr(ex1p, ecp);
    if (ex2p != NULL)
        RDB_del_expr(ex2p, ecp);
    if (ex3p != NULL)
        RDB_del_expr(ex3p, ecp);
    if (ex4p != NULL)
        RDB_del_expr(ex4p, ecp);
    return NULL;
}

static RDB_object *
db_exists_vt(RDB_database *dbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
	RDB_object *vtbp;
	RDB_expression *ex1p = NULL;
	RDB_expression *ex2p = NULL;
	RDB_expression *ex3p = NULL;
	
	ex1p = RDB_var_ref("dbname", ecp);
	if (ex1p == NULL)
	    return NULL;

    ex2p = RDB_string_to_expr(dbp->name, ecp);
    if (ex2p == NULL)
        goto error;

    ex3p = RDB_eq(ex1p, ex2p, ecp);
    if (ex3p == NULL)
        goto error;

    ex2p = NULL;
    ex1p = RDB_table_ref(dbp->dbrootp->dbtables_tbp, ecp);
    if (ex1p == NULL)
        goto error;

    ex2p = RDB_ro_op("where", ecp);
    if (ex2p == NULL)
        goto error;
    RDB_add_arg(ex2p, ex1p);
    RDB_add_arg(ex2p, ex3p);
    ex1p = NULL;
    ex3p = NULL;

    vtbp = RDB_expr_to_vtable(ex2p, ecp, txp);
    if (vtbp == NULL)
        goto error;
    return vtbp;

error:
    if (ex1p == NULL)
        RDB_del_expr(ex1p, ecp);
    if (ex2p == NULL)
        RDB_del_expr(ex2p, ecp);
    if (ex3p == NULL)
        RDB_del_expr(ex3p, ecp);
    return NULL;
}

/**
RDB_drop_db deletes the database specified by <var>dbp</var>.
The database must be empty.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>not_found_error
<dd>The database was not found.
<dt>element_exists_error
<dd>The database is not empty.
</dl>

The call may also fail for a @ref system-errors "system error".
*/
int
RDB_drop_db(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tx;
    RDB_object *vtbp;
    RDB_bool empty;
    RDB_expression *exp;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK)
        return ret;

    /*
     * Check if the database contains user tables
     */
    vtbp = user_tables_vt(dbp, ecp, &tx);
    if (vtbp == NULL)
        goto error;
    
    ret = RDB_table_is_empty(vtbp, ecp, &tx, &empty);
    RDB_drop_table(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }
    if (!empty) {
        RDB_raise_element_exists("database is not empty", ecp);
        goto error;
    }

    /*
     * Check if the database exists
     */
    vtbp = db_exists_vt(dbp, ecp, &tx);
    if (vtbp == NULL)
        goto error;

    ret = RDB_table_is_empty(vtbp, ecp, &tx, &empty);
    RDB_drop_table(vtbp, ecp, &tx);
    if (ret != RDB_OK)
        goto error;
    if (empty) {
        RDB_raise_not_found("database not found", ecp);
        goto error;
    }

    /*
     * Disassociate all tables from database
     */
    exp = RDB_eq(RDB_var_ref("dbname", ecp),
                  RDB_string_to_expr(dbp->name, ecp), ecp);
    if (exp == NULL) {
        goto error;
    }

    ret = RDB_delete(dbp->dbrootp->dbtables_tbp, exp, ecp, &tx);
    if (ret == RDB_ERROR) {
        goto error;
    }

    ret = RDB_commit(ecp, &tx);
    if (ret != RDB_OK)
        return ret;

    return release_db(dbp, ecp);

error:
    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
}

static RDB_object *
db_names_tb(RDB_object *dbtables_tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *ex1p = NULL;
    RDB_expression *ex2p = NULL;
    RDB_expression *ex3p = NULL;
    RDB_object *vtbp;

    ex1p = RDB_table_ref(dbtables_tbp, ecp);
    if (ex1p == NULL)
        return NULL;
    ex2p = RDB_string_to_expr("dbname", ecp);
    if (ex2p == NULL)
        return NULL;

    ex3p = RDB_ro_op("project", ecp);
    if (ex3p == NULL)
        goto error;
    RDB_add_arg(ex3p, ex1p);
    RDB_add_arg(ex3p, ex2p);
    ex1p = NULL;
    ex2p = NULL;
    vtbp = RDB_expr_to_vtable(ex3p, ecp, txp);
    if (vtbp == NULL)
        goto error;
    return vtbp;

error:
    if (ex1p == NULL)
        RDB_del_expr(ex1p, ecp);
    if (ex2p == NULL)
        RDB_del_expr(ex2p, ecp);
    if (ex3p == NULL)
        RDB_del_expr(ex3p, ecp);
    return NULL;
}

/**
After RDB_get_dbs has been called successfully, *arrp is
an array of strings which contains the names of all databases in *envp.

*arrp must either already be an array of RDB_STRING or having been
initialized using RDB_init_obj().

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>type_mismatch_error
<dd>*<var>arrp</var> already contains elements of a type different from
RDB_STRING.
</dl>

The call may fail for a @ref system-errors "system error".
*/
int
RDB_get_dbs(RDB_environment *envp, RDB_object *arrp, RDB_exec_context *ecp)
{
    int ret;
    int i;
    RDB_int len;
    RDB_object *vtbp;
    RDB_object resarr;
    RDB_transaction tx;
    RDB_dbroot *dbrootp = get_dbroot(envp, ecp); /* Get dbroot */
    if (dbrootp == NULL) {
        return RDB_ERROR;
    }

    ret = RDB_begin_tx_env(ecp, &tx, envp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }
    tx.dbp = NULL;

    vtbp = db_names_tb(dbrootp->dbtables_tbp, ecp, &tx);
    if (vtbp == NULL) {
        RDB_rollback(ecp, &tx);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&resarr);

    ret = RDB_table_to_array(&resarr, vtbp, 0, NULL, 0, ecp, &tx);
    if (ret != RDB_OK)
        goto cleanup;

    len = RDB_array_length(&resarr, ecp);
    if (len < 0) {
        ret = len;
        goto cleanup;
    }

    ret = RDB_set_array_length(arrp, len, ecp);
    if (ret != RDB_OK)
        goto cleanup;

    for (i = 0; i < len; i++) {
        RDB_object *tplp;

        tplp = RDB_array_get(&resarr, (RDB_int) i, ecp);
        if (tplp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }

        ret = RDB_array_set(arrp, (RDB_int) i, RDB_tuple_get(tplp, "dbname"),
                ecp);
        if (ret != RDB_OK)
            goto cleanup;
    }

cleanup:
    RDB_drop_table(vtbp, ecp, &tx);
    RDB_destroy_obj(&resarr, ecp);

    if (ret == RDB_OK)
        return RDB_commit(ecp, &tx);

    RDB_rollback(ecp, &tx);
    return RDB_ERROR;
}

/*@}*/

/**
 * Associate a RDB_object structure with a RDB_database structure.
 */
int
RDB_assoc_table_db(RDB_object *tbp, RDB_database *dbp, RDB_exec_context *ecp)
{
    /* Insert table into table map */
    int ret = RDB_hashmap_put(&dbp->tbmap, tbp->val.tb.name, tbp);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, NULL);
        return RDB_ERROR;
    }
    return RDB_OK;
}

/**
 * Check if the name is legal
 */
RDB_bool
RDB_legal_name(const char *name)
{
    int i;

    if (*name == '\0')
        return RDB_FALSE;

    for (i = 0; name[i] != '\0'; i++) {
        if (!isprint(name[i]) || isspace(name[i]) || (name[i] == '$'))
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

static RDB_object *
create_table(const char *name, RDB_type *reltyp,
                int keyc, const RDB_string_vec keyv[],
                int default_attrc, const RDB_attr *default_attrv,
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *tbp;
    RDB_transaction tx;

    tbp = RDB_new_rtable(name, RDB_TRUE, reltyp, keyc, keyv,
            default_attrc, default_attrv, RDB_TRUE, ecp);
    if (tbp == NULL) {
        return NULL;
    }

    /* Create subtransaction */
    if (RDB_begin_tx(ecp, &tx, txp->dbp, txp) != RDB_OK)
        return NULL;

    /* Insert table into catalog */
    if (RDB_cat_insert(tbp, ecp, &tx) != RDB_OK) {
        /* Don't destroy type */
        tbp->typ = NULL;
        RDB_rollback(ecp, &tx);
        RDB_free_obj(tbp, ecp);
        return NULL;
    }

    if (RDB_assoc_table_db(tbp, txp->dbp, ecp) != RDB_OK) {
        RDB_rollback(ecp, &tx);
        RDB_free_obj(tbp, ecp);
        return NULL;
    }

    if (RDB_commit(ecp, &tx) != RDB_OK) {
         RDB_hashmap_put(&txp->dbp->tbmap, name, NULL);
        RDB_free_obj(tbp, ecp);
        return NULL;
    }
   
    return tbp;
}

/** @defgroup table Table functions 
 * @{
 */

/** @struct RDB_attr rdb.h <rel/rdb.h>
 * This struct is used to specify attribute definitions.
 */

/** @struct RDB_string_vec rdb.h <rel/rdb.h>
 * This struct is used to specify attribute definitions.
 */

/**
<strong>RDB_create_table</strong> creates a persistent table
with name <var>name</var> in the database
the transaction *<var>txp</var> interacts with
and returns a pointer to the newly created RDB_object structure
which represents the table.

If an error occurs, an error value is left in *<var>ecp</var>.

The table will have <var>attrc</var> attributes. The individual
attributes are specified by the elements of <var>attrv</var>.
<var>options</var> is currently ignored, but should be set to zero
for compatibility with future versions.

The candidate keys for the table are specified by <var>keyc</var>
and <var>keyv</var>.

A candidate key must not be a subset of another.
If a single candidate key is specified, that key
may be empty (not contain any attributes).

The database *<var>txp</var> interacts with must be a user database.

Passing a <var>keyv</var> of NULL is equivalent to specifiying a single key
which consists of all attributes, that is, the table will be all-key.

To enforce the key constraints, DuroDBMS creates a unique hash index
for each key.

@returns

On success, a pointer to the newly created table is returned.
If an error occurred, NULL is returned.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>type_mismatch_error
<dd>The type of a default value does not match the type of the corresponding
attribute.
<dt>invalid_argument_error
<dd>One or more of the arguments are incorrect. For example, a key attribute
does not appear in <var>attrv</var>, etc.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
RDB_object *
RDB_create_table(const char *name,
                int attrc, const RDB_attr attrv[],
                int keyc, const RDB_string_vec keyv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *tbtyp = RDB_new_relation_type(attrc, attrv, ecp);
    if (tbtyp == NULL) {
        return NULL;
    }

    return RDB_create_table_from_type(name, tbtyp, keyc, keyv,
            attrc, attrv, ecp, txp);
}

/**
<strong>RDB_create_table_from_type</strong> acts like
RDB_create_table(), except that it takes
a RDB_type argument instead of attribute arguments.

<var>reltyp</var> must be a relation type and will be managed
by the table created.

If <var>default_attrc</var> is greater than zero,
<var>default_attrv</var> must point to an array of length <var>default_attrc</var>
where <var>name</var> is the attribute name and <var>defaultp</var>
points to the default value for that attribute.
Entries with a <var>defaultp</var> of NULL are ignored.
Other fields of RDB_attr are ignored, but <var>options</var> should be set to zero
for compatibility with future versions.

*/
RDB_object *
RDB_create_table_from_type(const char *name, RDB_type *reltyp,
        int keyc, const RDB_string_vec keyv[],
        int default_attrc, const RDB_attr default_attrv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;

    if (name != NULL && !RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid table name", ecp);
        return NULL;
    }

    if (reltyp->kind != RDB_TP_RELATION) {
        RDB_raise_type_mismatch("relation type required", ecp);
        return NULL;
    }

    for (i = 0; i < reltyp->def.basetyp->def.tuple.attrc; i++) {
        if (!RDB_legal_name(reltyp->def.basetyp->def.tuple.attrv[i].name)) {
            RDB_object str;
        
            RDB_init_obj(&str);
            if (RDB_string_to_obj(&str, "invalid attribute name: ", ecp)
                    != RDB_OK) {
                RDB_destroy_obj(&str, ecp);
                return NULL;
            }

            if (RDB_append_string(&str,
                    reltyp->def.basetyp->def.tuple.attrv[i].name, ecp) != RDB_OK) {
                RDB_destroy_obj(&str, ecp);
                return NULL;
            }

            RDB_raise_invalid_argument(RDB_obj_string(&str), ecp);
            RDB_destroy_obj(&str, ecp);            
            return NULL;
        }
    }

    /* name may only be NULL if table is transient */
    if ((name == NULL)) {
        RDB_raise_invalid_argument("persistent table must have a name", ecp);
        return NULL;
    }

    return create_table(name, reltyp, keyc, keyv, default_attrc, default_attrv,
            ecp, txp);
}

/**
RDB_get_table looks up the global table with name <var>name</var>
in the environment of the database the transaction
specified by <var>txp</var> interacts with and returns a pointer to it.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

A pointer to the table, or NULL if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>name_error
<dd>A table with the name <var>name</var> could not be found.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
RDB_object *
RDB_get_table(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *tbp;
    RDB_database *dbp;
    RDB_object *errp;

    /* Search table in all databases */
    dbp = txp->dbp->dbrootp->first_dbp;
    while (dbp != NULL) {
        tbp = RDB_hashmap_get(&dbp->tbmap, name);
        if (tbp != NULL) {
            /* Found */
            return tbp;
        }
        dbp = dbp->nextdbp;
    }

    if (RDB_env_trace(RDB_db_env(RDB_tx_db(txp))) > 0) {
        fprintf(stderr,
                "Trying to read table %s from the catalog\n",
                name);
    }

    /* If not found, read from catalog, search in real tables first */
    tbp = RDB_cat_get_rtable(name, ecp, txp);
    if (tbp != NULL)
        return tbp;
    errp = RDB_get_err(ecp);
    if (errp != NULL) {
        if (RDB_obj_type(errp) != &RDB_NOT_FOUND_ERROR)
            return NULL;
        RDB_clear_err(ecp);
    }

    tbp = RDB_cat_get_vtable(name, ecp, txp);

    if (tbp == NULL && RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        /* Replace not_found_error with name_error */
        RDB_raise_name(name, ecp);
    }
    return tbp;
}

/*
 * Check if there is a virtual table that depends on this table.
 * Raise in_use_error if such a table exists.
 */
static int
table_dep_check(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_int len;
    RDB_object tbarr;
    RDB_object *vtbp;
    RDB_object *dtbp;
    RDB_expression *wherep;
    RDB_expression *ex2p;
    RDB_expression *ex1p = RDB_var_ref("dbname", ecp);
    if (ex1p == NULL)
        return RDB_ERROR;

    ex2p = RDB_string_to_expr(RDB_db_name(RDB_tx_db(txp)), ecp);
    if (ex2p == NULL) {
        RDB_del_expr(ex1p, ecp);
        return RDB_ERROR;
    }

    wherep = RDB_eq(ex1p, ex2p, ecp);
    if (wherep == NULL) {
        RDB_del_expr(ex1p, ecp);
        RDB_del_expr(ex2p, ecp);
        return RDB_ERROR;
    }

    ex1p = RDB_ro_op("where", ecp);
    if (ex1p == NULL) {
        RDB_del_expr(wherep, ecp);
        return RDB_ERROR;
    }

    ex2p = RDB_table_ref(txp->dbp->dbrootp->dbtables_tbp, ecp);
    if (ex2p == NULL) {
        RDB_del_expr(wherep, ecp);
        RDB_del_expr(ex1p, ecp);
        return RDB_ERROR;
    }

    RDB_add_arg(ex1p, ex2p);
    RDB_add_arg(ex1p, wherep);            

    vtbp = RDB_expr_to_vtable(ex1p, ecp, txp);
    if (vtbp == NULL) {
        RDB_del_expr(ex1p, ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tbarr);
    ret = RDB_table_to_array(&tbarr, vtbp, 0, NULL, 0, ecp, txp);
    if (ret != RDB_OK) {
        RDB_drop_table(vtbp, ecp, txp);
        goto cleanup;
    }
    ret = RDB_drop_table(vtbp, ecp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    len = RDB_array_length(&tbarr, ecp);
    if (len == (RDB_int) RDB_ERROR) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    for (i = 0; i < len; i++) {
        RDB_object *tplp = RDB_array_get(&tbarr, (RDB_int) i, ecp);
        if (tplp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }

        dtbp = RDB_get_table(RDB_tuple_get_string(tplp, "tablename"), ecp, txp);
        if (dtbp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        if (!RDB_table_is_real(dtbp)) {
            if (RDB_expr_refers(RDB_vtable_expr(dtbp), tbp)) {
                RDB_raise_in_use("a virtual table depends on this table", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        }
    }
    ret = RDB_OK;           

cleanup:
    RDB_destroy_obj(&tbarr, ecp);

    return ret;
}

/**
RDB_drop_table deletes the table specified by <var>tbp</var>
and releases all resources associated with that table.
If the table is virtual, its unnamed child tables are also deleted.

If an error occurs, an error value is left in *<var>ecp</var>.

If the table is local, <var>txp</var> may be NULL.

@returns

On success, RDB_OK is returned. On failure, RDB_ERROR is returned.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>The table is global (persistent) and <var>txp</var>
does not point to a running transaction.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
int
RDB_drop_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_constraint *constrp;

    if (tbp->kind == RDB_OB_TABLE && tbp->val.tb.is_persistent) {
        RDB_database *dbp;
        RDB_dbroot *dbrootp;

        if (!RDB_tx_is_running(txp)) {
            RDB_raise_no_running_tx(ecp);
            return RDB_ERROR;
        }

        if (table_dep_check(tbp, ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }

        dbrootp = (RDB_dbroot *) RDB_env_xdata(txp->envp);

        /*
         * Read constraints from DB
         */
        if (!dbrootp->constraints_read) {
            if (RDB_read_constraints(ecp, txp) != RDB_OK) {
                return RDB_ERROR;
            }
        }

        /* Check if a constraint depends on this table */
        constrp = dbrootp->first_constrp;
        while (constrp != NULL) {
            if (RDB_expr_refers(constrp->exp, tbp)) {
                RDB_raise_in_use(constrp->name, ecp);
                return RDB_ERROR;
            }
            constrp = constrp->nextp;
        }

        /*
         * Remove table from all RDB_databases in list
         */
        for (dbp = dbrootp->first_dbp; dbp != NULL; dbp = dbp->nextdbp) {
            RDB_object *foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->val.tb.name);
            if (foundtbp != NULL) {
                RDB_hashmap_put(&dbp->tbmap, tbp->val.tb.name, NULL);
            }
        }

        /*
         * Delete recmap, if any
         */
        if (tbp->val.tb.stp != NULL) {
            ret = RDB_delete_stored_table(tbp->val.tb.stp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
            tbp->val.tb.stp = NULL;
        }

        /*
         * Remove table from catalog
         */
        ret = RDB_cat_delete(tbp, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }

    RDB_free_obj(tbp, ecp);
    return RDB_OK;
}

/**
RDB_set_table_name sets the name of the table to <var>name</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

On success, RDB_OK is returned. On failure, RDB_ERROR is returned.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>invalid_argument_error
<dd><var>name</var> is not a valid table name.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
int
RDB_set_table_name(RDB_object *tbp, const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;

    if (!RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid table name", ecp);
        return RDB_ERROR;
    }

    /* Check if virtual tables depend on this table */
    if (table_dep_check(tbp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    if (tbp->val.tb.is_persistent) {
        RDB_database *dbp;

        /* Update catalog */
        ret = RDB_cat_rename_table(tbp, name, ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        /* Delete and reinsert tables from/to table maps */
        for (dbp = txp->dbp->dbrootp->first_dbp; dbp != NULL;
                dbp = dbp->nextdbp) {
            RDB_object *foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->val.tb.name);
            if (foundtbp != NULL) {
                RDB_hashmap_put(&dbp->tbmap, tbp->val.tb.name, NULL);
                RDB_hashmap_put(&dbp->tbmap, name, tbp);
            }
        }
    }
    
    if (tbp->val.tb.name != NULL)
        RDB_free(tbp->val.tb.name);
    tbp->val.tb.name = RDB_dup_str(name);
    if (tbp->val.tb.name == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

/**
RDB_add_table adds the table specified by <var>tbp</var> to the
database the transaction specified by <var>txp</var> interacts with.

If an error occurs, an error value is left in *<var>ecp</var>.

If the table is a local (transient) table, it is made global
(persistent).

The table must have a name.

Currently, RDB_add_table is not supported for local real tables.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>invalid_argument_error
<dd>The table does not have a name.
<dt>element_exist_error
<dd>The table is already associated with the database.
<dt>not_supported_error
<dd>The table is a local real table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
int
RDB_add_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (tbp->val.tb.name == NULL) {
        RDB_raise_invalid_argument("missing table name", ecp);
        return RDB_ERROR;
    }

    /* Turning a local real table into a persistent table is not supported */
    if (!tbp->val.tb.is_persistent && RDB_vtable_expr(tbp) == NULL) {
        RDB_raise_not_supported(
                "operation not supported for local real tables", ecp);
        return RDB_ERROR;
    }

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (RDB_cat_insert(tbp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_assoc_table_db(tbp, txp->dbp, ecp) != RDB_OK)
        return RDB_ERROR;

    tbp->val.tb.is_persistent = RDB_TRUE;

    return RDB_OK;
}

/**
 * RDB_remove_table removes the table specified by <var>tbp</var> from the
database the transaction specified by <var>txp</var> interacts with.

If an error occurs, an error value is left in *<var>ecp</var>.

If the table is a global table, it is made local.

@returns RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>invalid_argument_error
<dd>The table does not belong to the database the transaction interacts
with.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.

@remarks <strong>This function is not implemented.</strong>
int
RDB_remove_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp);
*/

/*@}*/
