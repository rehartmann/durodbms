/*
 * Database functions.
 *
 * Copyright (C) 2003-2009, 2011-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "stable.h"
#include "typeimpl.h"
#include "serialize.h"
#include "catalog.h"
#include <obj/objinternal.h>
#include <gen/hashmapit.h>
#include <gen/hashtabit.h>
#include <gen/strfns.h>
#include <rec/sequence.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>

/*
 * Marker object indicating that a previous search for a table has failed.
 * Must not be accessed.
 */
static RDB_object null_tb;

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
const char *
RDB_db_name(const RDB_database *dbp)
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
free_typemap(RDB_hashmap *typemap, RDB_exec_context *ecp)
{
    RDB_hashmap_iter it;
    void *typ;

    RDB_init_hashmap_iter(&it, typemap);

    /*
     * Delete non-scalar actual reps first, because if we free all types
     * we cannot determine any more if an arep is non-scalar
     */
    while (RDB_hashmap_next(&it, &typ) != NULL) {
        if (typ != NULL) {
            RDB_type *utyp = (RDB_type *) typ;
            if (utyp->def.scalar.arep != NULL) {
                if (!RDB_type_is_scalar(utyp->def.scalar.arep)) {
                    RDB_del_nonscalar_type(utyp->def.scalar.arep, ecp);
                }

                /*
                 * Set arep to NULL so we don't access a scalar type that
                 * was already freed.
                 */
                utyp->def.scalar.arep = NULL;
            }
        }
    }
    RDB_destroy_hashmap_iter(&it);

    RDB_init_hashmap_iter(&it, typemap);
    while (RDB_hashmap_next(&it, &typ) != NULL) {
        if (typ != NULL) {
            RDB_del_type((RDB_type *) typ, ecp);
        }
    }
    RDB_destroy_hashmap_iter(&it);

    RDB_destroy_hashmap(typemap);
}

static void
free_dbroot(RDB_dbroot *dbrootp, RDB_exec_context *ecp)
{
    RDB_constraint *constrp, *nextconstrp;

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

    RDB_destroy_hashmap(&dbrootp->ptbmap);

    free_typemap(&dbrootp->utypemap, ecp);

    RDB_free(dbrootp);
}

static int
close_table(RDB_object *tbp, RDB_environment *envp, RDB_exec_context *ecp)
{
    RDB_dbroot *dbrootp;
    RDB_database *dbp;

    RDB_close_sequences(tbp, ecp);

    if (tbp->val.tbp->stp != NULL) {
        if (RDB_close_stored_table(tbp->val.tbp->stp, ecp) != RDB_OK)
            return RDB_ERROR;
        tbp->val.tbp->stp = NULL;
    }

    /*
     * Remove table from all RDB_databases in list
     */
    dbrootp = (RDB_dbroot *) RDB_env_xdata(envp);
    for (dbp = dbrootp->first_dbp; dbp != NULL; dbp = dbp->nextdbp) {
        RDB_object *foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->val.tbp->name);
        if (foundtbp != NULL) {
            RDB_hashmap_put(&dbp->tbmap, tbp->val.tbp->name, NULL);
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
    void *datap;

    RDB_init_hashmap_iter(&it, &dbp->tbmap);

    do {
        if (RDB_hashmap_next(&it, &datap) == NULL) {
            RDB_destroy_hashmap_iter(&it);
            return RDB_FALSE;
        }

        rtbp = datap != NULL ? (RDB_object *) datap : NULL;
    } while (rtbp == NULL || rtbp == &null_tb || (tbp == rtbp) || !RDB_table_refers(rtbp, tbp));
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

    RDB_init_hashmap_iter(&it, &dbp->tbmap);

    do {
        if (RDB_hashmap_next(&it, &datap) == NULL) {
            RDB_destroy_hashmap_iter(&it);
            return NULL;
        }

        tbp = (RDB_object *) datap;
    } while (tbp == NULL || tbp == &null_tb || !RDB_table_is_user(tbp) || table_refs(dbp, tbp));
    RDB_destroy_hashmap_iter(&it);
    return tbp;
}

static void
close_systables(RDB_dbroot *dbrootp, RDB_exec_context *ecp)
{
    close_table(dbrootp->rtables_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->table_attr_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->table_attr_defvals_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->vtables_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->ptables_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->table_recmap_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->dbtables_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->keys_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->types_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->possrepcomps_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->ro_ops_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->ro_op_versions_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->upd_ops_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->upd_op_versions_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->indexes_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->constraints_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->version_info_tbp, dbrootp->envp, ecp);
    close_table(dbrootp->subtype_tbp, dbrootp->envp, ecp);
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
    RDB_init_hashmap(&dbrootp->utypemap, RDB_DFL_MAP_CAPACITY);
    RDB_init_op_map(&dbrootp->ro_opmap);
    RDB_init_op_map(&dbrootp->upd_opmap);

    RDB_init_hashmap(&dbrootp->ptbmap, 100);

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
    if (RDB_assoc_table_db(dbrootp->ptables_tbp, dbp, ecp) != RDB_OK)
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
    if (RDB_assoc_table_db(dbrootp->ro_op_versions_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->upd_ops_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->upd_op_versions_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->indexes_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->constraints_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->version_info_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_assoc_table_db(dbrootp->subtype_tbp, dbp, ecp) != RDB_OK)
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
get_db(const char *name, RDB_dbroot *dbrootp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_database *dbp = NULL;
    RDB_transaction tx;

    /* Search the DB list for the database */
    for (dbp = dbrootp->first_dbp; dbp != NULL; dbp = dbp->nextdbp) {
        if (strcmp(dbp->name, name) == 0) {
            return dbp;
        }
    }

    /*
     * Not found, read database from catalog
     */

    dbp = new_db(name, ecp);
    if (dbp == NULL) {
        goto error;
    }

    if (assoc_systables(dbrootp, dbp, ecp) != RDB_OK)
        goto error;
    dbp->dbrootp = dbrootp;

    if (txp == NULL) {
        if (RDB_begin_tx(ecp, &tx, dbp, NULL) != RDB_OK) {
            goto error;
        }
    }

    if (RDB_cat_db_exists(name, dbrootp, ecp, txp != NULL ? txp : &tx) != RDB_OK) {
        if (txp == NULL)
            RDB_rollback(ecp, &tx);
        goto error;
    }

    if (txp == NULL) {
        if (RDB_commit(ecp, &tx) != RDB_OK)
            return NULL;
    }

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

    RDB_init_hashmap(&sdb.tbmap, 1);

    if (RDB_begin_tx_env(ecp, &tx, envp, NULL) != RDB_OK) {
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

    if (RDB_commit(ecp, &tx) != RDB_OK)
        goto error;

    RDB_env_set_xdata(envp, dbrootp);

    RDB_destroy_hashmap(&sdb.tbmap);

    return dbrootp;

error:
    RDB_destroy_hashmap(&sdb.tbmap);
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

        if (RDB_init_builtin(ecp) != RDB_OK) {
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
<var>txp</var> may be NULL.

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
                    RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get dbroot from env */
    RDB_dbroot *dbrootp = get_dbroot(envp, ecp);
    if (dbrootp == NULL)
        return NULL;

    return get_db(name, dbrootp, ecp, txp);
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
    int ret = RDB_hashmap_put(&dbp->tbmap, RDB_table_name(tbp), tbp);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp);
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
    RDB_transaction tx;
    RDB_obj_cleanup_func *cleanup_fp;

    RDB_object *tbp = RDB_hashmap_get(&txp->dbp->tbmap, name);
    if (tbp != NULL && tbp != &null_tb) {
        /* Table found - check if it exists in the catalog */
        if ((RDB_TB_CHECK & tbp->val.tbp->flags) && (RDB_check_table(tbp, ecp, txp) != RDB_OK)) {
            /* Table no longer valid - recycle its RDB_object structure */
            char *name = tbp->val.tbp->name;
            tbp->val.tbp->name = NULL;
            if (RDB_init_table_i(tbp, NULL, RDB_TRUE, reltyp,
                    keyc, keyv, default_attrc, default_attrv,
                    RDB_TRUE, NULL, ecp) != RDB_OK) {
                return NULL;
            }
            tbp->val.tbp->name = name;
        } else {
            RDB_raise_element_exists("table already exists", ecp);
            return NULL;
        }
    } else {
        tbp = RDB_new_rtable(name, RDB_TRUE, reltyp, keyc, keyv,
            default_attrc, default_attrv, RDB_TRUE, ecp);
        if (tbp == NULL) {
            return NULL;
        }
    }

    /* Create subtransaction */
    if (RDB_begin_tx(ecp, &tx, txp->dbp, txp) != RDB_OK)
        return NULL;

    /* Insert table into catalog */
    if (RDB_cat_insert(tbp, ecp, &tx) != RDB_OK) {
        RDB_rollback(ecp, &tx);
        goto error;
    }

    if (RDB_assoc_table_db(tbp, txp->dbp, ecp) != RDB_OK) {
        RDB_rollback(ecp, &tx);
        goto error;
    }

    if (RDB_commit(ecp, &tx) != RDB_OK) {
        RDB_hashmap_put(&txp->dbp->tbmap, name, NULL);
        goto error;
    }

    return tbp;

error:
    /* Don't destroy type */
    cleanup_fp = tbp->typ->cleanup_fp;
    tbp->typ = NULL;
    (*cleanup_fp)(tbp, ecp);
    RDB_free_obj(tbp, ecp);
    return NULL;
}

/** @defgroup table Table functions 
 * @{
 * \#include <dli/parse.h>
 */

/**
Create a real table with name <var>name</var> in the database
the transaction *<var>txp</var> interacts with
and return a pointer to the newly created RDB_object structure
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
<dt>element_exists_error
<dd>There is already a database table with name <var>name</var>.
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
    for (i = 0; i < default_attrc; i++) {
        if (default_attrv[i].defaultp != NULL
                && RDB_expr_is_serial(default_attrv[i].defaultp)) {
            RDB_attr *defattr = RDB_tuple_type_attr(reltyp->def.basetyp, default_attrv[i].name);
            if (defattr == NULL || defattr->typ != &RDB_INTEGER) {
                RDB_raise_invalid_argument("invalid serial()", ecp);
                return NULL;
            }
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
RDB_get_table looks up the real, virtual, or public table
with name <var>name</var> in the environment of the database
the transaction specified by <var>txp</var> interacts with
and returns a pointer to it.

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

If a call failed with a name_error, subsequent attempts to get the
same table from the same database will also fail.
This is to prevent costly multiple attempts to resolve variables by
searching them as tables in the catalog.
*/
RDB_object *
RDB_get_table(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *tbp;

    /* Search table in databases */
    tbp = RDB_hashmap_get(&txp->dbp->tbmap, name);
    if (tbp != NULL) {
        if (tbp == &null_tb) {
            /* A previous search has already failed */
            RDB_raise_name(name, ecp);
            return NULL;
        }
        /* Found */
        return tbp;
    }

    /* Search public table in db root */
    tbp = RDB_hashmap_get(&txp->dbp->dbrootp->ptbmap, name);
    if (tbp != NULL) {
        /* Found */
        return tbp;
    }

    /* If not found, read from catalog, search in real tables first */

    if (RDB_env_trace(RDB_db_env(RDB_tx_db(txp))) > 0) {
        fprintf(stderr,
                "Trying to read table %s from the catalog\n",
                name);
    }

    tbp = RDB_new_obj(ecp);
    if (tbp == NULL)
        return NULL;

    if (RDB_cat_get_table(tbp, name, ecp, txp) == RDB_OK)
        return tbp;

    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        /* Replace not_found_error with name_error */
        RDB_raise_name(name, ecp);
    }

    RDB_free_obj(tbp, ecp);
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NAME_ERROR) {
        /* Remember failed attempt */
        if (RDB_hashmap_put(&txp->dbp->tbmap, name, &null_tb) != RDB_OK)
            RDB_raise_no_memory(ecp);
    }
    return NULL;
}

/*
 * Check if table with name tbname depends on *tbp and raise in_use_error if it does
 */
static int
table_name_dep_check(const char *tbname, RDB_object *tbp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *tbexp;
    RDB_object *dtbp = RDB_get_table(tbname, ecp, txp);
    if (dtbp == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NAME_ERROR) {
            /* If tbname is not found, that's OK
             * (may happen when tbname is an unimplemented public table)
             */
            return RDB_OK;
        }
        return RDB_ERROR;
    }

    tbexp = RDB_vtable_expr(dtbp);
    if (tbexp != NULL) {
        if (RDB_expr_resolve_tbnames(tbexp, ecp, txp) != RDB_OK) {
            return RDB_ERROR;
        }
        if (RDB_expr_refers(tbexp, tbp)) {
            RDB_raise_in_use("a virtual table depends on this table", ecp);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
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

        ret = table_name_dep_check(RDB_tuple_get_string(tplp, "tablename"), tbp,
                ecp, txp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    /*
     * Check public tables
     */

    RDB_destroy_obj(&tbarr, ecp);
    RDB_init_obj(&tbarr);
    ret = RDB_table_to_array(&tbarr, txp->dbp->dbrootp->ptables_tbp, 0, NULL, 0, ecp, txp);
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

        ret = table_name_dep_check(RDB_tuple_get_string(tplp, "tablename"), tbp,
                ecp, txp);
        if (ret != RDB_OK)
            goto cleanup;
    }

    ret = RDB_OK;           

cleanup:
    RDB_destroy_obj(&tbarr, ecp);

    return ret;
}

static int
delete_sequences(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_hashmap_iter hiter;
    void *valp;
    int ret;
    const char *attrname;

    if (tbp->val.tbp->default_map == NULL)
        return RDB_OK;

    RDB_init_hashmap_iter(&hiter, tbp->val.tbp->default_map);
    while ((attrname = RDB_hashmap_next(&hiter, &valp)) != NULL) {
        RDB_attr_default *dflp = valp;
        if (RDB_expr_is_serial(dflp->exp)) {
            RDB_sequence *seqp;
            if (dflp->seqp != NULL) {
                seqp = dflp->seqp;
            } else {
                RDB_object seqname;
                RDB_init_obj(&seqname);
                if (RDB_seq_container_name(RDB_table_name(tbp), attrname, &seqname, ecp) != RDB_OK) {
                    RDB_destroy_obj(&seqname, ecp);
                    RDB_destroy_hashmap_iter(&hiter);
                    return RDB_ERROR;
                }
                seqp = RDB_open_sequence(RDB_obj_string(&seqname), RDB_DATAFILE,
                            RDB_db_env(RDB_tx_db(txp)), txp->tx, ecp);
                if (seqp == NULL) {
                    RDB_destroy_obj(&seqname, ecp);
                    RDB_destroy_hashmap_iter(&hiter);
                    RDB_handle_err(ecp, txp);
                    return RDB_ERROR;
                }
                RDB_destroy_obj(&seqname, ecp);
            }
            ret = RDB_delete_sequence(seqp, RDB_db_env(RDB_tx_db(txp)), txp->tx, ecp);
            dflp->seqp = NULL;
            if (ret != 0) {
                RDB_destroy_hashmap_iter(&hiter);
                RDB_handle_err(ecp, txp);
                return RDB_ERROR;
            }
        }
    }
    RDB_destroy_hashmap_iter(&hiter);
    return RDB_OK;
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
<dt>in_use_error
<dd>An existing virtual table depends on the table.
<dt>
<dd>A constraint depends on the table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
int
RDB_drop_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_constraint *constrp;

    if (tbp->kind == RDB_OB_TABLE && RDB_table_is_persistent(tbp)) {
        RDB_database *dbp;
        RDB_dbroot *dbrootp;

        if (!RDB_tx_is_running(txp)) {
            RDB_raise_no_running_tx(ecp);
            return RDB_ERROR;
        }

        /*
         * If the table is in CHECK state read it again from the catalog
         * because otherwise the recmap won't get deleted
         */
        if (RDB_TB_CHECK & tbp->val.tbp->flags) {
            if (RDB_check_table(tbp, ecp, txp) != RDB_OK)
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
            RDB_object *foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->val.tbp->name);
            if (foundtbp != NULL) {
                RDB_hashmap_put(&dbp->tbmap, tbp->val.tbp->name, NULL);
            }
        }

        /*
         * Remove table from public tables
         */
        if (RDB_hashmap_get(&dbrootp->ptbmap, tbp->val.tbp->name) != NULL) {
            if (RDB_hashmap_put(&dbrootp->ptbmap, tbp->val.tbp->name, NULL) != RDB_OK)
                return RDB_ERROR;
        }

        if (!RDB_env_queries(txp->envp)) {
            if (delete_sequences(tbp, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        }

        /*
         * Delete recmap, if any
         */
        if (tbp->val.tbp->stp != NULL) {
            ret = RDB_delete_stored_table(tbp->val.tbp->stp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
            tbp->val.tbp->stp = NULL;
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
 * Drop table by name. This is currently the only way to drop a unmapped public table.
 */
int
RDB_drop_table_by_name(const char *tbname, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *tbp = RDB_get_table(tbname, ecp, txp);
    if (tbp != NULL) {
        return RDB_drop_table(tbp, ecp, txp);
    }
    /* Could be a public unmapped table - delete it from the catalog */
    return RDB_cat_delete_ptable(tbname, ecp, txp);
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
    RDB_object *foundtbp;

    if (!RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid table name", ecp);
        return RDB_ERROR;
    }

    /* Check if virtual tables depend on this table */
    if (table_dep_check(tbp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_table_is_persistent(tbp)) {
        RDB_database *dbp;

        /* Update catalog */
        ret = RDB_cat_rename_table(tbp, name, ecp, txp);
        if (ret != RDB_OK) {
            if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
                RDB_raise_internal(
                        "table is persisten but was not found in the catalog",
                        ecp);
            }
            return RDB_ERROR;
        }

        /* Delete and reinsert tables from/to table maps */

        for (dbp = txp->dbp->dbrootp->first_dbp; dbp != NULL;
                dbp = dbp->nextdbp) {
            foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->val.tbp->name);
            if (foundtbp != NULL) {
                RDB_hashmap_put(&dbp->tbmap, tbp->val.tbp->name, NULL);
                RDB_hashmap_put(&dbp->tbmap, name, tbp);
            }
        }
        foundtbp = RDB_hashmap_get(&txp->dbp->dbrootp->ptbmap,
                tbp->val.tbp->name);
        if (foundtbp != NULL) {
            RDB_hashmap_put(&txp->dbp->dbrootp->ptbmap, tbp->val.tbp->name, NULL);
            RDB_hashmap_put(&txp->dbp->dbrootp->ptbmap, name, tbp);
        }

        /* Rename sequences */
        if (!RDB_env_queries(txp->envp) && tbp->val.tbp->default_map != NULL) {
            RDB_attr_default *entryp;
            RDB_hashmap_iter hiter;
            void *valp;
            const char *attrnamp;

            RDB_init_hashmap_iter(&hiter, tbp->val.tbp->default_map);
            while ((attrnamp = RDB_hashmap_next(&hiter, &valp)) != NULL) {
                entryp = valp;
                if (entryp != NULL && RDB_expr_is_serial(entryp->exp)) {
                    RDB_object oldname;
                    RDB_object newname;

                    RDB_init_obj(&oldname);
                    RDB_init_obj(&newname);
                    if (entryp->seqp != NULL) {
                        RDB_close_sequence(entryp->seqp, ecp);
                        entryp->seqp = NULL;
                    }

                    if (RDB_seq_container_name(tbp->val.tbp->name, attrnamp,
                            &oldname, ecp) != RDB_OK) {
                        RDB_destroy_obj(&oldname, ecp);
                        RDB_destroy_obj(&newname, ecp);
                        return RDB_ERROR;
                    }
                    if (RDB_seq_container_name(name, attrnamp, &newname, ecp) != RDB_OK) {
                        RDB_destroy_obj(&oldname, ecp);
                        RDB_destroy_obj(&newname, ecp);
                        return RDB_ERROR;
                    }
                    ret = RDB_rename_sequence(RDB_obj_string(&oldname),
                            RDB_obj_string(&newname), RDB_DATAFILE,
                            RDB_db_env(RDB_tx_db(txp)), txp->tx, ecp);
                    RDB_destroy_obj(&oldname, ecp);
                    RDB_destroy_obj(&newname, ecp);
                    if (ret != RDB_OK) {
                        RDB_handle_err(ecp, txp);
                        return RDB_ERROR;
                    }
                }
            }
            RDB_destroy_hashmap_iter(&hiter);
        }

        /* Rename generated indexes */
        if (tbp->val.tbp->stp != NULL) {
            int i;
            char *suffix;
            char *newidxname;
            for (i = 0; i < tbp->val.tbp->stp->indexc; i++) {
                if (tbp->val.tbp->stp->indexv[i].name != NULL) {
                    suffix = strchr(tbp->val.tbp->stp->indexv[i].name, '$');
                    if (suffix != NULL) {
                        newidxname = RDB_alloc(strlen(name) + strlen(suffix) + 1, ecp);
                        if (newidxname == NULL)
                            return RDB_ERROR;
                    }
                    strcpy(newidxname, name);
                    strcat(newidxname, suffix);
                    if (RDB_cat_rename_index(tbp->val.tbp->stp->indexv[i].name,
                            newidxname, ecp, txp) != RDB_OK) {
                        RDB_free(newidxname);
                        RDB_handle_err(ecp, txp);
                        return RDB_ERROR;
                    }
                    RDB_free(tbp->val.tbp->stp->indexv[i].name);
                    tbp->val.tbp->stp->indexv[i].name = newidxname;
                }
            }
        }
    }

    if (tbp->val.tbp->name != NULL)
        RDB_free(tbp->val.tbp->name);
    tbp->val.tbp->name = RDB_dup_str(name);
    if (tbp->val.tbp->name == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

/**
 * If dbp is not NULL, RDB_add_table adds the table *<var>tbp</var> to the
 * database *<var>dbp</var>.
If dbp is NULL, the table is added to the catalog and to the database
the transaction specified by <var>txp</var> interacts with.

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
RDB_add_table(RDB_object *tbp, RDB_database *dbp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    if (tbp->val.tbp->name == NULL) {
        RDB_raise_invalid_argument("missing table name", ecp);
        return RDB_ERROR;
    }

    /* Turning a local real table into a persistent table is not supported */
    if (!RDB_table_is_persistent(tbp) && RDB_vtable_expr(tbp) == NULL) {
        RDB_raise_not_supported(
                "operation not supported for local real tables", ecp);
        return RDB_ERROR;
    }

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (dbp == NULL) {
        if (RDB_cat_insert(tbp, ecp, txp) != RDB_OK)
            return RDB_ERROR;
    } else {
        if (RDB_cat_dbtables_insert(tbp, dbp, ecp, txp) != RDB_OK)
            return RDB_ERROR;
    }

    if (RDB_assoc_table_db(tbp, dbp == NULL ? RDB_tx_db(txp) : dbp, ecp)
            != RDB_OK) {
        return RDB_ERROR;
    }

    tbp->val.tbp->flags |= RDB_TB_PERSISTENT;

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

/*
 * Close all user tables
 */
int
RDB_close_user_tables(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    RDB_hashmap_iter it;
    void *datap;

    /* Close public tables */

    RDB_init_hashmap_iter(&it, &dbp->dbrootp->ptbmap);
    while (RDB_hashmap_next(&it, &datap) != NULL) {
        if (datap != NULL) {
            tbp = datap;
            if (close_table(tbp, dbp->dbrootp->envp, ecp)
                    != RDB_OK)
                return RDB_ERROR;
        }
    }
    RDB_destroy_hashmap_iter(&it);

    RDB_clear_hashmap(&dbp->dbrootp->ptbmap);

    while ((tbp = find_del_table(dbp)) != NULL) {
        if (close_table(tbp, dbp->dbrootp->envp, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    return RDB_OK;
}

/*
 * Set CHECK flag for all user tables
 */
int
RDB_set_user_tables_check(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    RDB_hashmap_iter it;
    void *datap;

    /* Public tables */

    RDB_init_hashmap_iter(&it, &dbp->dbrootp->ptbmap);
    while (RDB_hashmap_next(&it, &datap) != NULL) {
        if (datap != NULL) {
            tbp = datap;
            tbp->val.tbp->flags |= RDB_TB_CHECK;
        }
    }
    RDB_destroy_hashmap_iter(&it);

    RDB_init_hashmap_iter(&it, &dbp->tbmap);
    while (RDB_hashmap_next(&it, &datap) != NULL) {
        if (datap != NULL && datap != &null_tb) {
            tbp = datap;
            if (RDB_table_is_user(tbp)) {
                if (tbp->val.tbp->stp != NULL) {
                    RDB_close_stored_table(tbp->val.tbp->stp, ecp);
                    tbp->val.tbp->stp = NULL;
                }
                tbp->val.tbp->flags |= RDB_TB_CHECK;
            }

            /* Close all sequences */
            if (tbp->val.tbp->default_map != NULL) {
                RDB_hashmap_iter hiter;
                void *valp;
                RDB_attr_default *entryp;

                RDB_init_hashmap_iter(&hiter, tbp->val.tbp->default_map);
                while (RDB_hashmap_next(&hiter, &valp) != NULL) {
                    entryp = valp;
                    if (entryp->seqp != NULL) {
                        RDB_close_sequence(entryp->seqp, ecp);
                        entryp->seqp = NULL;
                    }
                }
                RDB_destroy_hashmap_iter(&hiter);
            }
        }
    }
    RDB_destroy_hashmap_iter(&it);

    return RDB_OK;
}
