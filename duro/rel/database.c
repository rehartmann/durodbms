/*
 * $Id$
 *
 * Copyright (C) 2003-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include "stable.h"
#include "typeimpl.h"
#include "serialize.h"
#include "catalog.h"
#include <dli/tabletostr.h>
#include <gen/hashmapit.h>
#include <gen/hashtabit.h>
#include <gen/strfns.h>
#include <string.h>
#include <stdio.h>
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
    RDB_hashmap_iter it;
    char *keyp;
    void *datap;
    RDB_constraint *constrp, *nextconstrp;

    RDB_destroy_hashmap(&dbrootp->typemap);

    /*
     * destroy user-defined operators in memory
     */
    RDB_init_hashmap_iter(&it, &dbrootp->ro_opmap);
    while ((datap = RDB_hashmap_next(&it, &keyp)) != NULL) {
        RDB_ro_op_desc *op = datap;

        if (op != NULL)
            _RDB_free_ro_ops(op, ecp);
    }
    RDB_destroy_hashmap_iter(&it);

    /*
     * Destroy constraints
     */
    constrp = dbrootp->first_constrp;
    while (constrp != NULL) {
        nextconstrp = constrp->nextp;
        RDB_free(constrp->name);
        RDB_drop_expr(constrp->exp, ecp);
        RDB_free(constrp);
        constrp = nextconstrp;
    }

    RDB_destroy_hashmap(&dbrootp->ro_opmap);

    RDB_destroy_op_map(&dbrootp->upd_opmap);

    RDB_destroy_hashtable(&dbrootp->empty_tbtab);
    RDB_free(dbrootp);
}

static int
close_table(RDB_object *tbp, RDB_environment *envp, RDB_exec_context *ecp)
{
    int ret;
    RDB_dbroot *dbrootp;
    RDB_database *dbp;

    if (tbp->var.tb.stp != NULL) {
        ret = _RDB_close_stored_table(tbp->var.tb.stp, ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        tbp->var.tb.stp = NULL;
    }

    /*
     * Remove table from all RDB_databases in list
     */
    dbrootp = (RDB_dbroot *)RDB_env_private(envp);
    for (dbp = dbrootp->first_dbp; dbp != NULL; dbp = dbp->nextdbp) {
        RDB_object *foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->var.tb.name);
        if (foundtbp != NULL) {
            RDB_hashmap_put(&dbp->tbmap, tbp->var.tb.name, NULL);
        }
    }

    return _RDB_free_obj(tbp, ecp);
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
    } while (rtbp == NULL || (tbp == rtbp) || !_RDB_table_refers(rtbp, tbp));
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
    } while (tbp == NULL || !tbp->var.tb.is_user || table_refs(dbp, tbp));
    RDB_destroy_hashmap_iter(&it);
    return tbp;
}

static int
release_db(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object *tbp;

    /*
     * Close all user tables
     */
    
    while ((tbp = find_del_table(dbp)) != NULL) {
        ret = close_table(tbp, dbp->dbrootp->envp, ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }

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
    RDB_dbroot *dbrootp = (RDB_dbroot *) RDB_env_private(envp);
    RDB_database *dbp;
    RDB_database *nextdbp;
    RDB_hashtable_iter tit;
    RDB_expression *eexp;
    RDB_exec_context ec;

    if (dbrootp == NULL)
        return;

    RDB_init_exec_context(&ec);

    /* Destroy tables used in IS_EMPTY constraints */
    RDB_init_hashtable_iter(&tit, &dbrootp->empty_tbtab);
    while ((eexp = RDB_hashtable_next(&tit)) != NULL) {
        RDB_drop_expr(eexp, &ec);
    }
    RDB_destroy_hashtable_iter(&tit);

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

static unsigned
hash_exp(const void *exp, void *arg)
{
    return ((RDB_expression *) exp)->kind; /* !! */
}

static RDB_bool
exp_equals(const void *ex1p, const void *ex2p, void *argp)
{
    RDB_bool res;
    struct _RDB_tx_and_ec *tep = (struct _RDB_tx_and_ec *) argp;

    _RDB_expr_equals((RDB_expression *) ex1p, (RDB_expression *) ex2p,
            tep->ecp, tep->txp, &res);
    return res;
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
    RDB_init_hashmap(&dbrootp->ro_opmap, RDB_DFL_MAP_CAPACITY);
    RDB_init_op_map(&dbrootp->upd_opmap);
    RDB_init_hashtable(&dbrootp->empty_tbtab, RDB_DFL_MAP_CAPACITY,
            &hash_exp, &exp_equals);

    dbrootp->first_dbp = NULL;
    dbrootp->first_constrp = NULL;
    dbrootp->constraints_read = RDB_FALSE;

    return dbrootp;
}

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
    if (_RDB_assoc_table_db(dbrootp->table_attr_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->table_attr_defvals_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->rtables_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->vtables_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->dbtables_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->keys_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->types_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->possrepcomps_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->ro_ops_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->upd_ops_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->indexes_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->constraints_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (_RDB_assoc_table_db(dbrootp->version_info_tbp, dbp, ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_OK;
}

/*
 * Create and initialize RDB_dbroot structure. Use dbp to create the transaction.
 * If newdb is true, create the database in the catalog.
 */
static int
create_dbroot(RDB_environment *envp, RDB_bool newdb,
              RDB_exec_context *ecp, RDB_dbroot **dbrootpp)
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
        return RDB_ERROR;
    }

    ret = _RDB_begin_tx(ecp, &tx, envp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    tx.dbp = &sdb;
    sdb.dbrootp = dbrootp;
    ret = _RDB_open_systables(dbrootp, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        goto error;
    }

    ret = RDB_commit(ecp, &tx);
    if (ret != RDB_OK)
        goto error;

    RDB_env_private(envp) = dbrootp;

    *dbrootpp = dbrootp;
    return RDB_OK;

error:
    free_dbroot(dbrootp, ecp);
    return ret;
}

/**
RDB_create_db_from_env creates a database from a database environment.
If an error occurs, an error value is left in <var>ecp</var>.

@returns

A pointer to the newly created database, or NULL if an error occurred.

@par Errors:

<dl>
<dt>RDB_ELEMENT_EXIST_ERROR
<dd>A database with the name <var>name</var> already exixts.
<dt>RDB_VERSION_MISMATCH_ERROR
<dd>The version number stored in the catalog does not match
the version of the library.
</dl>

The call may also fail for a @ref system-errors "system error".
*/
RDB_database *
RDB_create_db_from_env(const char *name, RDB_environment *envp,
                       RDB_exec_context *ecp)
{
    int ret;
    RDB_database *dbp;
    RDB_dbroot *dbrootp;
    RDB_transaction tx;

    if (!_RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid database name", ecp);
        return NULL;
    }

    dbrootp = (RDB_dbroot *)RDB_env_private(envp);
    dbp = new_db(name, ecp);
    if (dbp == NULL) {
        return NULL;
    }

    if (dbrootp == NULL) {
        /*
         * No dbroot found, initialize builtin types and libltdl
         * and create RDB_dbroot structure
         */
        if (_RDB_init_builtin_types(ecp) != RDB_OK) {
            goto error;
        }
        ret = create_dbroot(envp, RDB_TRUE, ecp, &dbrootp);
        if (ret != RDB_OK) {
            goto error;
        }
        lt_dlinit();
    }

    /*
     * Create DB in catalog
     */

    dbp->dbrootp = dbrootp;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = _RDB_cat_create_db(ecp, &tx);
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

/**
RDB_get_db_from_env obtains a pointer to the database with name
<var>name</var> in the environment specified by <var>envp</var>.
If an error occurs, an error value is left in <var>ecp</var>.

@returns

On success, a pointer to the database is returned. If an error occurred, NULL is
returned.

@par Errors:

<dl>
<dt>RDB_NOT_FOUND_ERROR
<dd>A database with the name <var>name</var> could not be found.
<dt>RDB_VERSION_MISMATCH_ERROR
<dd>The version number stored in the catalog does not match
the version of the library.
</dl>

The call may also fail for a @ref system-errors "system error".
*/
RDB_database *
RDB_get_db_from_env(const char *name, RDB_environment *envp,
                    RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tx;
    RDB_object tpl;
    RDB_bool b;
    RDB_database *dbp = NULL;
    RDB_dbroot *dbrootp = (RDB_dbroot *) RDB_env_private(envp);
    RDB_bool crdbroot = RDB_FALSE;

    if (dbrootp == NULL) {
        /*
         * No dbroot found, initialize builtin types and libltdl
         * and create RDB_dbroot structure
         */

        if (_RDB_init_builtin_types(ecp) != RDB_OK) {
            goto error;
        }
        lt_dlinit();
        ret = create_dbroot(envp, RDB_FALSE, ecp, &dbrootp);
        if (ret != RDB_OK) {
            goto error;
        }
        crdbroot = RDB_TRUE;
    }

    /* search the DB list for the database */
    for (dbp = dbrootp->first_dbp; dbp != NULL; dbp = dbp->nextdbp) {
        if (strcmp(dbp->name, name) == 0) {
            return dbp;
        }
    }

    /*
     * Not found, read database from catalog
     */

    ret = _RDB_begin_tx(ecp, &tx, envp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    /*
     * Check if the database exists by checking if the DBTABLES contains
     * SYS_RTABLES for this database.
     */

    RDB_init_obj(&tpl);
    ret = RDB_tuple_set_string(&tpl, "TABLENAME", "SYS_RTABLES", ecp);
    if (ret != RDB_OK) {
        RDB_rollback(ecp, &tx);
        goto error;
    }
    ret = RDB_tuple_set_string(&tpl, "DBNAME", name, ecp);
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
    if (crdbroot) {
        close_systables(dbrootp, ecp);
        free_dbroot(dbrootp, ecp);
        RDB_env_private(envp) = NULL;
    }

    if (dbp != NULL) {
        free_db(dbp);
    }
    lt_dlexit();
    return NULL;
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
    ex2p = RDB_var_ref("IS_USER", ecp);
    if (ex2p == NULL) {
        goto error;
    }
    ex3p = RDB_ro_op("WHERE", ecp);
    if (ex3p == NULL) {
        goto error;
    }
    RDB_add_arg(ex3p, ex1p);
    RDB_add_arg(ex3p, ex2p);

    ex2p = NULL;
    ex1p = RDB_var_ref("DBNAME", ecp);
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
    ex2p = RDB_ro_op("WHERE", ecp);
    if (ex2p == NULL)
        goto error;
    RDB_add_arg(ex2p, ex1p);
    RDB_add_arg(ex2p, ex4p);
    ex4p = NULL;
    ex1p = RDB_ro_op("JOIN", ecp);
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
    if (ex1p == NULL)
        RDB_drop_expr(ex1p, ecp);
    if (ex2p == NULL)
        RDB_drop_expr(ex2p, ecp);
    if (ex3p == NULL)
        RDB_drop_expr(ex3p, ecp);
    if (ex1p == NULL)
        RDB_drop_expr(ex4p, ecp);
    return NULL;
}

static RDB_object *
db_exists_vt(RDB_database *dbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
	RDB_object *vtbp;
	RDB_expression *ex1p = NULL;
	RDB_expression *ex2p = NULL;
	RDB_expression *ex3p = NULL;
	
	ex1p = RDB_var_ref("DBNAME", ecp);
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

    ex2p = RDB_ro_op("WHERE", ecp);
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
        RDB_drop_expr(ex1p, ecp);
    if (ex2p == NULL)
        RDB_drop_expr(ex2p, ecp);
    if (ex3p == NULL)
        RDB_drop_expr(ex3p, ecp);
    return NULL;
}

/**
RDB_drop_db deletes the database specified by <var>dbp</var>.
The database must be empty.

If an error occurs, an error value is left in <var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>RDB_NOT_FOUND_ERROR
<dd>The database was not found.
<dt>RDB_ELEMENT_EXISTS_ERROR
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

    /* Disassociate all tables from database */

    exp = RDB_eq(RDB_var_ref("DBNAME", ecp),
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
    ex2p = RDB_string_to_expr("DBNAME", ecp);
    if (ex2p == NULL)
        return NULL;

    ex3p = RDB_ro_op("PROJECT", ecp);
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
        RDB_drop_expr(ex1p, ecp);
    if (ex2p == NULL)
        RDB_drop_expr(ex2p, ecp);
    if (ex3p == NULL)
        RDB_drop_expr(ex3p, ecp);
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
<dt>RDB_TYPE_MISMATCH_ERROR
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
    RDB_dbroot *dbrootp = (RDB_dbroot *) RDB_env_private(envp);

    if (dbrootp == NULL) {
        /*
         * No dbroot found, initialize builtin types and libltdl
         * and create RDB_dbroot structure
         */
        if (_RDB_init_builtin_types(ecp) != RDB_OK) {
            return RDB_ERROR;
        }
        ret = create_dbroot(envp, RDB_TRUE, ecp, &dbrootp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }
        lt_dlinit();
    }

    ret = _RDB_begin_tx(ecp, &tx, envp, NULL);
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

    ret = RDB_table_to_array(&resarr, vtbp, 0, NULL, ecp, &tx);
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

        ret = RDB_array_set(arrp, (RDB_int) i, RDB_tuple_get(tplp, "DBNAME"),
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
_RDB_assoc_table_db(RDB_object *tbp, RDB_database *dbp, RDB_exec_context *ecp)
{
    /* Insert table into table map */
    int ret = RDB_hashmap_put(&dbp->tbmap, tbp->var.tb.name, tbp);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, NULL);
        return RDB_ERROR;
    }
    return RDB_OK;
}

/**
 * Check if the name is legal
 */
RDB_bool
_RDB_legal_name(const char *name)
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
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *tbp;
    RDB_transaction tx;

    /* Create subtransaction */
    if (RDB_begin_tx(ecp, &tx, txp->dbp, txp) != RDB_OK)
        return NULL;

    tbp = _RDB_new_rtable(name, RDB_TRUE, reltyp,
                keyc, keyv, RDB_TRUE, ecp);
    if (tbp == NULL) {
        return NULL;
    }

    /* Insert table into catalog */
    if (_RDB_cat_insert(tbp, ecp, &tx) != RDB_OK) {
        /* Don't destroy type */
        tbp->typ = NULL;
        RDB_rollback(ecp, &tx);
        _RDB_free_obj(tbp, ecp);
        return NULL;
    }

    if (_RDB_assoc_table_db(tbp, txp->dbp, ecp) != RDB_OK) {
        RDB_rollback(ecp, &tx);
        _RDB_free_obj(tbp, ecp);
        return NULL;
    }

    if (RDB_commit(ecp, &tx) != RDB_OK) {
         RDB_hashmap_put(&txp->dbp->tbmap, name, NULL);
        _RDB_free_obj(tbp, ecp);
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
and returns a pointer to the newly created RDB_object structure.

If an error occurs, an error value is left in *<var>ecp</var>.

The table will have <var>attrc</var> attributes. The individual
attributes are specified by the elements of <var>attrv</var>.

The candidate keys for the table are specified by <var>keyc</var>
and <var>keyv</var>.

At least one candidate key must be specified.
A candidate key may not be a subset of another.
If a single candidate key is specified, that key
may be empty (not contain any attributes).

Passing a <var>keyv</var> of NULL is equivalent to specifiying a single key
which consists of all attributes, that is, the table will become all-key.

To enforce the key constraints, Duro creates a unique hash index
for each key.

@returns

On success, a pointer to the newly created table is returned.
If an error occurred, NULL is returned.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_TYPE_MISMATCH_ERROR
<dd>The type of a default value does not match the type of the corresponding
attribute.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd>One or more of the arguments are incorrect. For example, a key attribute
does not appear in <var>attrv</var>, etc.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
RDB_object *
RDB_create_table(const char *name,
                int attrc, const RDB_attr heading[],
                int keyc, const RDB_string_vec keyv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *tbtyp = RDB_create_relation_type(attrc, heading, ecp);
    if (tbtyp == NULL) {
        return NULL;
    }
    if (_RDB_set_defvals(tbtyp, attrc, heading, ecp) != RDB_OK) {
        RDB_drop_type(tbtyp, ecp, NULL);
        return NULL;
    }

    return RDB_create_table_from_type(name, tbtyp, keyc, keyv,
            ecp, txp);
}

/**
<strong>RDB_create_table_from_type</strong> acts like
RDB_create_table(), except that it takes
a RDB_type argument instead of attribute arguments.
*<var>reltyp</var> must be a relation type and will be managed
by the table created.
*/
RDB_object *
RDB_create_table_from_type(const char *name, RDB_type *reltyp,
                int keyc, const RDB_string_vec keyv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;

    if (name != NULL && !_RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid table name", ecp);
        return NULL;
    }

    if (reltyp->kind != RDB_TP_RELATION) {
        RDB_raise_type_mismatch("relation type required", ecp);
        return NULL;
    }

    for (i = 0; i < reltyp->var.basetyp->var.tuple.attrc; i++) {
        if (!_RDB_legal_name(reltyp->var.basetyp->var.tuple.attrv[i].name)) {
            RDB_object str;
        
            RDB_init_obj(&str);
            if (RDB_string_to_obj(&str, "invalid attribute name: ", ecp)
                    != RDB_OK) {
                RDB_destroy_obj(&str, ecp);
                return NULL;
            }

            if (RDB_append_string(&str,
                    reltyp->var.basetyp->var.tuple.attrv[i].name, ecp) != RDB_OK) {
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

    return create_table(name, reltyp, keyc, keyv, ecp, txp);
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
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_NOT_FOUND_ERROR
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

    tbp = _RDB_cat_get_rtable(name, ecp, txp);
    if (tbp != NULL)
        return tbp;
    errp = RDB_get_err(ecp);
    if (errp != NULL) {
        if (RDB_obj_type(errp) != &RDB_NOT_FOUND_ERROR)
            return NULL;
        RDB_clear_err(ecp);
    }

    return _RDB_cat_get_vtable(name, ecp, txp);
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
<dt>RDB_INVALID_TRANSACTION_ERROR
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

    if (tbp->kind == RDB_OB_TABLE && tbp->var.tb.is_persistent) {
        RDB_database *dbp;
        RDB_dbroot *dbrootp;

        if (!RDB_tx_is_running(txp)) {
            RDB_raise_invalid_tx(ecp);
            return RDB_ERROR;
        }
    
        /*
         * Remove table from all RDB_databases in list
         */
        dbrootp = (RDB_dbroot *) RDB_env_private(txp->envp);
        for (dbp = dbrootp->first_dbp; dbp != NULL; dbp = dbp->nextdbp) {
            RDB_object *foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->var.tb.name);
            if (foundtbp != NULL) {
                RDB_hashmap_put(&dbp->tbmap, tbp->var.tb.name, NULL);
            }
        }

        /*
         * Remove table from catalog
         */
        ret = _RDB_cat_delete(tbp, ecp, txp);
        if (ret != RDB_OK)
            return ret;

        /*
         * Delete recmap, if any
         */
        if (tbp->var.tb.stp != NULL) {
            ret = _RDB_delete_stored_table(tbp->var.tb.stp, ecp, txp);
            if (ret != RDB_OK)
                return RDB_ERROR;
            tbp->var.tb.stp = NULL;
        }
    }

    _RDB_free_obj(tbp, ecp);
    return RDB_OK;
}

/**
RDB_set_table_name sets the name of the table to <var>name</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

On success, RDB_OK is returned. On failure, RDB_ERROR is returned.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_INVALID_ARGUMENT_ERROR
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

    if (!_RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid table name", ecp);
        return RDB_ERROR;
    }

    /* !! should check if virtual tables depend on this table */

    if (tbp->var.tb.is_persistent) {
        RDB_database *dbp;

        /* Update catalog */
        ret = _RDB_cat_rename_table(tbp, name, ecp, txp);
        if (ret != RDB_OK) {
            return RDB_ERROR;
        }

        /* Delete and reinsert tables from/to table maps */
        for (dbp = txp->dbp->dbrootp->first_dbp; dbp != NULL;
                dbp = dbp->nextdbp) {
            RDB_object *foundtbp = RDB_hashmap_get(&dbp->tbmap, tbp->var.tb.name);
            if (foundtbp != NULL) {
                RDB_hashmap_put(&dbp->tbmap, tbp->var.tb.name, NULL);
                RDB_hashmap_put(&dbp->tbmap, name, tbp);
            }
        }
    }
    
    if (tbp->var.tb.name != NULL)
        RDB_free(tbp->var.tb.name);
    tbp->var.tb.name = RDB_dup_str(name);
    if (tbp->var.tb.name == NULL) {
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
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_INVALID_ARGUMENT_ERROR
<dd>The table does not have a name.
<dt>RDB_ELEMENT_EXIST_ERROR
<dd>The table is already associated with the database.
<dt>RDB_NOT_SUPPORTED_ERROR
<dd>The table is a local real table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
int
RDB_add_table(RDB_object *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    if (tbp->var.tb.name == NULL) {
        RDB_raise_invalid_argument("missing table name", ecp);
        return RDB_ERROR;
    }

    /* Turning a local real table into a persistent table is not supported */
    if (!tbp->var.tb.is_persistent && tbp->var.tb.exp == NULL) {
        RDB_raise_not_supported(
                "operation not supported for local real tables", ecp);
        return RDB_ERROR;
    }

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_cat_insert(tbp, ecp, txp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    ret = _RDB_assoc_table_db(tbp, txp->dbp, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    tbp->var.tb.is_persistent = RDB_TRUE;

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
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_INVALID_ARGUMENT_ERROR
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

/** @defgroup type Type functions
 * @{
 */

/** @struct RDB_possrep rdb.h <rel/rdb.h>
 * Specifies a possible representations.
 */

/**
RDB_get_type obtains a pointer to RDB_type structure which
represents the type with the name <var>name</var>
and stores that pointer at the location pointed to by <var>typp</var>.

@returns

On success, RDB_OK is returned. Any other return value indicates an error.

@par Errors:

<dl>
<dt>RDB_INVALID_TRANSACTION_ERROR
<dd><var>txp</var> does not point to a running transaction.
<dt>RDB_NOT_FOUND_ERROR
<dd>A type with the name <var>name</var> could not be found.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
RDB_type *
RDB_get_type(const char *name, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *typ;
    int ret;

    /*
     * search type in built-in type map
     */
    typ = RDB_hashmap_get(&_RDB_builtin_type_map, name);
    if (typ != NULL) {
        return typ;
    }

    if (txp == NULL) {
        RDB_raise_not_found(name, ecp);
        return NULL;
    }

    /*
     * search type in dbroot type map
     */
    typ = RDB_hashmap_get(&txp->dbp->dbrootp->typemap, name);
    if (typ != NULL) {
        return typ;
    }
    
    /*
     * search type in catalog
     */
    ret = _RDB_cat_get_type(name, ecp, txp, &typ);
    if (ret != RDB_OK)
        return NULL;

    /*
     * put type into type map
     */
    ret = RDB_hashmap_put(&txp->dbp->dbrootp->typemap, name, typ);
    if (ret != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    return typ;
}

/*@}*/
