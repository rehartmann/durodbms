#include <rel/rdb.h>
#include <bdbrec/bdbenv.h>
#include <db.h>

#include <stdio.h>
#include <assert.h>

int
create_tables(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_object *tbp;
    RDB_string_vec key;
    int ret;
    
    RDB_attr emp_attrs[] = {
        {"EMPNO", &RDB_INTEGER, NULL, 0 },
        {"NAME", &RDB_STRING, NULL, 0 },
        {"SALARY", &RDB_FLOAT, NULL, 0 },
        {"DEPTNO", &RDB_INTEGER, NULL, 0 }
    };

    char *emp_keyattrs1[] = { "EMPNO" };
    char *emp_keyattrs2[] = { "NAME" };

    RDB_string_vec emp_keyattrs[] = {
        { 1, emp_keyattrs1 },
        { 1, emp_keyattrs2 }
    };

    RDB_attr dept_attrs[] = {
        {"DEPTNO", &RDB_INTEGER, NULL, 0 },
        {"DEPTNAME", &RDB_STRING, NULL, 0 }
    };

    char *dept_key_attrs[] = { "DEPTNO" };

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(ecp))));
        return ret;
    }

    printf("Creating table EMPS1\n");
    tbp = RDB_create_table("EMPS1", 4, emp_attrs, 2, emp_keyattrs,
                           ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    printf("Creating table EMPS2\n");
    
    /* Set default value for SALARY */
    emp_attrs[2].defaultp = RDB_float_to_expr((RDB_float) 4000.0, ecp);

    tbp = RDB_create_table("EMPS2", 4, emp_attrs, 2, emp_keyattrs,
                           ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    printf("Creating table depts\n");
    key.strv = dept_key_attrs;
    key.strc = 1;
    tbp = RDB_create_table("depts", 2, dept_attrs, 1, &key, ecp, &tx);
    if (tbp == NULL) {
        RDB_rollback(ecp, &tx);
        return RDB_ERROR;
    }

    return RDB_commit(ecp, &tx);
}

int
fill_tables(RDB_database *dbp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object deptpl, emptpl;
    RDB_transaction tx;
    RDB_object *tbp, *tbp2, *tbp3;

    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    RDB_init_obj(&emptpl);
    RDB_init_obj(&deptpl);

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    assert(tbp != NULL);
    tbp2 = RDB_get_table("EMPS2", ecp, &tx);
    assert(tbp2 != NULL);
    tbp3 = RDB_get_table("depts", ecp, &tx);
    assert(tbp3 != NULL);

    printf("Filling EMPS1\n");

    ret = RDB_tuple_set_int(&emptpl, "EMPNO", 1, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&emptpl, "NAME", "Smith", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_float(&emptpl, "SALARY", (RDB_float)4000.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&emptpl, "DEPTNO", 1, ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp, &emptpl, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_tuple_set_int(&emptpl, "EMPNO", 2, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&emptpl, "NAME", "Jones", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_float(&emptpl, "SALARY", (RDB_float)4100.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&emptpl, "DEPTNO", 2, ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp, &emptpl, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Filling EMPS2\n");

    RDB_destroy_obj(&emptpl, ecp);
    RDB_init_obj(&emptpl);

    ret = RDB_tuple_set_int(&emptpl, "EMPNO", 1, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&emptpl, "NAME", "Smith", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&emptpl, "DEPTNO", 1, ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp2, &emptpl, ecp, &tx);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_tuple_set_int(&emptpl, "EMPNO", 3, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&emptpl, "NAME", "Clarke", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&emptpl, "DEPTNO", 2, ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp2, &emptpl, ecp, &tx);
    if (ret != RDB_OK)
        goto error;

    printf("Filling depts\n");

    ret = RDB_tuple_set_int(&deptpl, "DEPTNO", 1, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&deptpl, "DEPTNAME", "Dept. I", ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp3, &deptpl, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_tuple_set_int(&deptpl, "DEPTNO", 2, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&deptpl, "DEPTNAME", "Dept. II", ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp3, &deptpl, ecp, &tx);
    if (ret != RDB_OK)
        goto error;

    RDB_destroy_obj(&emptpl, ecp);
    RDB_destroy_obj(&deptpl, ecp);

    return RDB_commit(ecp, &tx);

error:
    RDB_destroy_obj(&emptpl, ecp);
    RDB_destroy_obj(&deptpl, ecp);
    
    RDB_rollback(ecp, &tx);

    return RDB_ERROR;
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    RDB_exec_context ec;

    printf("Opening environment\n");
    ret = RDB_create_env("dbenv", &envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_bdb_env(envp)->set_errfile(RDB_bdb_env(envp), stderr);
    RDB_init_exec_context(&ec);

    printf("Creating DB\n");
    dbp = RDB_create_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n",
                RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 1;
    }

    ret = create_tables(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    ret = fill_tables(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    printf ("Closing environment\n");
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        RDB_destroy_exec_context(&ec);
        return 2;
    }

    RDB_destroy_exec_context(&ec);
    return 0;
}
