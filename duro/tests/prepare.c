/* $Id$ */

#include <rel/rdb.h>
#include <stdio.h>

RDB_attr emp_attrs[] = {
    {"EMPNO", &RDB_INTEGER, NULL, 0 },
    {"NAME", &RDB_STRING, NULL, 0 },
    {"SALARY", &RDB_RATIONAL, NULL, 0 },
    {"DEPTNO", &RDB_INTEGER, NULL, 0 }
};

char *emp_keyattrs1[] = { "EMPNO" };
char *emp_keyattrs2[] = { "NAME" };

RDB_key_attrs emp_keyattrs[] = {
    { 1, emp_keyattrs1 },
    { 1, emp_keyattrs2 }
};

RDB_attr dept_attrs[] = {
    {"DEPTNO", &RDB_INTEGER, NULL, 0 },
    {"DEPTNAME", &RDB_STRING, NULL, 0 }
};

char *dept_key_attrs[] = { "DEPTNO" };

int
create_tables(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_key_attrs key;
    int ret;
    
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return ret;
    }

    printf("Creating table EMPS1\n");
    ret = RDB_create_table("EMPS1", RDB_TRUE, 4, emp_attrs, 2, emp_keyattrs,
                           &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating table EMPS2\n");
    ret = RDB_create_table("EMPS2", RDB_TRUE, 4, emp_attrs, 2, emp_keyattrs,
                           &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Creating table DEPTS\n");
    key.attrv = dept_key_attrs;
    key.attrc = 1;
    ret = RDB_create_table("DEPTS", RDB_TRUE, 2, dept_attrs, 1, &key, &tx, &tbp);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_commit(&tx);
    return ret;
}

int
fill_tables(RDB_database *dbp)
{
    int ret;
    RDB_tuple deptpl, emptpl;
    RDB_transaction tx;
    RDB_table *tbp, *tbp2, *tbp3;

    RDB_init_tuple(&emptpl);

    RDB_init_tuple(&deptpl);

    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        goto error;
    }

    RDB_get_table("EMPS1", &tx, &tbp);
    RDB_get_table("EMPS2", &tx, &tbp2);
    RDB_get_table("DEPTS", &tx, &tbp3);

    printf("Filling EMPS1\n");

    ret = RDB_tuple_set_int(&emptpl, "EMPNO", 1);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&emptpl, "NAME", "Smith");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&emptpl, "SALARY", (RDB_rational)4000.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&emptpl, "DEPTNO", 1);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp, &emptpl, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = RDB_tuple_set_int(&emptpl, "EMPNO", 2);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&emptpl, "NAME", "Jones");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&emptpl, "SALARY", (RDB_rational)4100.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&emptpl, "DEPTNO", 2);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp, &emptpl, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Filling EMPS2\n");

    ret = RDB_tuple_set_int(&emptpl, "EMPNO", 1);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&emptpl, "NAME", "Smith");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&emptpl, "SALARY", (RDB_rational)4000.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&emptpl, "DEPTNO", 1);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp2, &emptpl, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_tuple_set_int(&emptpl, "EMPNO", 3);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&emptpl, "NAME", "Clarke");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&emptpl, "SALARY", (RDB_rational)4000.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&emptpl, "DEPTNO", 2);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp2, &emptpl, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    printf("Filling DEPTS\n");

    ret = RDB_tuple_set_int(&deptpl, "DEPTNO", 1);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&deptpl, "DEPTNAME", "Dept. I");
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp3, &deptpl, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    ret = RDB_tuple_set_int(&deptpl, "DEPTNO", 2);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&deptpl, "DEPTNAME", "Dept. II");
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(tbp3, &deptpl, &tx);
    if (ret != RDB_OK) {
        RDB_rollback(&tx);
        return ret;
    }

    RDB_destroy_tuple(&emptpl);
    RDB_destroy_tuple(&deptpl);

    return RDB_commit(&tx);

error:
    RDB_destroy_tuple(&emptpl);
    RDB_destroy_tuple(&deptpl);
    
    return ret;
}

int
main(void) {
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    
    printf("Creating environment\n");
    ret = RDB_create_env("db", 0, &envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }
    printf("Creating DB\n");
    ret = RDB_create_db_from_env("TEST", envp, &dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = create_tables(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    ret = fill_tables(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    printf ("Closing environment\n");
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;
}
