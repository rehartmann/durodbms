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
    { emp_keyattrs1, 1 },
    { emp_keyattrs2, 1 }
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
    int err;
    
    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return err;
    }

    printf("Creating table EMPS1\n");
    err = RDB_create_table("EMPS1", RDB_TRUE, 4, emp_attrs, 2, emp_keyattrs,
                           &tx, &tbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Creating table EMPS2\n");
    err = RDB_create_table("EMPS2", RDB_TRUE, 4, emp_attrs, 2, emp_keyattrs,
                           &tx, &tbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Creating table DEPTS\n");
    key.attrv = dept_key_attrs;
    key.attrc = 1;
    err = RDB_create_table("DEPTS", RDB_TRUE, 2, dept_attrs, 1, &key, &tx, &tbp);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    err = RDB_commit(&tx);
    return err;
}

int
fill_tables(RDB_database *dbp)
{
    int err;
    RDB_tuple deptpl, emptpl;
    RDB_transaction tx;
    RDB_table *tbp, *tbp2, *tbp3;

    RDB_init_tuple(&emptpl);

    RDB_init_tuple(&deptpl);

    err = RDB_begin_tx(&tx, dbp, NULL);
    if (err != RDB_OK) {
        goto error;
    }

    RDB_get_table(dbp, "EMPS1", &tbp);
    RDB_get_table(dbp, "EMPS2", &tbp2);
    RDB_get_table(dbp, "DEPTS", &tbp3);

    printf("Filling EMPS1\n");

    err = RDB_tuple_set_int(&emptpl, "EMPNO", 1);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_string(&emptpl, "NAME", "Smith");
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_rational(&emptpl, "SALARY", (RDB_rational)4000.0);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_int(&emptpl, "DEPTNO", 1);
    if (err != RDB_OK)
        goto error;

    err = RDB_insert(tbp, &emptpl, &tx);
    if (err != RDB_OK) {
        goto error;
    }

    err = RDB_tuple_set_int(&emptpl, "EMPNO", 2);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_string(&emptpl, "NAME", "Jones");
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_rational(&emptpl, "SALARY", (RDB_rational)4100.0);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_int(&emptpl, "DEPTNO", 2);
    if (err != RDB_OK)
        goto error;

    err = RDB_insert(tbp, &emptpl, &tx);
    if (err != RDB_OK) {
        goto error;
    }

    printf("Filling EMPS2\n");

    err = RDB_tuple_set_int(&emptpl, "EMPNO", 1);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_string(&emptpl, "NAME", "Smith");
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_rational(&emptpl, "SALARY", (RDB_rational)4000.0);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_int(&emptpl, "DEPTNO", 1);
    if (err != RDB_OK)
        goto error;

    err = RDB_insert(tbp2, &emptpl, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    err = RDB_tuple_set_int(&emptpl, "EMPNO", 3);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_string(&emptpl, "NAME", "Clarke");
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_rational(&emptpl, "SALARY", (RDB_rational)4000.0);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_int(&emptpl, "DEPTNO", 2);
    if (err != RDB_OK)
        goto error;

    err = RDB_insert(tbp2, &emptpl, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    printf("Filling DEPTS\n");

    err = RDB_tuple_set_int(&deptpl, "DEPTNO", 1);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_string(&deptpl, "DEPTNAME", "Dept. I");
    if (err != RDB_OK)
        goto error;

    err = RDB_insert(tbp3, &deptpl, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    err = RDB_tuple_set_int(&deptpl, "DEPTNO", 2);
    if (err != RDB_OK)
        goto error;
    err = RDB_tuple_set_string(&deptpl, "DEPTNAME", "Dept. II");
    if (err != RDB_OK)
        goto error;

    err = RDB_insert(tbp3, &deptpl, &tx);
    if (err != RDB_OK) {
        RDB_rollback(&tx);
        return err;
    }

    RDB_destroy_tuple(&emptpl);
    RDB_destroy_tuple(&deptpl);

    return RDB_commit(&tx);

error:
    RDB_destroy_tuple(&emptpl);
    RDB_destroy_tuple(&deptpl);
    
    return err;
}

int
main() {
    RDB_environment *dsp;
    RDB_database *dbp;
    int err;
    
    printf("Creating environment\n");
    err = RDB_create_env("db", 0, &dsp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }
    printf("Creating DB\n");
    err = RDB_create_db("TEST", dsp, &dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 1;
    }

    err = create_tables(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    err = fill_tables(dbp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    printf ("Closing environment\n");
    err = RDB_close_env(dsp);
    if (err != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(err));
        return 2;
    }

    return 0;
}
