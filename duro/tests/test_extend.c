/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
print_extend(RDB_table *vtbp, RDB_transaction *txp)
{
    RDB_array array;
    RDB_tuple tpl;
    int ret;
    int i;
    RDB_seq_item sq;

    RDB_init_array(&array);

    sq.attrname = "SALARY_AFTER_TAX";
    sq.asc = RDB_TRUE;

    ret = RDB_table_to_array(vtbp, &array, 1, &sq, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    RDB_init_tuple(&tpl);    
    for (i = 0; (ret = RDB_array_get_tuple(&array, i, &tpl)) == RDB_OK; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(&tpl, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(&tpl, "NAME"));
        printf("SALARY: %f\n", (float)RDB_tuple_get_rational(&tpl, "SALARY"));
        printf("SALARY_AFTER_TAX: %f\n",
                (float)RDB_tuple_get_rational(&tpl, "SALARY_AFTER_TAX"));
        printf("NAME_LEN: %d\n", (int)RDB_tuple_get_int(&tpl, "NAME_LEN"));
    }
    RDB_destroy_tuple(&tpl);
    if (ret != RDB_NOT_FOUND) {
        RDB_rollback(txp);
        goto error;
    }

    RDB_destroy_array(&array);
    return RDB_OK;
error:
    RDB_destroy_array(&array);
    return ret;
}

int
insert_extend(RDB_table *vtbp, RDB_transaction *txp)
{
    RDB_tuple tpl;
    int ret;

    RDB_init_tuple(&tpl);    

    printf("Inserting tuple #1\n");

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 3);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Johnson");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY_AFTER_TAX", (RDB_rational)4200.0);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(vtbp, &tpl, txp);
    if (ret == RDB_PREDICATE_VIOLATION) {
        printf("Return code: %s - OK\n", RDB_strerror(ret));
    } else {
        printf("Error: %s\n", RDB_strerror(ret));
    }

    printf("Inserting tuple #2\n");

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 3);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Johnson");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY_AFTER_TAX", (RDB_rational)-100.0);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "NAME_LEN", (RDB_int)7);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(vtbp, &tpl, txp);
    if (ret != RDB_OK)
        goto error;

    RDB_destroy_tuple(&tpl);
    return RDB_OK;

error:
    RDB_destroy_tuple(&tpl);
    return ret;
}

int
test_extend(RDB_database *dbp)
{
    RDB_transaction tx;
    RDB_table *tbp, *vtbp;
    int ret;
    RDB_virtual_attr extend[] = {
        { "SALARY_AFTER_TAX", NULL },
        { "NAME_LEN", NULL }
    };

    extend[0].exp = RDB_subtract(RDB_expr_attr("SALARY", &RDB_RATIONAL),
                         RDB_rational_const(4100));
    extend[1].exp = RDB_strlen(RDB_expr_attr("NAME", &RDB_STRING));

    printf("Starting transaction\n");
    ret = RDB_begin_tx(&tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    ret = RDB_get_table("EMPS1", &tx, &tbp);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Extending EMPS1 (SALARY_AFTER_TAX)\n");

    ret = RDB_extend(tbp, 2, extend, &vtbp);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Converting extended table to array\n");
    ret = print_extend(vtbp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = insert_extend(vtbp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Dropping extension\n");
    RDB_drop_table(vtbp, &tx);

    printf("End of transaction\n");
    /* Abort transaction, since we don't want the update to be persistent */
    return RDB_rollback(&tx);

error:
    printf("Dropping extension\n");
    RDB_drop_table(vtbp, &tx);

    RDB_rollback(&tx);
    return ret;
}

int
main(void)
{
    RDB_environment *envp;
    RDB_database *dbp;
    int ret;
    
    printf("Opening environment\n");
    ret = RDB_open_env("dbenv", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    RDB_set_errfile(envp, stderr);

    ret = RDB_get_db_from_env("TEST", envp, &dbp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 1;
    }

    ret = test_extend(dbp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        goto error;
    }
    
    printf ("Closing environment\n");
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_strerror(ret));
        return 2;
    }

    return 0;

error:
    printf ("Closing environment\n");
    RDB_close_env(envp);
    return 2;
}
