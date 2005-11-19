/* $Id$ */

#include <rel/rdb.h>
#include <stdlib.h>
#include <stdio.h>

int
print_extend(RDB_table *vtbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object array;
    RDB_object *tplp;
    int ret;
    int i;
    RDB_seq_item sq;

    RDB_init_obj(&array);

    sq.attrname = "SALARY_AFTER_TAX";
    sq.asc = RDB_TRUE;

    ret = RDB_table_to_array(&array, vtbp, 1, &sq, ecp, txp);
    if (ret != RDB_OK) {
        goto error;
    }
    
    for (i = 0; (tplp = RDB_array_get(&array, i, ecp)) != NULL; i++) {
        printf("EMPNO: %d\n", (int)RDB_tuple_get_int(tplp, "EMPNO"));
        printf("NAME: %s\n", RDB_tuple_get_string(tplp, "NAME"));
        printf("SALARY: %f\n", (float)RDB_tuple_get_rational(tplp, "SALARY"));
        printf("SALARY_AFTER_TAX: %f\n",
                (float)RDB_tuple_get_rational(tplp, "SALARY_AFTER_TAX"));
        printf("NAME_LEN: %d\n", (int)RDB_tuple_get_int(tplp, "NAME_LEN"));
    }
    /* !!
    if (ret != RDB_NOT_FOUND) {
        RDB_rollback(ecp, txp);
        goto error;
    }
    */
    RDB_clear_err(ecp);

    RDB_destroy_obj(&array, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&array, ecp);
    return RDB_ERROR;
}

int
insert_extend(RDB_table *vtbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;

    RDB_init_obj(&tpl);    

    printf("Inserting tuple #1\n");

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 3, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Johnson", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY_AFTER_TAX",
            (RDB_rational)4200.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "NAME_LEN", (RDB_int)7, ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(vtbp, &tpl, ecp, txp);
    if (ret != RDB_OK) {
        RDB_type *errtyp = RDB_obj_type(RDB_get_err(ecp));
        if (errtyp == &RDB_PREDICATE_VIOLATION_ERROR) {
            printf("Error: predicate violation - OK\n");
        } else {
            printf("Error: %s\n", errtyp->name);
        }
    }

    printf("Inserting tuple #2\n");

    ret = RDB_tuple_set_int(&tpl, "EMPNO", 3, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "NAME", "Johnson", ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY", (RDB_rational)4000.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "DEPTNO", 1, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_rational(&tpl, "SALARY_AFTER_TAX",
            (RDB_rational)-100.0, ecp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "NAME_LEN", (RDB_int)7, ecp);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(vtbp, &tpl, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    RDB_destroy_obj(&tpl, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

int
test_extend(RDB_database *dbp, RDB_exec_context *ecp)
{
    RDB_transaction tx;
    RDB_table *tbp;
    RDB_expression *exp;
    int ret;
    RDB_table *vtbp = NULL;
    RDB_virtual_attr extend[] = {
        { "SALARY_AFTER_TAX", NULL },
        { "NAME_LEN", NULL }
    };

    printf("Starting transaction\n");
    ret = RDB_begin_tx(ecp, &tx, dbp, NULL);
    if (ret != RDB_OK) {
        return ret;
    }

    extend[0].exp = RDB_ro_op_va("-", ecp, RDB_expr_attr("SALARY", ecp),
            RDB_rational_to_expr(4100, ecp), (RDB_expression *) NULL);
    if (extend[0].exp == NULL)
        return RDB_ERROR;

    exp = RDB_expr_attr("NAME", ecp);
    if (exp == NULL) {
        ret = RDB_ERROR;
        goto error;
    }
    extend[1].exp = RDB_ro_op_va("LENGTH", ecp, exp, (RDB_expression *) NULL);
    if (extend[1].exp == NULL) {
        ret = RDB_ERROR;
        goto error;
    }

    tbp = RDB_get_table("EMPS1", ecp, &tx);
    if (tbp == NULL) {
        goto error;
    }

    printf("Extending EMPS1 (SALARY_AFTER_TAX,NAME_LEN)\n");

    vtbp = RDB_extend(tbp, 2, extend, ecp, &tx);
    if (vtbp == NULL) {
        goto error;
    }

    printf("Converting extended table to array\n");
    ret = print_extend(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    ret = insert_extend(vtbp, ecp, &tx);
    if (ret != RDB_OK) {
        goto error;
    }

    printf("Dropping extension\n");
    RDB_drop_table(vtbp, ecp, &tx);

    printf("End of transaction\n");
    /* Abort transaction, since we don't want the update to be persistent */
    return RDB_rollback(ecp, &tx);

error:
    if (vtbp != NULL)
        RDB_drop_table(vtbp, ecp, &tx);

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
    ret = RDB_open_env("dbenv", &envp);
    if (ret != 0) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 1;
    }

    RDB_set_errfile(envp, stderr);

    RDB_init_exec_context(&ec);
    dbp = RDB_get_db_from_env("TEST", envp, &ec);
    if (dbp == NULL) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        return 1;
    }

    ret = test_extend(dbp, &ec);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        goto error;
    }
    RDB_destroy_exec_context(&ec);

    printf ("Closing environment\n");
    ret = RDB_close_env(envp);
    if (ret != RDB_OK) {
        fprintf(stderr, "Error: %s\n", db_strerror(ret));
        return 2;
    }

    return 0;

error:
    printf ("Closing environment\n");
    RDB_destroy_exec_context(&ec);
    RDB_close_env(envp);
    return 2;
}
