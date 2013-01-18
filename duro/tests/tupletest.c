/* $Id$ */

#include <rel/rdb.h>
#include <stdio.h>

int main(void)
{
    const void *datap;
    int i;
    int res;
    RDB_exec_context ec;
    RDB_object tpl;

    RDB_init_exec_context(&ec);
    if (RDB_init_builtin_types(&ec) != RDB_OK) {
        fprintf(stderr, "error initializing built-in types\n");
        return 2;
    }

    RDB_init_obj(&tpl);
    RDB_destroy_obj(&tpl, &ec);

    RDB_init_obj(&tpl);
    res = RDB_tuple_set_string(&tpl, "A", "Aaa", &ec);
    if (res != RDB_OK) {
        fprintf(stderr, "Error %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_obj(&tpl, &ec);
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    res = RDB_tuple_set_int(&tpl, "B", (RDB_int)4711, &ec);
    if (res != RDB_OK) {
        fprintf(stderr, "Error %s\n", RDB_type_name(RDB_obj_type(RDB_get_err(&ec))));
        RDB_destroy_obj(&tpl, &ec);
        RDB_destroy_exec_context(&ec);
        return 2;
    }
    
    datap = RDB_tuple_get_string(&tpl, "A");
    printf("%s -> %s\n", "A", (char *)datap);

    i = RDB_tuple_get_int(&tpl, "B");
    printf("%s -> %d\n", "B", i);
    
    RDB_destroy_obj(&tpl, &ec);

    RDB_destroy_exec_context(&ec);
    return 0;
}
