/* $Id$ */

#include <rel/rdb.h>
#include <stdio.h>

int main() {
    const void *datap;
    int i;
    int res;
    RDB_tuple tpl;

    RDB_init_tuple(&tpl);
    RDB_deinit_tuple(&tpl);

    RDB_init_tuple(&tpl);
    res = RDB_tuple_set_string(&tpl, "A", "Aaa");
    if (res != RDB_OK) {
        fprintf(stderr, "Error %s\n", RDB_strerror(res));
        RDB_deinit_tuple(&tpl);
        return 2;
    }
    res = RDB_tuple_set_int(&tpl, "B", (RDB_int)4711);
    if (res != RDB_OK) {
        fprintf(stderr, "Error %s\n", RDB_strerror(res));
        RDB_deinit_tuple(&tpl);
        return 2;
    }
    
    datap = RDB_tuple_get_string(&tpl, "A");
    printf("%s -> %s\n", "A", (char *)datap);

    i = RDB_tuple_get_int(&tpl, "B");
    printf("%s -> %d\n", "B", i);
    
    RDB_deinit_tuple(&tpl);
    
    return 0;
}
