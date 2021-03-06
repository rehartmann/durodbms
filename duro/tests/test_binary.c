/* $Id$ */

#include <rel/rdb.h>
#include <stdio.h>

int
main(void)
{
    RDB_object val;
    void *bufp;
    size_t len;
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);
    RDB_init_obj(&val);

    RDB_binary_set(&val, 0, "ABCD", 4, &ec);
    RDB_binary_set(&val, 4, "EFGH", 5, &ec);

    RDB_binary_get(&val, 0, 9, &ec, &bufp, &len);

    printf("%s\n%d\n%d\n", (char *) bufp, (int) RDB_binary_length(&val),
            (int) len);

    RDB_destroy_obj(&val, &ec);
    RDB_destroy_exec_context(&ec);

    return 0;
}
