/* $Id$ */

#include <rel/rdb.h>
#include <stdio.h>

int main() {
    RDB_value val;
    char buf[9];

    RDB_init_value(&val);

    RDB_binary_set(&val, 0, "ABCD", 4);
    RDB_binary_set(&val, 4, "EFGH", 5);

    RDB_binary_get(&val, 0, buf, 9);

    printf("%s\n%d\n", buf, RDB_binary_get_length(&val));

    RDB_deinit_value(&val);

    return 0;
}
