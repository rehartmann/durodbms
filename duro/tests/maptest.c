/* $Id */

#include <gen/hashmap.h>
#include <stdio.h>

int
main(void)
{
    int i;
    RDB_hashmap map;

    RDB_init_hashmap(&map, 3);

    i = 1;
    RDB_hashmap_put(&map, "A", &i, sizeof(i));
    i = 2;
    RDB_hashmap_put(&map, "B", &i, sizeof(i));
    i = 3;
    RDB_hashmap_put(&map, "C", &i, sizeof(i));
    i = 4;
    RDB_hashmap_put(&map, "D", &i, sizeof(i));

    printf("A -> %d\n", *(int *)RDB_hashmap_get(&map, "A", NULL));
    printf("B -> %d\n", *(int *)RDB_hashmap_get(&map, "B", NULL));
    printf("C -> %d\n", *(int *)RDB_hashmap_get(&map, "C", NULL));
    printf("D -> %d\n", *(int *)RDB_hashmap_get(&map, "D", NULL));
    
    RDB_destroy_hashmap(&map);
    
    return 0;
}
