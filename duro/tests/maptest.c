/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <gen/hashmap.h>
#include <stdio.h>

int
main(void)
{
    int n[] = { 1, 2, 3, 4};
    RDB_hashmap map;

    RDB_init_hashmap(&map, 3);

    RDB_hashmap_put(&map, "A", &n[0]);
    RDB_hashmap_put(&map, "B", &n[1]);
    RDB_hashmap_put(&map, "C", &n[2]);
    RDB_hashmap_put(&map, "D", &n[3]);

    if (*(int *)RDB_hashmap_get(&map, "A") != 1) {
        fprintf(stderr, "wrong value for key \"A\"\n");
        return 1;
    }
    if (*(int *)RDB_hashmap_get(&map, "B") != 2) {
        fprintf(stderr, "wrong value for key \"A\"\n");
        return 1;
    }
    if (*(int *)RDB_hashmap_get(&map, "C") != 3) {
        fprintf(stderr, "wrong value for key \"A\"\n");
        return 1;
    }
    if (*(int *)RDB_hashmap_get(&map, "D") != 4) {
        fprintf(stderr, "wrong value for key \"A\"\n");
        return 1;
    }
    
    RDB_destroy_hashmap(&map);
    
    return 0;
}
