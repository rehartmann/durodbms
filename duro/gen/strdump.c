/*
 * strdump.c
 *
 *  Created on: 13.01.2013
 *      Author: rene
 */

#include "strdump.h"

void
RDB_dump(void *datap, size_t size, FILE *fp)
{
    int i;
    char *cp = (char*)datap;

    for (i = 0; i < size; i++)
        fprintf(fp, "%d ", (int)cp[i]);
    fputs("\n", fp);
}
