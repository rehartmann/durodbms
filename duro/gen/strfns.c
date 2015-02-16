/*
 * Copyright (C) 2003-2008, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "strfns.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

char *
RDB_dup_str(const char *str) {
    char *resp = malloc(strlen(str) + 1);
    
    if (resp == NULL)
        return NULL;
    
    return strcpy(resp, str);
}

void
RDB_free_strvec(int cnt, char **strv) {
    int i;

    for (i = 0; i < cnt; i++)
        free(strv[i]);
    free(strv);
}

char **
RDB_dup_strvec(int cnt, char **srcv) {
    int i;
    char **resv;
    
    resv = malloc(sizeof(char *) * cnt);
    if (resv == NULL)
        return NULL;

    for (i = 0; i < cnt; i++)
        resv[i] = NULL;

    for (i = 0; i < cnt; i++) {
        resv[i] = RDB_dup_str(srcv[i]);
        if (resv[i] == NULL) {
            RDB_free_strvec(cnt, resv);
            return NULL;
        }
    }
    
    return resv;
}

int
RDB_find_str(int strc, char *strv[], const char *str)
{
    int i;
    
    for (i = 0; i < strc; i++) {
        if (strcmp(strv[i], str) == 0)
            return i;
    }
    return -1;
}

unsigned
RDB_hash_str(const char *str)
{
    unsigned hash = 5381;
    int c;

    while ((c = *str++) != '\0')
        hash = (hash * 33) ^ c;

    return hash;
}
