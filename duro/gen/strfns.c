/*$Id$*/

#include "strfns.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

char *RDB_dup_str(const char *str) {
    char *resp = malloc(strlen(str) + 1);
    
    if (resp == NULL)
        return NULL;
    
    return strcpy(resp, str);
}

void RDB_free_strvec(int cnt, char **strv) {
    int i;

    for (i = 0; i < cnt; i++)
        free(strv[i]);
    free(strv);
}

char **RDB_dup_strvec(int cnt, char **srcv) {
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
RDB_split_str(const char *str, char *(*substrvp[]))
{
    int i;
    int slen = strlen(str);
    int attrc;

    if (str[0] == '\0') {
        attrc = 0;
    } else {
        const char *sp;
        const char *ep;
    
        attrc = 1;
        for (i = 0; i < slen; i++) {
            if (str[i] == ' ')
                attrc++;
        }
        *substrvp = malloc(sizeof(char *) * attrc);
        if (*substrvp == NULL)
            return -1;

        for (i = 0; i < attrc; i++)
            (*substrvp)[i] = NULL;

        sp = str;
        for (i = 0; sp != NULL; i++) {
            ep = strchr(sp, ' ');
            if (ep != NULL) {
                ep++;
                (*substrvp)[i] = malloc(ep - sp + 1);
                if ((*substrvp)[i] == NULL)
                    goto error;
                strncpy((*substrvp)[i], sp, ep - sp);
                (*substrvp)[i][ep - sp] = '\0';
            } else {
                (*substrvp)[i] = RDB_dup_str(sp);
                if ((*substrvp)[i] == NULL)
                    goto error;
            }
            sp = ep;
        }
    }
    return attrc;
error:
    for (i = 0; i < attrc; i++)
        free((*substrvp)[i]);
    free(*substrvp);
    return -1;
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

void
_RDB_dump(void *datap, size_t size)
{
    int i;
    char *cp = (char*)datap;

    for (i = 0; i < size; i++)
        printf("%d ", (int)cp[i]);
    printf("\n");
}
