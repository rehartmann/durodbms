#ifndef RDB_STRFNS_H
#define RDB_STRFNS_H

/*$Id$*/

#include <stdlib.h>

/*
 * Create a copy of string str on the heap, including the
 * terminating null byte.
 * Returns the copy or NULL if allocating the memory for the
 * copy failed.
 */
char *
RDB_dup_str(const char *strp);

/*
 * Duplicate a vector of strings.
 */
char **
RDB_dup_strvec(int cnt, char **srcv);

/*
 * Free a vector of strings.
 */
void
RDB_free_strvec(int cnt, char **strv);

/*
 * Split string into substrings separated by blanks.
 * Return the number of substrings.
 * If the operation fails because of insufficient memory, -1 is returned.
 */
int
RDB_split_str(const char *str, char *(*substrvp[]));

void
_RDB_dump(void *datap, size_t size);

#endif
