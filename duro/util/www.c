/*
 * www.c
 *
 *  Created on: 31.12.2012
 *      Author: Rene Hartmann
 */

#include <gen/strfns.h>
#include <rel/rdb.h>
#include <rel/typeimpl.h>

#include <stdlib.h>
#include <string.h>

static void
urldecode (char *srcp)
{
    char buf[] = "  ";
    char *dstp = srcp;
    long pc;

    for(;;) {
        switch(*srcp) {
            case '+':
                *dstp++ = ' ';
                srcp++;
                break;
            case '%':
                /* Check for invalid percent encoding */
                if (srcp[1] == '\0' || srcp[2] == '\0') {
                    *dstp++ = *srcp++;
                    return;
                }
                buf[0] = srcp[1];
                buf[1] = srcp[2];
                pc = strtol(buf, NULL, 16);
                *dstp++ = (char) pc;
                if (pc == 0)
                    return;
                srcp += 3;
                break;
            case '\0':
                *dstp = '\0';
                return;
            default:
                *dstp++ = *srcp++;
        }
    }
}

/*
 * Convert WWW form data to a tuple.
 * For each name/value pair set the value of the corresponding tuple attribute
 * to the value of the pair.
 * A tuple attribute corresponds to a pair if the attribute name is either
 * equal to the name of the pair, or to the name of the pair converted to lowercase.
 * If no such attribute is found, the pair is ignored.
 * If the tuple value is of type integer or float, convert the value
 * using atoi() (integer) or atof() (float).
 */
int
RDB_net_form_to_tuple(RDB_object *tplp, const char *srcp,
        RDB_exec_context *ecp)
{
    char *startp, *endp;
    RDB_object *objp;
    RDB_bool lastpair = RDB_FALSE;

    /* Make a copy so the delimiters can be safely replaced by nulls */
    char *datap = RDB_dup_str(srcp);
    if (datap == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    startp = datap;
    for (;;) {
        /* Get name */
        endp = strchr(startp, '=');
        if (endp == NULL)
            break;
        *endp = '\0';

        objp = RDB_tuple_get(tplp, startp);

        /* Get value */
        startp = endp + 1;
        endp = strchr(startp, '&');
        if (endp == NULL) /* End of data reached */ {
            endp = strchr(startp, '\0');
            lastpair = RDB_TRUE;
        } else {
            *endp = '\0';
        }

        if (objp != NULL) {
            RDB_type *typ = RDB_obj_type(objp);
            urldecode(startp);

            if (typ == &RDB_STRING) {
                if (RDB_string_to_obj(objp, startp, ecp) != RDB_OK)
                    goto error;
            } else if (typ == &RDB_INTEGER) {
                RDB_int_to_obj(objp, (RDB_int) atoi(startp));
            } else if (typ == &RDB_FLOAT) {
                RDB_float_to_obj(objp, (RDB_float) atof(startp));
            }
        }
        if (lastpair)
            break;
        startp = endp + 1;
    }
    RDB_free(datap);
    return RDB_OK;

error:
    RDB_free(datap);
    return RDB_ERROR;
}

int
RDB_net_hescape(RDB_object *dstp, const char *srcp,
        RDB_exec_context *ecp)
{
    const char *cp;
    int ret;

    if (RDB_string_to_obj(dstp, "", ecp) != RDB_OK)
        return RDB_ERROR;

    for (cp = srcp; *cp != '\0'; cp++) {
        switch (*cp) {
        case '<':
            ret = RDB_append_string(dstp, "&lt;", ecp);
            break;
        case '>':
            ret = RDB_append_string(dstp, "&gt;", ecp);
            break;
        case '&':
            ret = RDB_append_string(dstp, "&amp;", ecp);
            break;
        case '\'':
            ret = RDB_append_string(dstp, "&#39;", ecp);
            break;
        case '"':
            ret = RDB_append_string(dstp, "&quot;", ecp);
            break;
        default:
            ret = RDB_append_char(dstp, *cp, ecp);
        }
        if (ret != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}
