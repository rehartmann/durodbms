/* $Id$ */

#include "duro.h"
#include <string.h>

int
Duro_operator_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    RDB_bool update;
    /* TclState *statep = (TclState *) data; */

    if (objc != 7) {
        Tcl_WrongNumArgs(interp, 1, objv, "name opt returnsOrUpdates tx args body");
        return TCL_ERROR;
    }
    if (strcmp (Tcl_GetStringFromObj(objv[2], NULL), "-updates") == 0)
        update = RDB_TRUE;
    if (strcmp (Tcl_GetStringFromObj(objv[2], NULL), "-returns") == 0)
        update = RDB_FALSE;
    else
        Tcl_SetResult(interp, "invalid option, must be -updates or -returns",
                TCL_STATIC);

    return TCL_OK;
}
