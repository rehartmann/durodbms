#include "duro.h"
#include <gen/strfns.h>
#include <string.h>

int
table_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int ret;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    TclState *statep = (TclState *) data;

    const char *sub_cmds[] = {
        "create", "drop", NULL
    };
    enum table_ix {
        create_ix, drop_ix
    };
    int result, index;

    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "option ?arg ...?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj(interp, objv[1], sub_cmds, "option", 0, &index)
            != RDB_OK) {
        return TCL_ERROR;
    }

    switch (index) {
        case create_ix:
        {
            int attrc, keyc;
            RDB_attr *attrv;
            RDB_string_vec *keyv;
            int ival;
            RDB_bool persistent;
            int i;
            char *txstr;

            if (objc != 7) {
                Tcl_WrongNumArgs(interp, 2, objv, "name persistent attrs keys tx");
                return TCL_ERROR;
            }
            ret = Tcl_GetIntFromObj(interp, objv[3], &ival);
            persistent = (RDB_bool) (ival != 0);

            txstr = Tcl_GetStringFromObj(objv[6], NULL);
            entryp = Tcl_FindHashEntry(&statep->txs, txstr);
            if (entryp == NULL) {
                Tcl_AppendResult(interp, "Unknown transaction: ", attrv[2], NULL);
                return TCL_ERROR;
            }
            txp = Tcl_GetHashValue(entryp);

            ret = Tcl_ListObjLength(interp, objv[4], &attrc);
            attrv = malloc(sizeof (RDB_attr) * attrc);
            for (i = 0; i < attrc; i++) {
                Tcl_Obj *attrobjp, *nameobjp, *typeobjp;
            
                Tcl_ListObjIndex(interp, objv[4], i, &attrobjp);
                Tcl_ListObjIndex(interp, attrobjp, 0, &nameobjp);
                Tcl_ListObjIndex(interp, attrobjp, 1, &typeobjp);
                attrv[i].name = RDB_dup_str(Tcl_GetStringFromObj(nameobjp, NULL));
                ret = RDB_get_type(Tcl_GetStringFromObj(typeobjp, NULL),
                        txp, &attrv[i].typ);
                if (ret != RDB_OK) {
                    Tcl_AppendResult(interp, RDB_strerror(ret), NULL);
                    return TCL_ERROR;
                }
                attrv[i].defaultp = NULL;
                attrv[i].options = 0;
            }

            ret = Tcl_ListObjLength(interp, objv[5], &keyc);
            keyv = malloc(sizeof (RDB_string_vec) * keyc);
            /* ... */

            break;
        }
        case drop_ix:
            break;
    }
    return TCL_OK;
}
