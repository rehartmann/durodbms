#include "tabletostr.h"
#include <string.h>

static int
append_str(RDB_object *objp, const char *str)
{
    int len = objp->var.bin.len + strlen(str);
    char *nstr = realloc(objp->var.bin.datap, len);
    if (nstr == NULL)
        return RDB_NO_MEMORY;

    objp->var.bin.datap = nstr;
    strcpy(((char *)objp->var.bin.datap) + objp->var.bin.len - 1, str);
    objp->var.bin.len = len;
    return RDB_OK;
}

static int
append_aggr_op(RDB_object *objp, RDB_aggregate_op op)
{
    switch (op) {
        case RDB_COUNT:
            return append_str(objp, "COUNT");
        case RDB_SUM:
            return append_str(objp, "SUM");
        case RDB_AVG:
            return append_str(objp, "AVG");
        case RDB_MAX:
            return append_str(objp, "MAX");
        case RDB_MIN:
            return append_str(objp, "MIN");
        case RDB_ALL:
            return append_str(objp, "ALL");
        case RDB_ANY:
            return append_str(objp, "ANY");
        case RDB_COUNTD:
            return append_str(objp, "COUNTD");
        case RDB_SUMD:
            return append_str(objp, "SUMD");
        case RDB_AVGD:
            return append_str(objp, "AVGD");
    }
    abort();
}

static int
append_ex(RDB_object *objp, RDB_expression *exp)
{
    RDB_object dst;
    int ret;

    switch (exp->kind) {
        case RDB_EX_OBJ:
             RDB_init_obj(&dst);
             ret = RDB_obj_to_string(&dst, &exp->var.obj);
             if (ret != RDB_OK) {
                 RDB_destroy_obj(&dst);
                 return ret;
             }
             ret = append_str(objp, dst.var.bin.datap);
             RDB_destroy_obj(&dst);
             if (ret != RDB_OK)
                 return ret;
             break;
        case RDB_EX_ATTR:
             ret = append_str(objp, exp->var.attrname);
             if (ret != RDB_OK)
                 return ret;
            break;
        case RDB_EX_EQ:
        case RDB_EX_NEQ:
        case RDB_EX_LT:
        case RDB_EX_GT:
        case RDB_EX_LET:
        case RDB_EX_GET:
        case RDB_EX_ADD:
        case RDB_EX_SUBTRACT:
        case RDB_EX_MULTIPLY:
        case RDB_EX_DIVIDE:
        case RDB_EX_SUBSET:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            switch (objp->kind) {
                case RDB_EX_EQ:
                    ret = append_str(objp, ") = (");
                    break;
                case RDB_EX_NEQ:
                    ret = append_str(objp, ") <> (");
                    break;
                case RDB_EX_LT:
                    ret = append_str(objp, ") < (");
                    break;
                case RDB_EX_GT:
                    ret = append_str(objp, ") > (");
                    break;
                case RDB_EX_LET:
                    ret = append_str(objp, ") <= (");
                    break;
                case RDB_EX_GET:
                    ret = append_str(objp, ") >= (");
                    break;
/* !!
                case RDB_EX_REGMATCH: AND OR NOT
                    ret = append_str(objp, ") MATCHES (");
                    break;
*/
                case RDB_EX_ADD:
                    ret = append_str(objp, ") + (");
                    break;
                case RDB_EX_SUBTRACT:
                    ret = append_str(objp, ") - (");
                    break;
                case RDB_EX_MULTIPLY:
                    ret = append_str(objp, ") * (");
                    break;
                case RDB_EX_DIVIDE:
                    ret = append_str(objp, ") / (");
                    break;
/* !!
                case RDB_EX_CONCAT:
                    ret = append_str(objp, ") || (");
                    break;
*/
                case RDB_EX_SUBSET:
                    ret = append_str(objp, ") SUBSET (");
                    break;
                default: ; /* never reached */
            }
            ret = append_ex(objp, exp->var.op.argv[1]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_CONTAINS:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[1]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") IN (");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_IS_EMPTY:
            ret = append_str(objp, "IS_EMPTY(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
        case RDB_EX_NEGATE:
            ret = append_str(objp, "-(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_TO_INTEGER:
            ret = append_str(objp, "INTEGER(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_TO_RATIONAL:
            ret = append_str(objp, "RATIONAL(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_TO_STRING:
            ret = append_str(objp, "STRING(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_TUPLE_ATTR:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ").");
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, exp->var.op.name);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_GET_COMP:
            ret = append_str(objp, "THE_");
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, exp->var.op.name);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_USER_OP:
            ret = append_str(objp, exp->var.op.name);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_EX_AGGREGATE:
            ret = append_aggr_op(objp, exp->var.op.op);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " (");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, exp->var.op.argv[0]);
            if (ret != RDB_OK)
                return ret;
            if (exp->var.op.op != RDB_COUNT) {
                ret = append_str(objp, " ,");
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, exp->var.op.name);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
    }
    return RDB_OK;
}

static int
append_table(RDB_object *objp, RDB_table *tbp)
{
    int ret;
    int i;

    switch (tbp->kind) {
        case RDB_TB_REAL:
        {
            if (RDB_table_name(tbp) == NULL)
                return RDB_INVALID_ARGUMENT;
            ret = append_str(objp, RDB_table_name(tbp));
            if (ret != RDB_OK)
                return ret;
            break;
        }
        case RDB_TB_SELECT_INDEX:
        case RDB_TB_SELECT:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.select.tbp);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") WHERE (");
            if (ret != RDB_OK)
                return ret;
            ret = append_ex(objp, tbp->var.select.exp);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNION:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var._union.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") UNION (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var._union.tb2p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_MINUS:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.minus.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") MINUS (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.minus.tb2p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_INTERSECT:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.intersect.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") INTERSECT (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.intersect.tb2p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_JOIN:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.join.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") JOIN (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.join.tb2p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_EXTEND:
            ret = append_str(objp, "EXTEND ");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.extend.tbp);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " ADD (");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.extend.attrc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                    return ret;
                }
                ret = append_ex(objp, tbp->var.extend.attrv[i].exp);
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, " AS ");
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, tbp->var.extend.attrv[i].name);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_PROJECT:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.project.tbp);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") {");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->typ->var.basetyp->var.tuple.attrc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                    return ret;
                }
                ret = append_str(objp,
                        tbp->typ->var.basetyp->var.tuple.attrv[i].name);
                if (ret != RDB_OK)
                return ret;
            }
            ret = append_str(objp, "}");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_RENAME:
            ret = append_table(objp, tbp->var.rename.tbp);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " RENAME (");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.rename.renc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                    return ret;
                }
                ret = append_str(objp, tbp->var.rename.renv[i].from);
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, " AS ");
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, tbp->var.rename.renv[i].to);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SUMMARIZE:
            ret = append_str(objp, "SUMMARIZE ");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.summarize.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " PER ");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.summarize.tb2p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " ADD (");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.summarize.addc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                        return ret;
                }
                ret = append_aggr_op(objp, tbp->var.summarize.addv[i].op);
                if (ret != RDB_OK)
                    return ret;
                if (tbp->var.summarize.addv[i].op != RDB_COUNT) {
                    ret = append_str(objp, " (");
                    if (ret != RDB_OK)
                        return ret;
                    ret = append_ex(objp, tbp->var.summarize.addv[i].exp);
                    if (ret != RDB_OK)
                        return ret;
                    ret = append_str(objp, ")");
                    if (ret != RDB_OK)
                        return ret;
                }
                ret = append_str(objp, " AS ");
                if (ret != RDB_OK)
                    return ret;
                ret = append_str(objp, tbp->var.summarize.addv[i].name);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_WRAP:
            ret = append_table(objp, tbp->var.wrap.tbp);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " WRAP (");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.wrap.wrapc; i++) {
                int j;

                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                        return ret;
                }
                ret = append_str(objp, "{");
                if (ret != RDB_OK)
                    return ret;
                for (j = 0; j < tbp->var.wrap.wrapv[i].attrc; j++) {
                    if (j > 0) {
                        ret = append_str(objp, ", ");
                        if (ret != RDB_OK)
                            return ret;
                    }
                    ret = append_str(objp, tbp->var.wrap.wrapv[i].attrv[j]);
                    if (ret != RDB_OK)
                        return ret;
                }
                ret = append_str(objp, "} AS ");
                if (ret != RDB_OK)
                    return ret;                    
                ret = append_str(objp, tbp->var.wrap.wrapv[i].attrname);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNWRAP:
            ret = append_table(objp, tbp->var.unwrap.tbp);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " UNWRAP (");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.unwrap.attrc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                        return ret;
                }
                ret = append_str(objp, tbp->var.unwrap.attrv[i]);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_SDIVIDE:
            ret = append_str(objp, "(");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.sdivide.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") DIVIDEBY (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.sdivide.tb2p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ") PER (");
            if (ret != RDB_OK)
                return ret;
            ret = append_table(objp, tbp->var.sdivide.tb1p);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, ")");
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_GROUP:
            ret = append_table(objp, tbp->var.group.tbp);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " GROUP {");
            if (ret != RDB_OK)
                return ret;
            for (i = 0; i < tbp->var.group.attrc; i++) {
                if (i > 0) {
                    ret = append_str(objp, ", ");
                    if (ret != RDB_OK)
                    return ret;
                }
                ret = append_str(objp, tbp->var.group.attrv[i]);
                if (ret != RDB_OK)
                    return ret;
            }
            ret = append_str(objp, "} AS ");
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, tbp->var.group.gattr);
            if (ret != RDB_OK)
                return ret;
            break;
        case RDB_TB_UNGROUP:
            ret = append_table(objp, tbp->var.ungroup.tbp);
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, " UNGROUP ");
            if (ret != RDB_OK)
                return ret;
            ret = append_str(objp, tbp->var.ungroup.attr);
            if (ret != RDB_OK)
                return ret;
            break;
    }
    return RDB_OK;
}

int
_RDB_table_to_str(RDB_object *objp, RDB_table *tbp)
{
    int ret;

    ret = RDB_string_to_obj(objp, "");
    if (ret != RDB_OK)
        return ret;

    return append_table(objp, tbp);
}
