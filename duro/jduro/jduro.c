/*
 * jduro.c
 *
 * Functions implementing JDuro's native methods.
 *
 *  Created on: 23.02.2014
 *      Author: Rene Hartmann
 */

#include "jduro.h"

RDB_exec_context JDuro_ec;

int
JDuro_throw_exception_from_error(JNIEnv *env, JDuro_session *sessionp, const char *reason,
        RDB_exec_context *ecp)
{
    jobject errobj;
    jstring msgobj;
    jobject exception;
    jmethodID constructorID;

    jclass clazz = (*env)->FindClass(env, "net/sf/duro/DException");
    if (clazz == NULL)
        return -1;

    constructorID = (*env)->GetMethodID(env, clazz, "<init>",
            "(Ljava/lang/String;Ljava/lang/Object;)V");
    if (constructorID == NULL)
        return -1;

    errobj = JDuro_duro_obj_to_jobj(env, RDB_get_err(ecp), sessionp);
    if (errobj == NULL)
        return -1;

    msgobj = (*env)->NewStringUTF(env, reason);
    if (msgobj == NULL)
        return -1;

    exception = (*env)->NewObject(env, clazz, constructorID, msgobj, errobj);
    if (exception == NULL)
        return -1;

    return (*env)->Throw(env, exception);
}

static void
del_session(JNIEnv *env, JDuro_session *sessionp)
{
    if (sessionp->booleanClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->booleanClass);
    if (sessionp->tupleClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->tupleClass);
    if (sessionp->integerClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->integerClass);
    if (sessionp->stringClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->stringClass);
    if (sessionp->doubleClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->doubleClass);
    if (sessionp->byteArrayClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->byteArrayClass);
    if (sessionp->hashSetClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->hashSetClass);
    if (sessionp->sessionObj != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->sessionObj);
    Duro_destroy_interp(&sessionp->interp);
    free(sessionp);
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DuroDSession_initInterp(JNIEnv *env, jobject obj)
{
    jclass clazz;
    jfieldID interpFieldID;
    JDuro_session *sessionp = malloc(sizeof (JDuro_session));
    if (sessionp == NULL) {
        jclass clazz = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
        (*env)->ThrowNew(env, clazz, "");
        return;
    }

    sessionp->sessionObj = (*env)->NewGlobalRef(env, obj);
    if (sessionp->sessionObj == NULL)
        return;

    RDB_init_exec_context(&JDuro_ec);

    if (RDB_init_builtin(&JDuro_ec) != RDB_OK) {
        /*
         * Don't use JDuro_throw_exception_from_error() because that requires
         * an interpreter
         */
        clazz = (*env)->FindClass(env, "net/sf/duro/DException");
        (*env)->ThrowNew(env, clazz,
                RDB_type_name(RDB_obj_type(RDB_get_err(&JDuro_ec))));
        free(sessionp);
        return;
    }
    if (Duro_init_interp(&sessionp->interp, &JDuro_ec, NULL, "") != RDB_OK) {
        free(sessionp);
        return;
    }

    sessionp->booleanClass = NULL;
    sessionp->integerClass = NULL;
    sessionp->stringClass = NULL;
    sessionp->doubleClass = NULL;
    sessionp->tupleClass = NULL;
    sessionp->byteArrayClass = NULL;
    sessionp->hashSetClass = NULL;

    clazz = (*env)->FindClass(env, "java/lang/Boolean");
    if (clazz == NULL) {
        del_session(env, sessionp);
        return;
    }
    sessionp->booleanClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->booleanClass == NULL) {
        del_session(env, sessionp);
        return;
    }

    clazz = (*env)->FindClass(env, "java/lang/Integer");
    if (clazz == NULL) {
        del_session(env, sessionp);
        return;
    }
    sessionp->integerClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->integerClass == NULL) {
        del_session(env, sessionp);
        return;
    }

    clazz = (*env)->FindClass(env, "java/lang/String");
    if (clazz == NULL) {
        del_session(env, sessionp);
        return;
    }
    sessionp->stringClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->stringClass == NULL) {
        del_session(env, sessionp);
        return;
    }

    clazz = (*env)->FindClass(env, "java/lang/Double");
    if (clazz == NULL) {
        del_session(env, sessionp);
        return;
    }
    sessionp->doubleClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->doubleClass == NULL) {
        del_session(env, sessionp);
        return;
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/Tuple");
    if (clazz == NULL) {
        del_session(env, sessionp);
        return;
    }
    sessionp->tupleClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->tupleClass == NULL) {
        del_session(env, sessionp);
        return;
    }

    clazz = (*env)->FindClass(env, "[B");
    if (clazz == NULL) {
        del_session(env, sessionp);
        return;
    }
    sessionp->byteArrayClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->byteArrayClass == NULL) {
        del_session(env, sessionp);
        return;
    }

    clazz = (*env)->FindClass(env, "java/util/HashSet");
    if (clazz == NULL) {
        del_session(env, sessionp);
        return;
    }
    sessionp->hashSetClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->hashSetClass == NULL) {
        del_session(env, sessionp);
        return;
    }

    clazz = (*env)->GetObjectClass(env, obj);
    interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    (*env)->SetLongField(env, obj, interpFieldID, (intptr_t) sessionp);
}

JDuro_session *
JDuro_jobj_session(JNIEnv *env, jobject obj)
{
    jlong interpField;
    jclass clazz = (*env)->GetObjectClass(env, obj);
    jfieldID interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    if (interpFieldID == NULL)
        return NULL;
    interpField = (*env)->GetLongField(env, obj, interpFieldID);
    if (interpField == (jlong) 0) {
        return NULL;
    }
    return (JDuro_session *) (intptr_t) interpField;
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DuroDSession_destroyInterp(JNIEnv *env, jobject obj)
{
    JDuro_session *sessionp;
    jclass clazz = (*env)->GetObjectClass(env, obj);
    jfieldID interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    jlong interpField = (*env)->GetLongField(env, obj, interpFieldID);

    if (interpField == (jlong) 0)
        return;

    sessionp = (JDuro_session *) (intptr_t) interpField;

    (*env)->SetLongField(env, obj, interpFieldID, (jlong) 0);
    del_session(env, sessionp);

    RDB_destroy_exec_context(&JDuro_ec);
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DuroDSession_executeI(JNIEnv *env, jobject jobj,
        jstring statements)
{
    int ret;
    jclass clazz = (*env)->GetObjectClass(env, jobj);
    jfieldID interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    JDuro_session *sessionp = (JDuro_session *) (intptr_t)
            (*env)->GetLongField(env, jobj, interpFieldID);
    const char *str = (*env)->GetStringUTFChars(env, statements, 0);
    if (str == NULL)
        return;

    ret = Duro_dt_execute_str(str, &sessionp->interp, &JDuro_ec);
    (*env)->ReleaseStringUTFChars(env, statements, str);
    if (ret != RDB_OK) {
        JDuro_throw_exception_from_error(env, sessionp, "execution of statements failed",
                &JDuro_ec);
    }
}

static jobject
tuple_to_jobj(JNIEnv *env, const RDB_object *tup, JDuro_session *sessionp)
{
    int i;
    jmethodID setAttributeID;
    jobject jobj;
    int n;
    char **namev;
    jmethodID constructorID = (*env)->GetMethodID(env, sessionp->tupleClass,
            "<init>", "()V");
    if (constructorID == NULL)
        return NULL;
    setAttributeID = (*env)->GetMethodID(env, sessionp->tupleClass,
            "setAttribute", "(Ljava/lang/String;Ljava/lang/Object;)V");
    if (setAttributeID == NULL)
        return NULL;
    jobj = (*env)->NewObject(env, sessionp->tupleClass, constructorID);
    n = RDB_tuple_size(tup);
    namev = malloc(sizeof(char *) * n);
    if (namev == NULL)
        return NULL;
    RDB_tuple_attr_names(tup, namev);

    for (i = 0; i < n; i++) {
        RDB_object *attrp = RDB_tuple_get(tup, namev[i]);
        jstring jstr = (*env)->NewStringUTF(env, namev[i]);
        (*env)->CallVoidMethod(env, jobj, setAttributeID,
                jstr, JDuro_duro_obj_to_jobj(env, attrp, sessionp));
    }
    free(namev);
    return jobj;
}

static jobject
table_to_jobj(JNIEnv *env, const RDB_object *tbp, JDuro_session *sessionp)
{
    RDB_object tpl;
    RDB_qresult *qrp;
    jobject elem;
    jmethodID constructorId;
    jmethodID addID;
    jobject jtable;
    constructorId = (*env)->GetMethodID(env, sessionp->hashSetClass,
            "<init>", "()V");
    if (constructorId == NULL)
        return NULL;
    addID = (*env)->GetMethodID( env, sessionp->hashSetClass, "add",
            "(Ljava/lang/Object;)Z");
    if (addID == NULL)
        return NULL;
    jtable = (*env)->NewObject(env, sessionp->hashSetClass, constructorId);
    if (jtable == NULL)
        return NULL;

    qrp = RDB_table_iterator((RDB_object *) tbp, 0, NULL, &JDuro_ec, NULL);
    if (qrp == NULL) {
        JDuro_throw_exception_from_error(env, sessionp,
                "cannot create table iterator", &JDuro_ec);
        return NULL;
    }
    RDB_init_obj(&tpl);
    while (RDB_next_tuple(qrp, &tpl, &JDuro_ec, NULL) == RDB_OK) {
        elem = JDuro_duro_obj_to_jobj(env, &tpl, sessionp);
        if (elem == NULL)
            goto error;
        (*env)->CallObjectMethod(env, jtable, addID, elem);
    }

    RDB_del_table_iterator(qrp, &JDuro_ec, NULL);
    RDB_destroy_obj(&tpl, &JDuro_ec);
    return jtable;

error:
    RDB_del_table_iterator(qrp, &JDuro_ec, NULL);
    RDB_destroy_obj(&tpl, &JDuro_ec);
    return NULL;
}

static jobject
array_to_jobj(JNIEnv *env, const RDB_object *arrp, JDuro_session *sessionp)
{
    int i;
    jobject objval;
    jobjectArray jobjarr;
    jclass clazz;
    RDB_int size = RDB_array_length(arrp, &JDuro_ec);

    RDB_type *elemtyp = RDB_base_type(RDB_obj_type(arrp));
    if (elemtyp == &RDB_INTEGER) {
        clazz = sessionp->integerClass;
    } else if (elemtyp == &RDB_FLOAT) {
        clazz = sessionp->doubleClass;
    } else if (elemtyp == &RDB_BOOLEAN) {
        clazz = sessionp->booleanClass;
    } else if (elemtyp == &RDB_STRING) {
        clazz = sessionp->stringClass;
    } else if (RDB_type_is_tuple(elemtyp)) {
        clazz = sessionp->tupleClass;
    } else if (RDB_type_is_relation(elemtyp)) {
        clazz = sessionp->hashSetClass;
    } else if (elemtyp == &RDB_BINARY) {
        clazz = sessionp->byteArrayClass;
    } else {
        clazz = (*env)->FindClass(env, "net/sf/duro/DuroPossrepObject");
    }

    jobjarr = (*env)->NewObjectArray(env, (jsize) size, clazz, NULL);
    if (jobjarr == NULL)
        return NULL;
    for (i = 0; i < size; i++) {
         objval = JDuro_duro_obj_to_jobj(env, RDB_array_get(arrp,
                 (RDB_int) i, &JDuro_ec), sessionp);
         if (objval == NULL)
             return NULL;
         (*env)->SetObjectArrayElement(env, jobjarr, (jsize) i, objval);
    }
    return jobjarr;
}

/*
 * Convert *objp to a Java object.
 */
jobject
JDuro_duro_obj_to_jobj(JNIEnv *env, const RDB_object *objp, JDuro_session *sessionp)
{
    jmethodID constructorID;
    jclass clazz;
    RDB_object *cobjp;
    jobject jobj = NULL;
    RDB_type *typ = RDB_obj_type(objp);

    if (RDB_is_tuple(objp))
        return tuple_to_jobj(env, objp, sessionp);
    if (typ == NULL) {
        (*env)->ThrowNew(env,
                         (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                         "missing type");
        return NULL;
    }
    if (typ == &RDB_STRING) {
        return (jobject) (*env)->NewStringUTF(env, RDB_obj_string(objp));
    }
    if (typ == &RDB_INTEGER) {
        constructorID = (*env)->GetMethodID(env, sessionp->integerClass,
                "<init>", "(I)V");
        return (*env)->NewObject(env, sessionp->integerClass, constructorID,
                (jint) RDB_obj_int(objp));
    }
    if (typ == &RDB_FLOAT) {
        constructorID = (*env)->GetMethodID(env, sessionp->doubleClass,
                "<init>", "(D)V");
        return (*env)->NewObject(env, sessionp->doubleClass, constructorID,
                (jdouble) RDB_obj_float(objp));
    }
    if (typ == &RDB_BOOLEAN) {
        constructorID = (*env)->GetMethodID(env, sessionp->booleanClass,
                "<init>", "(Z)V");
        return (*env)->NewObject(env, sessionp->booleanClass,
                constructorID, (jboolean) RDB_obj_bool(objp));
    }
    if (typ == &RDB_BINARY) {
        void *bp;
        jsize len = (jsize) RDB_binary_length(objp);
        jobj = (*env)->NewByteArray(env, len);
        if (jobj == NULL)
            return NULL;
        if (RDB_binary_get(objp, 0, (size_t) len, &JDuro_ec, &bp, NULL) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp,
                    "getting byte array failed", &JDuro_ec);
            return NULL;
        }

        (*env)->SetByteArrayRegion(env, jobj, 0, len, bp);
        return jobj;
    }
    if (RDB_type_is_array(typ)) {
        return array_to_jobj(env, objp, sessionp);
    }
    if (RDB_type_is_relation(typ)) {
        return table_to_jobj(env, objp, sessionp);
    }

    /*
     * Create a copy of *objp which is managed by the Java object
     */
    cobjp = RDB_new_obj(&JDuro_ec);
    if (cobjp == NULL) {
        JDuro_throw_exception_from_error(env, sessionp,
                "creating RDB_object failed", &JDuro_ec);
        goto error;
    }

    if (RDB_copy_obj(cobjp, objp, &JDuro_ec) == RDB_ERROR) {
        JDuro_throw_exception_from_error(env, sessionp,
                "copying RDB_object failed", &JDuro_ec);
        goto error;
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/DuroPossrepObject");
    if (clazz == NULL)
        goto error;
    constructorID = (*env)->GetMethodID(env, clazz, "<init>",
            "(JLnet/sf/duro/DuroDSession;)V");
    if (constructorID == NULL) {
        goto error;
    }

    /* objp will be managed by the Java object */

    jobj = (*env)->NewObject(env, clazz, constructorID,
            (jlong) (intptr_t) cobjp, sessionp->sessionObj);
    if (jobj == NULL)
        goto error;
    return jobj;

error:
    if (cobjp != NULL)
        RDB_free_obj(cobjp, &JDuro_ec);
    return NULL;
}

JNIEXPORT jobject
JNICALL Java_net_sf_duro_DuroDSession_evaluateI(JNIEnv *env, jobject obj,
        jstring expression)
{
    RDB_object result;
    jobject jresult;
    RDB_expression *exp;
    const char *str;
    JDuro_session *sessionp = JDuro_jobj_session(env, obj);
    if (sessionp == NULL)
        return NULL;
    str = (*env)->GetStringUTFChars(env, expression, 0);

    exp = Duro_dt_parse_expr_str(str, &sessionp->interp, &JDuro_ec);
    (*env)->ReleaseStringUTFChars(env, expression, str);
    if (exp == NULL) {
        JDuro_throw_exception_from_error(env, sessionp,
                "evaluating expression failed", &JDuro_ec);
        return NULL;
    }

    RDB_init_obj(&result);
    if (Duro_evaluate_retry(exp, &sessionp->interp, &JDuro_ec,
            &result) != RDB_OK) {
        JDuro_throw_exception_from_error(env, sessionp,
                "expression evaluation failed", &JDuro_ec);
        goto error;
    }

    /* If the expression type is missing, try to get it */
    if (RDB_obj_type(&result) == NULL
            && !RDB_is_tuple(&result)) {
        RDB_type *typ = Duro_expr_type_retry(exp, &sessionp->interp, &JDuro_ec);
        if (typ == NULL) {
            JDuro_throw_exception_from_error(env, sessionp,
                    "getting expression type failed", &JDuro_ec);
            goto error;
        }
        RDB_obj_set_typeinfo(&result, typ);
    }
    jresult = JDuro_duro_obj_to_jobj(env, &result, sessionp);
    RDB_destroy_obj(&result, &JDuro_ec);
    RDB_del_expr(exp, &JDuro_ec);
    return jresult;

error:
    RDB_destroy_obj(&result, &JDuro_ec);
    RDB_del_expr(exp, &JDuro_ec);
    return NULL;
}

static int
jtuple_to_obj(JNIEnv *env, RDB_object *dstp, jobject obj, JDuro_session *sessionp,
        RDB_exec_context *ecp)
{
    jstring attrnameobj;
    jobject attrvalobj;
    const char *attrname;
    RDB_object attrval;
    jmethodID getAttributeMethodID;
    jobject keyset;
    RDB_type *dsttyp = RDB_obj_type(dstp);
    jclass tupleclazz = (*env)->GetObjectClass(env, obj);
    jmethodID methodID = (*env)->GetMethodID(env, tupleclazz, "attributeNames",
            "()Ljava/util/Set;");
    if (methodID == NULL)
        return -1;
    keyset = (*env)->CallObjectMethod(env, obj, methodID);

    getAttributeMethodID = (*env)->GetMethodID(env, tupleclazz, "getAttribute",
            "(Ljava/lang/String;)Ljava/lang/Object;");
    if (getAttributeMethodID == NULL)
        return -1;

    if (dsttyp != NULL) {
        int i;
        RDB_attr *attrv;
        int attrc;
        RDB_object *attrvalp;
        jmethodID sizeMethodID = (*env)->GetMethodID(env, tupleclazz, "attributeNames",
                "()Ljava/util/Set;");

        if (!RDB_type_is_tuple(dsttyp)) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be tuple");
            return -1;
        }

        /*
         * Copy all attributes
         */

        /* Get attributes from type */
        attrv = RDB_type_attrs(dsttyp, &attrc);

        /* Compare size */
        sizeMethodID = (*env)->GetMethodID(env, tupleclazz, "size", "()I");
        if (sizeMethodID == NULL)
            return -1;
        if (attrc != (int) (*env)->CallIntMethod(env, obj, sizeMethodID)) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "tuple size differs from destination");
            return -1;
        }

        for (i = 0; i < attrc; i++) {
            /* Get Duro tuple attribute */
            attrvalp = RDB_tuple_get(dstp, attrv[i].name);
            if (attrvalp == NULL) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "attribute not present in destination");
                return -1;
            }

            /* Get attribute from Java tuple */
            attrnameobj = (*env)->NewStringUTF(env, attrv[i].name);
            if (attrnameobj == NULL) {
                return -1;
            }
            attrvalobj = (*env)->CallObjectMethod(env, obj, getAttributeMethodID,
                    attrnameobj);
            if (attrvalobj == NULL) {
                if ((*env)->ExceptionOccurred(env) == NULL) {
                    (*env)->ThrowNew(env,
                            (*env)->FindClass(env,
                                    "java/lang/IllegalArgumentException"),
                                    "missing attribute");
                }
                return -1;
            }

            /* Convert attribute value */
            RDB_init_obj(&attrval);
            if (JDuro_jobj_to_duro_obj(env, attrvalobj, &attrval, sessionp, ecp) != 0) {
                RDB_destroy_obj(&attrval, ecp);
                return -1;
            }

            /* Copy attribute to tuple */
            if (RDB_copy_obj(attrvalp, &attrval, &JDuro_ec) != RDB_OK) {
                JDuro_throw_exception_from_error(env, sessionp, "setting tuple attribute failed", ecp);
                RDB_destroy_obj(&attrval, ecp);
                return -1;
            }
            RDB_destroy_obj(&attrval, ecp);
        }
    } else {
        jobject iter;
        jclass iterclazz;
        jclass setclazz;
        jmethodID hasNextMethodID;
        jmethodID nextMethodID;
        setclazz = (*env)->GetObjectClass(env, keyset);
        methodID = (*env)->GetMethodID(env, setclazz, "iterator",
                "()Ljava/util/Iterator;");
        iter = (*env)->CallObjectMethod(env, keyset, methodID);
        if (iter == NULL)
            return -1;
        iterclazz = (*env)->GetObjectClass(env, iter);
        hasNextMethodID = (*env)->GetMethodID(env, iterclazz, "hasNext",
                "()Z");
        nextMethodID = (*env)->GetMethodID(env, iterclazz, "next",
                "()Ljava/lang/Object;");

        for(;;) {
            /* Invoke hashNext() to check if there are any attributes left to read */
            if (!(*env)->CallBooleanMethod(env, iter, hasNextMethodID))
                break;

            /* Get attribute name and value */
            attrnameobj = (*env)->CallObjectMethod(env, iter, nextMethodID);
            attrvalobj = (*env)->CallObjectMethod(env, obj, getAttributeMethodID,
                    attrnameobj);
            attrname = (*env)->GetStringUTFChars(env, attrnameobj, 0);

            RDB_init_obj(&attrval);
            if (JDuro_jobj_to_duro_obj(env, attrvalobj, &attrval, sessionp, ecp) != 0) {
                RDB_destroy_obj(&attrval, ecp);
                return -1;
            }
            if (RDB_tuple_set(dstp, attrname, &attrval, ecp) != RDB_OK) {
                JDuro_throw_exception_from_error(env, sessionp,
                        "setting tuple attribute failed", ecp);
                RDB_destroy_obj(&attrval, ecp);
                return -1;
            }
            RDB_destroy_obj(&attrval, ecp);

            (*env)->ReleaseStringUTFChars(env, attrnameobj, attrname);
        }
    }
    return 0;
}

static int
jobj_to_table(JNIEnv *env, jobject obj, RDB_object *dstp,
        JDuro_session *sessionp, RDB_exec_context *ecp)
{
    RDB_object tpl;
    jobject iter;
    jobject elem;
    jclass iterclazz, setclazz;
    jmethodID methodID, hasNextMethodID, nextMethodID;

    if (RDB_obj_type(dstp) == NULL
            || !RDB_type_is_relation(RDB_obj_type(dstp))) {
        RDB_raise_type_mismatch("not a table", &JDuro_ec);
        JDuro_throw_exception_from_error(env, sessionp,
                "destination is not a table", &JDuro_ec);
        return -1;
    }

    setclazz = (*env)->GetObjectClass(env, obj);
    methodID = (*env)->GetMethodID(env, setclazz, "iterator",
            "()Ljava/util/Iterator;");
    iter = (*env)->CallObjectMethod(env, obj, methodID);
    if (iter == NULL)
        return -1;
    iterclazz = (*env)->GetObjectClass(env, iter);
    hasNextMethodID = (*env)->GetMethodID(env, iterclazz, "hasNext",
            "()Z");
    nextMethodID = (*env)->GetMethodID(env, iterclazz, "next",
            "()Ljava/lang/Object;");

    /*
     * Clear table
     */
    if (RDB_delete(dstp, NULL, &JDuro_ec, Duro_dt_tx(&sessionp->interp)) != RDB_OK) {
        JDuro_throw_exception_from_error(env, sessionp, "delete failed", &JDuro_ec);
        return -1;
    }

    RDB_init_obj(&tpl);
    for(;;) {
        /* Invoke hashNext() to check if there are any attributes left to read */
        if (!(*env)->CallBooleanMethod(env, iter, hasNextMethodID))
            break;

        /* Get next set element */
        elem = (*env)->CallObjectMethod(env, iter, nextMethodID);
        if (elem == NULL)
            goto error;

        /* Convert it to a Duro tuple */
        if (JDuro_jobj_to_duro_obj(env, elem, &tpl, sessionp, &JDuro_ec) == -1)
            goto error;

        /* Insert tuple into table */
        if (RDB_insert(dstp, &tpl, &JDuro_ec,
                Duro_dt_tx(&sessionp->interp)) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp, "insert failed", &JDuro_ec);
            goto error;
        }
    }
    RDB_destroy_obj(&tpl, &JDuro_ec);
    return 0;

error:
    RDB_destroy_obj(&tpl, &JDuro_ec);
    return -1;
}

/*
 * Convert a Java object to a RDB_object.
 * *dstp must be either newly initialized or carry type information.
 * If the Java object is a java.util.Set, *dstp must be an empty table.
 */
int
JDuro_jobj_to_duro_obj(JNIEnv *env, jobject obj, RDB_object *dstp,
        JDuro_session *sessionp, RDB_exec_context *ecp)
{
    jsize len;
    jmethodID methodID;
    jclass clazz;
    RDB_type *typ = RDB_obj_type(dstp);
    if ((*env)->IsInstanceOf(env, obj, sessionp->stringClass)) {
        const char *strval = (*env)->GetStringUTFChars(env, obj, 0);

        if (typ != NULL && typ != &RDB_STRING) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be string");
            return -1;
        }

        if (RDB_string_to_obj(dstp, strval, ecp) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp,
                    "getting string data failed", ecp);
            return -1;
        }
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->integerClass)) {
        if (typ != NULL && typ != &RDB_INTEGER) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be integer");
            return -1;
        }
        methodID = (*env)->GetMethodID(env, sessionp->integerClass,
                "intValue", "()I");
        RDB_int_to_obj(dstp, (RDB_int) (*env)->CallIntMethod(env, obj, methodID));
        return 0;
    }

    clazz = (*env)->FindClass(env, "java/lang/Double");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        if (typ != NULL && typ != &RDB_FLOAT) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be float");
            return -1;
        }

        methodID = (*env)->GetMethodID(env, clazz, "doubleValue", "()D");
        RDB_float_to_obj(dstp, (RDB_float) (*env)->CallDoubleMethod(env, obj, methodID));
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->booleanClass)) {
        if (typ != NULL && typ != &RDB_BOOLEAN) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be boolean");
            return -1;
        }

        methodID = (*env)->GetMethodID(env, sessionp->booleanClass,
                "booleanValue", "()Z");
        RDB_bool_to_obj(dstp, (RDB_float) (*env)->CallBooleanMethod(env, obj, methodID));
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->byteArrayClass)) {
        jbyte *bp;

        if (typ != NULL && typ != &RDB_BINARY) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be binary");
            return -1;
        }

        len = (*env)->GetArrayLength(env, obj);
        bp = (*env)->GetByteArrayElements(env, obj, NULL);
        if (bp == NULL)
            return -1;
        if (RDB_binary_set (dstp, 0, bp, (size_t) len, &JDuro_ec) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp, "setting binary data failed",
                    &JDuro_ec);
            return -1;
        }
        (*env)->ReleaseByteArrayElements(env, obj, bp, JNI_ABORT);
        return 0;
    }

    /* Other array types */
    clazz = (*env)->FindClass(env, "[Ljava/lang/Object;");
    if (clazz == NULL)
        return -1;
    if ((*env)->IsAssignableFrom(env, (*env)->GetObjectClass(env, obj), clazz)) {
        /* Convert array of objects */
        int i;
        RDB_object *dstelemp;
        RDB_type *basetyp = NULL;
        len = (*env)->GetArrayLength(env, obj);

        if (typ != NULL && !RDB_type_is_array(typ)) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be array");
            return -1;
        }

        if (typ != NULL)
            basetyp = RDB_base_type(typ);

        if (basetyp != NULL) {
            /* Check base type against Java array type */
            clazz = (*env)->FindClass(env, "[Ljava/lang/String;");
            if (clazz == NULL)
                return -1;
            if ((*env)->IsInstanceOf(env, obj, clazz) && basetyp != &RDB_STRING) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "destination must be string array");
                return -1;
            }

            clazz = (*env)->FindClass(env, "[Ljava/lang/Integer;");
            if (clazz == NULL)
                return -1;
            if ((*env)->IsInstanceOf(env, obj, clazz) && basetyp != &RDB_INTEGER) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "destination must be integer array");
                return -1;
            }

            clazz = (*env)->FindClass(env, "[Ljava/lang/Double;");
            if (clazz == NULL)
                return -1;
            if ((*env)->IsInstanceOf(env, obj, clazz) && basetyp != &RDB_FLOAT) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "destination must be float array");
                return -1;
            }

            clazz = (*env)->FindClass(env, "[Ljava/lang/Boolean;");
            if (clazz == NULL)
                return -1;
            if ((*env)->IsInstanceOf(env, obj, clazz) && basetyp != &RDB_BOOLEAN) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "destination must be boolean array");
                return -1;
            }

            clazz = (*env)->FindClass(env, "[[B");
            if (clazz == NULL)
                return -1;
            if ((*env)->IsInstanceOf(env, obj, clazz) && basetyp != &RDB_BINARY) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "destination must be array of binary");
                return -1;
            }

        }

        if (RDB_set_array_length(dstp, (RDB_int) len, &JDuro_ec) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp, "setting binary data failed",
                    &JDuro_ec);
            return -1;
        }
        for (i = 0; i < len; i++) {
            dstelemp = RDB_array_get(dstp, (RDB_int) i, &JDuro_ec);

            if (JDuro_jobj_to_duro_obj(env, (*env)->GetObjectArrayElement(env,
                                       obj, (jsize) i),
                                 dstelemp, sessionp, &JDuro_ec) != 0) {
                return -1;
            }
        }
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->tupleClass)) {
        return jtuple_to_obj(env, dstp, obj, sessionp, ecp);
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/PossrepObject");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        jfieldID refFieldID = (*env)->GetFieldID(env, clazz, "ref", "J");
        RDB_object *objp = (RDB_object *) (intptr_t)
                (*env)->GetLongField(env, obj, refFieldID);
        if (objp == NULL)
            return -1;

        if (RDB_copy_obj(dstp, objp, ecp) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp, "copy failed", ecp);
            return -1;
        }
        return 0;
    }

    clazz = (*env)->FindClass(env, "java/util/Set");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        return jobj_to_table(env, obj, dstp, sessionp, &JDuro_ec);
    }

    clazz = (*env)->FindClass(env, "java/lang/IllegalArgumentException");
    (*env)->ThrowNew(env, clazz, "unsupported class");
    return -1;
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DuroDSession_setVarI(JNIEnv *env, jobject obj,
        jstring name, jobject value)
{
    const char *namestr;
    RDB_object *dstp;
    JDuro_session *sessionp = JDuro_jobj_session(env, obj);
    if (sessionp == NULL)
        return;
    namestr = (*env)->GetStringUTFChars(env, name, 0);
    if (namestr == NULL)
        return;

    dstp = Duro_lookup_var(namestr, &sessionp->interp, &JDuro_ec);
    if (dstp == NULL) {
        JDuro_throw_exception_from_error(env, sessionp, "variable lookup failed", &JDuro_ec);
        goto cleanup;
    }

    if (JDuro_jobj_to_duro_obj(env, value, dstp, sessionp, &JDuro_ec) != 0) {
        goto cleanup;
    }

cleanup:
    (*env)->ReleaseStringUTFChars(env, name, namestr);
}
