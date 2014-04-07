/*
 * jduro.c
 *
 *  Created on: 23.02.2014
 *      Author: rene
 */

#include <jni.h>
#include <dli/iinterp.h>

static RDB_exec_context JDuro_ec;

static jobject
duro_obj_to_jobj(JNIEnv *, const RDB_object *, jobject);

static int
throw_exception_from_error(JNIEnv *env, jobject dInstance, const char *reason,
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

    errobj = duro_obj_to_jobj(env, RDB_get_err(ecp), dInstance);
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

JNIEXPORT void
JNICALL Java_net_sf_duro_DInstance_initInterp(JNIEnv *env, jobject obj)
{
    jclass clazz;
    jfieldID interpFieldID;
    Duro_interp *interp = malloc(sizeof (Duro_interp));
    if (interp == NULL) {
        jclass clazz = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
        (*env)->ThrowNew(env, clazz, "");
        return;
    }

    RDB_init_exec_context(&JDuro_ec);

    if (RDB_init_builtin(&JDuro_ec) != RDB_OK) {
        /*
         * Don't use throw_exception_from_error() because that requires
         * an interpreter;
         */
        clazz = (*env)->FindClass(env, "net/sf/duro/DException");
        (*env)->ThrowNew(env, clazz,
                RDB_type_name(RDB_obj_type(RDB_get_err(&JDuro_ec))));
        return;
    }
    if (Duro_init_interp(interp, &JDuro_ec, "") != RDB_OK) {
        return;
    }

    clazz = (*env)->GetObjectClass(env, obj);
    interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    (*env)->SetLongField(env, obj, interpFieldID, (intptr_t) interp);
}

static Duro_interp *
jobj_interp(JNIEnv *env, jobject obj)
{
    jclass clazz = (*env)->GetObjectClass(env, obj);
    jfieldID interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    jlong interpField = (*env)->GetLongField(env, obj, interpFieldID);
    if (interpField == (jlong) 0)
        return NULL;
    return (Duro_interp *) (intptr_t) interpField;
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DInstance_destroyInterp(JNIEnv *env, jobject obj)
{
    Duro_interp *interp;
    jclass clazz = (*env)->GetObjectClass(env, obj);
    jfieldID interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    jlong interpField = (*env)->GetLongField(env, obj, interpFieldID);

    if (interpField == (jlong) 0)
        return;

    interp = (Duro_interp *) (intptr_t) interpField;

    (*env)->SetLongField(env, obj, interpFieldID, (jlong) 0);
    Duro_destroy_interp(interp);
    free(interp);

    RDB_destroy_exec_context(&JDuro_ec);
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DInstance_executeI(JNIEnv *env, jobject jobj,
        jstring statements)
{
    int ret;
    jclass clazz = (*env)->GetObjectClass(env, jobj);
    jfieldID interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    Duro_interp *interp = (Duro_interp *) (intptr_t)
            (*env)->GetLongField(env, jobj, interpFieldID);
    const char *str = (*env)->GetStringUTFChars(env, statements, 0);
    if (str == NULL)
        return;

    ret = Duro_dt_execute_str(NULL, str, interp, &JDuro_ec);
    (*env)->ReleaseStringUTFChars(env, statements, str);
    if (ret != RDB_OK) {
        throw_exception_from_error(env, jobj, "execution of statements failed",
                &JDuro_ec);
    }
}

static jobject
tuple_to_jobj(JNIEnv *env, const RDB_object *tup, jobject dinstance)
{
    int i;
    jclass clazz = (*env)->FindClass(env, "net/sf/duro/Tuple");
    jmethodID constructorID = (*env)->GetMethodID(env, clazz, "<init>", "()V");
    jmethodID setAttributeID = (*env)->GetMethodID(env, clazz, "setAttribute",
            "(Ljava/lang/String;Ljava/lang/Object;)V");
    jobject jobj = (*env)->NewObject(env, clazz, constructorID);
    int n = RDB_tuple_size(tup);
    char **namev = malloc(sizeof(char *) * n);
    if (namev == NULL)
        return NULL;
    RDB_tuple_attr_names(tup, namev);

    for (i = 0; i < n; i++) {
        RDB_object *attrp = RDB_tuple_get(tup, namev[i]);
        jstring jstr = (*env)->NewStringUTF(env, namev[i]);
        (*env)->CallVoidMethod(env, jobj, setAttributeID,
                jstr, duro_obj_to_jobj(env, attrp, dinstance));
    }
    free(namev);
    return jobj;
}

static jobject
table_to_jobj(JNIEnv *env, const RDB_object *tbp, jobject dInstance)
{
    RDB_object tpl;
    RDB_qresult *qrp;
    jobject elem;
    jmethodID constructorId;
    jmethodID addID;
    jobject jtable;
    jclass clazz = (*env)->FindClass(env, "java/util/HashSet");
    if (clazz == NULL)
        return NULL;
    constructorId = (*env)->GetMethodID(env, clazz, "<init>", "()V");
    if (constructorId == NULL)
        return NULL;
    addID = (*env)->GetMethodID( env, clazz, "add", "(Ljava/lang/Object;)Z");
    if (addID == NULL)
        return NULL;
    jtable = (*env)->NewObject(env, clazz, constructorId);
    if (jtable == NULL)
        return NULL;

    qrp = RDB_table_iterator((RDB_object *) tbp, 0, NULL, &JDuro_ec, NULL);
    if (qrp == NULL) {
        throw_exception_from_error(env, dInstance,
                "cannot create table iterator", &JDuro_ec);
        return NULL;
    }
    RDB_init_obj(&tpl);
    while (RDB_next_tuple(qrp, &tpl, &JDuro_ec, NULL) == RDB_OK) {
        elem = duro_obj_to_jobj(env, &tpl, dInstance);
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
array_to_jobj(JNIEnv *env, const RDB_object *arrp, jobject dInstance)
{
    int i;
    jobject objval;
    jobjectArray jobjarr;
    jclass clazz;
    RDB_int size = RDB_array_length(arrp, &JDuro_ec);

    RDB_type *elemtyp = RDB_base_type(RDB_obj_type(arrp));
    if (elemtyp == &RDB_INTEGER) {
        jint val;
        jintArray jarr = (*env)->NewIntArray(env, (jsize) size);
        if (jarr == NULL)
            return NULL;
        for (i = 0; i < size; i++) {
            val = (jint) RDB_obj_int(RDB_array_get(arrp, (RDB_int) i, &JDuro_ec));
            (*env)->SetIntArrayRegion(env, jarr, (jsize) i, (jsize) 1, &val);
        }
        return jarr;
    }

    if (elemtyp == &RDB_FLOAT) {
        jdoubleArray jarr = (*env)->NewDoubleArray(env, (jsize) size);
        jdouble val;
        if (jarr == NULL)
            return NULL;
        for (i = 0; i < size; i++) {
            val = (jdouble) RDB_obj_float(RDB_array_get(arrp, (RDB_int) i, &JDuro_ec));
            (*env)->SetDoubleArrayRegion(env, jarr, (jsize) i, (jsize) 1, &val);
        }
        return jarr;
    }

    if (elemtyp == &RDB_BOOLEAN) {
        jboolean val;
        jbooleanArray jarr = (*env)->NewBooleanArray(env, (jsize) size);
        if (jarr == NULL)
            return NULL;
        for (i = 0; i < size; i++) {
            val = (jint) RDB_obj_bool(RDB_array_get(arrp, (RDB_int) i, &JDuro_ec));
            (*env)->SetBooleanArrayRegion(env, jarr, (jsize) i, (jsize) 1, &val);
        }
        return jarr;
    }

    if (elemtyp == &RDB_STRING) {
        clazz = (*env)->FindClass(env, "java/lang/String");
    } else if (RDB_type_is_tuple(elemtyp)) {
        clazz = (*env)->FindClass(env, "net/sf/duro/Tuple");
    } else if (RDB_type_is_relation(elemtyp)) {
        clazz = (*env)->FindClass(env, "java/util/HashSet");
    } else {
        clazz = (*env)->FindClass(env, "net/sf/duro/PossrepObject");
    }

    jobjarr = (*env)->NewObjectArray(env, (jsize) size, clazz, NULL);
    if (jobjarr == NULL)
        return NULL;
    for (i = 0; i < size; i++) {
         objval = duro_obj_to_jobj(env, RDB_array_get(arrp, (RDB_int) i, &JDuro_ec),
                 dInstance);
         if (objval == NULL)
             return NULL;
         (*env)->SetObjectArrayElement(env, jobjarr, (jsize) i, objval);
    }
    return jobjarr;
}

/*
 * Convert *objp to a Java object.
 */
static jobject
duro_obj_to_jobj(JNIEnv *env, const RDB_object *objp, jobject dInstance)
{
    jmethodID constructorID;
    jclass clazz;
    jobject jobj = NULL;
    RDB_type *typ = RDB_obj_type(objp);

    if (RDB_is_tuple(objp))
        return tuple_to_jobj(env, objp, dInstance);
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
        clazz = (*env)->FindClass(env, "java/lang/Integer");
        constructorID = (*env)->GetMethodID(env, clazz, "<init>", "(I)V");
        return (*env)->NewObject(env, clazz, constructorID, (jint) RDB_obj_int(objp));
    }
    if (typ == &RDB_FLOAT) {
        clazz = (*env)->FindClass(env, "java/lang/Double");
        constructorID = (*env)->GetMethodID(env, clazz, "<init>", "(D)V");
        return (*env)->NewObject(env, clazz, constructorID, (jdouble) RDB_obj_float(objp));
    }
    if (typ == &RDB_BOOLEAN) {
        clazz = (*env)->FindClass(env, "java/lang/Boolean");
        constructorID = (*env)->GetMethodID(env, clazz, "<init>", "(Z)V");
        return (*env)->NewObject(env, clazz, constructorID, (jboolean) RDB_obj_bool(objp));
    }
    if (typ == &RDB_BINARY) {
        void *bp;
        jsize len = (jsize) RDB_binary_length(objp);
        jobj = (*env)->NewByteArray(env, len);
        if (jobj == NULL)
            return NULL;
        if (RDB_binary_get(objp, 0, (size_t) len, &JDuro_ec, &bp, NULL) != RDB_OK) {
            throw_exception_from_error(env, dInstance,
                    "getting byte array failed", &JDuro_ec);
            return NULL;
        }

        (*env)->SetByteArrayRegion(env, jobj, 0, len, bp);
        return jobj;
    }
    if (RDB_type_is_array(typ)) {
        return array_to_jobj(env, objp, dInstance);
    }
    if (RDB_type_is_relation(typ)) {
        return table_to_jobj(env, objp, dInstance);
    }

    /*
     * Create a copy of *objp which is managed by the Java object
     */
    RDB_object *cobjp = RDB_new_obj(&JDuro_ec);
    if (cobjp == NULL) {
        throw_exception_from_error(env, dInstance,
                "creating RDB_object failed", &JDuro_ec);
        goto error;
    }

    if (RDB_copy_obj(cobjp, objp, &JDuro_ec) == RDB_ERROR) {
        throw_exception_from_error(env, dInstance,
                "copying RDB_object failed", &JDuro_ec);
        goto error;
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/PossrepObject");
    if (clazz == NULL)
        goto error;
    constructorID = (*env)->GetMethodID(env, clazz, "<init>",
            "(JLnet/sf/duro/DInstance;)V");
    if (constructorID == NULL) {
        goto error;
    }

    /* objp will be managed by the Java object */
    jobj = (*env)->NewObject(env, clazz, constructorID, (jlong) (intptr_t) cobjp,
            dInstance);
    if (jobj == NULL)
        goto error;
    return jobj;

error:
    if (cobjp != NULL)
        RDB_free_obj(cobjp, &JDuro_ec);
    return NULL;
}

JNIEXPORT jobject
JNICALL Java_net_sf_duro_DInstance_evaluateI(JNIEnv *env, jobject obj,
        jstring expression)
{
    RDB_object result;
    jobject jresult;
    RDB_expression *exp;
    const char *str;
    Duro_interp *interp = jobj_interp(env, obj);
    if (interp == NULL)
        return NULL;
    str = (*env)->GetStringUTFChars(env, expression, 0);

    exp = Duro_dt_parse_expr_str(NULL, str, interp, &JDuro_ec);
    (*env)->ReleaseStringUTFChars(env, expression, str);
    if (exp == NULL) {
        throw_exception_from_error(env, obj,
                "evaluating expression failed", &JDuro_ec);
        return NULL;
    }

    RDB_init_obj(&result);
    if (Duro_evaluate_retry(exp, interp, &JDuro_ec, &result) != RDB_OK) {
        throw_exception_from_error(env, obj,
                "expression evaluation failed", &JDuro_ec);
        goto error;
    }
    if (RDB_obj_type(&result) == NULL
            && !RDB_is_tuple(&result)) {
        RDB_type *typ = Duro_expr_type_retry(exp, interp, &JDuro_ec);
        if (typ == NULL) {
            throw_exception_from_error(env, obj,
                    "getting expression type failed", &JDuro_ec);
            goto error;
        }
        RDB_obj_set_typeinfo(&result, typ);
    }
    jresult = duro_obj_to_jobj(env, &result, obj);
    RDB_destroy_obj(&result, &JDuro_ec);
    RDB_del_expr(exp, &JDuro_ec);
    return jresult;

error:
    RDB_destroy_obj(&result, &JDuro_ec);
    RDB_del_expr(exp, &JDuro_ec);
    return NULL;
}

static int
jobj_to_duro_obj(JNIEnv *, jobject, RDB_object *, jobject, RDB_exec_context *);

static int
jtuple_to_obj(JNIEnv *env, RDB_object *dstp, jobject obj, jobject dInstance,
        RDB_exec_context *ecp)
{
    jobject iter;
    jobject attrnameobj;
    jobject attrvalobj;
    const char *attrname;
    RDB_object attrval;
    jclass iterclazz;
    jclass setclazz;
    jmethodID hasNextMethodID;
    jmethodID nextMethodID;
    jmethodID getAttributeMethodID;
    jclass tupleclazz = (*env)->GetObjectClass(env, obj);
    jmethodID methodID = (*env)->GetMethodID(env, tupleclazz, "keySet",
            "()Ljava/util/Set;");
    jobject keyset = (*env)->CallObjectMethod(env, obj, methodID);

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
    getAttributeMethodID = (*env)->GetMethodID(env, tupleclazz, "getAttribute",
            "(Ljava/lang/String;)Ljava/lang/Object;");

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
        if (jobj_to_duro_obj(env, attrvalobj, &attrval, dInstance, ecp) != 0) {
            RDB_destroy_obj(&attrval, ecp);
            return -1;
        }
        if (RDB_tuple_set(dstp, attrname, &attrval, ecp) != RDB_OK) {
            throw_exception_from_error(env, dInstance, "setting tuple attribute failed", ecp);
            RDB_destroy_obj(&attrval, ecp);
            return -1;
        }
        RDB_destroy_obj(&attrval, ecp);

        (*env)->ReleaseStringUTFChars(env, attrnameobj, attrname);
    }
    return 0;
}

static int
jobj_to_table(JNIEnv *env, jobject obj, RDB_object *dstp,
        jobject dInstance, RDB_exec_context *ecp)
{
    RDB_object tpl;
    jobject iter;
    jobject elem;
    jclass iterclazz, setclazz;
    jmethodID methodID, hasNextMethodID, nextMethodID;
    Duro_interp *interp = jobj_interp(env, dInstance);
    if (interp == NULL)
        return -1;

    if (RDB_obj_type(dstp) == NULL
            || !RDB_type_is_relation(RDB_obj_type(dstp))) {
        RDB_raise_type_mismatch("not a table", &JDuro_ec);
        throw_exception_from_error(env, dInstance,
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
    if (RDB_delete(dstp, NULL, &JDuro_ec, Duro_dt_tx(interp)) != RDB_OK) {
        throw_exception_from_error(env, dInstance, "delete failed", &JDuro_ec);
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
        if (jobj_to_duro_obj(env, elem, &tpl, dInstance, &JDuro_ec) == -1)
            goto error;

        /* Insert tuple into table */
        if (RDB_insert(dstp, &tpl, &JDuro_ec, Duro_dt_tx(interp)) != RDB_OK) {
            throw_exception_from_error(env, dInstance, "insert failed", &JDuro_ec);
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
 * If the Java object is a java.util.Set, *dstp must be an empty table.
 */
static int
jobj_to_duro_obj(JNIEnv *env, jobject obj, RDB_object *dstp,
        jobject dInstance, RDB_exec_context *ecp)
{
    jmethodID methodID;
    jclass clazz = (*env)->FindClass(env, "java/lang/String");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        const char *strval = (*env)->GetStringUTFChars(env, obj, 0);
        if (RDB_string_to_obj(dstp, strval, ecp) != RDB_OK) {
            throw_exception_from_error(env, dInstance, "getting string data failed", ecp);
            return -1;
        }
        return 0;
    }

    clazz = (*env)->FindClass(env, "java/lang/Integer");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        methodID = (*env)->GetMethodID(env, clazz, "intValue", "()I");
        RDB_int_to_obj(dstp, (RDB_int) (*env)->CallIntMethod(env, obj, methodID));
        return 0;
    }

    clazz = (*env)->FindClass(env, "java/lang/Double");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        methodID = (*env)->GetMethodID(env, clazz, "doubleValue", "()D");
        RDB_float_to_obj(dstp, (RDB_float) (*env)->CallDoubleMethod(env, obj, methodID));
        return 0;
    }

    clazz = (*env)->FindClass(env, "java/lang/Boolean");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        methodID = (*env)->GetMethodID(env, clazz, "booleanValue", "()Z");
        RDB_bool_to_obj(dstp, (RDB_float) (*env)->CallBooleanMethod(env, obj, methodID));
        return 0;
    }

    clazz = (*env)->FindClass(env, "[B");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        jsize len = (*env)->GetArrayLength(env, obj);
        jbyte *bp = (*env)->GetByteArrayElements(env, obj, NULL);
        if (bp == NULL)
            return -1;
        if (RDB_binary_set (dstp, 0, bp, (size_t) len, &JDuro_ec) != RDB_OK) {
            throw_exception_from_error(env, dInstance, "setting binary data failed",
                    &JDuro_ec);
            return -1;
        }
        (*env)->ReleaseByteArrayElements(env, obj, bp, JNI_ABORT);
        return 0;
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/Tuple");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        return jtuple_to_obj(env, dstp, obj, dInstance, ecp);
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/PossrepObject");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        jfieldID refFieldID = (*env)->GetFieldID(env, clazz, "ref", "J");
        RDB_object *objp = (RDB_object *) (intptr_t)
                (*env)->GetLongField(env, obj, refFieldID);
        if (objp == NULL)
            return -1;

        if (RDB_copy_obj(dstp, objp, ecp) != RDB_OK) {
            throw_exception_from_error(env, dInstance, "copy failed", ecp);
            return -1;
        }
        return 0;
    }

    clazz = (*env)->FindClass(env, "java/util/Set");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        return jobj_to_table(env, obj, dstp, dInstance, &JDuro_ec);
    }

    clazz = (*env)->FindClass(env, "java/lang/IllegalArgumentException");
    (*env)->ThrowNew(env, clazz, "unsupported class");
    return -1;
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DInstance_setVarI(JNIEnv *env, jobject obj,
        jstring name, jobject value)
{
    const char *namestr;
    RDB_object *dstp;
    Duro_interp *interp = jobj_interp(env, obj);
    if (interp == NULL)
        return;
    namestr = (*env)->GetStringUTFChars(env, name, 0);
    if (namestr == NULL)
        return;

    dstp = Duro_lookup_var(namestr, interp, &JDuro_ec);
    if (dstp == NULL) {
        throw_exception_from_error(env, obj, "variable lookup failed", &JDuro_ec);
        goto cleanup;
    }

    if (jobj_to_duro_obj(env, value, dstp, obj, &JDuro_ec) != 0) {
        goto cleanup;
    }

cleanup:
    (*env)->ReleaseStringUTFChars(env, name, namestr);
}

JNIEXPORT jobject
JNICALL Java_net_sf_duro_PossrepObject_getPropertyI(JNIEnv *env, jobject obj,
        jstring name, jobject dInstance)
{
    const char *namestr;
    RDB_transaction *txp;
    RDB_object compval;
    Duro_interp *interp;
    RDB_environment *envp;
    jobject result;
    jclass clazz = (*env)->GetObjectClass(env, obj);
    jfieldID refFieldID = (*env)->GetFieldID(env, clazz, "ref", "J");
    RDB_object *objp = (RDB_object *) (intptr_t)
            (*env)->GetLongField(env, obj, refFieldID);
    interp = jobj_interp(env, dInstance);
    if (interp == NULL)
        return NULL;

    txp = Duro_dt_tx(interp);
    if (txp == NULL) {
        envp = Duro_dt_env(interp);
    }

    namestr = (*env)->GetStringUTFChars(env, name, 0);
    if (namestr == NULL)
        return NULL;

    /* Get property */
    RDB_init_obj(&compval);
    if (RDB_obj_property(objp, namestr, &compval, envp, &JDuro_ec, txp) != RDB_OK) {
        throw_exception_from_error(env, dInstance, "getting property failed", &JDuro_ec);
        goto error;
    }
    result = duro_obj_to_jobj(env, &compval, dInstance);
    if (result == NULL)
        goto error;

    RDB_destroy_obj(&compval, &JDuro_ec);
    (*env)->ReleaseStringUTFChars(env, name, namestr);
    return result;

error:
    RDB_destroy_obj(&compval, &JDuro_ec);
    (*env)->ReleaseStringUTFChars(env, name, namestr);
    return NULL;
}

JNIEXPORT void
JNICALL Java_net_sf_duro_PossrepObject_setPropertyI(JNIEnv *env, jobject obj,
        jstring name, jobject dInstance, jobject value)
{
    const char *namestr;
    RDB_transaction *txp;
    RDB_object compval;
    Duro_interp *interp;
    RDB_environment *envp;
    jclass clazz = (*env)->GetObjectClass(env, obj);
    jfieldID refFieldID = (*env)->GetFieldID(env, clazz, "ref", "J");
    RDB_object *objp = (RDB_object *) (intptr_t)
            (*env)->GetLongField(env, obj, refFieldID);
    interp = jobj_interp(env, dInstance);
    if (interp == NULL)
        return;

    txp = Duro_dt_tx(interp);
    if (txp == NULL) {
        envp = Duro_dt_env(interp);
    }

    namestr = (*env)->GetStringUTFChars(env, name, 0);
    if (namestr == NULL)
        return;

    RDB_init_obj(&compval);

    if (jobj_to_duro_obj(env, value, &compval, dInstance, &JDuro_ec) != 0) {
        RDB_destroy_obj(&compval, &JDuro_ec);
        return;
    }

    if (RDB_obj_set_propery(objp, namestr, &compval, envp, &JDuro_ec, txp) != RDB_OK)
        throw_exception_from_error(env, dInstance, "setting property failed",
                &JDuro_ec);
    RDB_destroy_obj(&compval, &JDuro_ec);
}

JNIEXPORT void
JNICALL Java_net_sf_duro_PossrepObject_disposeI(JNIEnv *env, jobject obj,
        jobject dInstance)
{
    jclass clazz = (*env)->GetObjectClass(env, obj);
    jfieldID refFieldID = (*env)->GetFieldID(env, clazz, "ref", "J");
    RDB_object *objp = (RDB_object *) (intptr_t)
            (*env)->GetLongField(env, obj, refFieldID);
    if (objp == NULL)
        return;
    if (RDB_free_obj(objp, &JDuro_ec) != RDB_OK)
        throw_exception_from_error(env, dInstance, "freeing RDB_object failed",
                &JDuro_ec);
}

JNIEXPORT jstring
JNICALL Java_net_sf_duro_PossrepObject_getTypeName(JNIEnv *env, jobject obj)
{
    RDB_type *typ;
    const char *typename;
    jclass clazz = (*env)->GetObjectClass(env, obj);
    jfieldID refFieldID = (*env)->GetFieldID(env, clazz, "ref", "J");
    RDB_object *objp = (RDB_object *) (intptr_t)
            (*env)->GetLongField(env, obj, refFieldID);
    if (objp == NULL)
        return NULL;

    typ = RDB_obj_type(objp);
    if (typ == NULL)
        return NULL;

    typename = RDB_type_name(typ);
    if (typename == NULL)
        return NULL;

    return (*env)->NewStringUTF(env, typename);
}
