/*
 * probj.c
 *
 *  Created on: 02.07.2014
 *      Author: Rene Hartmann
 */

#include "jduro.h"

JNIEXPORT jobject
JNICALL Java_net_sf_duro_DefaultPossrepObject_getProperty(JNIEnv *env, jclass clazz,
        jstring name, jobject dInstance, jlong ref)
{
    const char *namestr;
    RDB_transaction *txp;
    RDB_object compval;
    JDuro_session *sessionp;
    RDB_environment *envp;
    jobject result;
    RDB_object *objp = (RDB_object *) (intptr_t) ref;

    sessionp = JDuro_jobj_session(env, dInstance);
    if (sessionp == NULL) {
        return NULL;
    }

    txp = Duro_dt_tx(&sessionp->interp);
    if (txp == NULL) {
        envp = Duro_dt_env(&sessionp->interp);
    }

    namestr = (*env)->GetStringUTFChars(env, name, 0);
    if (namestr == NULL)
        return NULL;

    /* Get property */
    RDB_init_obj(&compval);
    if (RDB_obj_property(objp, namestr, &compval, envp, &sessionp->ec, txp) != RDB_OK) {
        JDuro_throw_exception_from_error(env, sessionp, "getting property failed", &sessionp->ec);
        goto error;
    }
    result = JDuro_duro_obj_to_jobj(env, &compval, RDB_FALSE, sessionp);
    if (result == NULL)
        goto error;

    RDB_destroy_obj(&compval, &sessionp->ec);
    (*env)->ReleaseStringUTFChars(env, name, namestr);
    return result;

error:
    RDB_destroy_obj(&compval, &sessionp->ec);
    (*env)->ReleaseStringUTFChars(env, name, namestr);
    return NULL;
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DefaultPossrepObject_setProperty(JNIEnv *env, jclass clazz,
        jstring name, jobject dInstance, jlong ref, jobject value)
{
    const char *namestr;
    RDB_transaction *txp;
    RDB_object compval;
    JDuro_session *sessionp;
    RDB_environment *envp;
    RDB_object *objp = (RDB_object *) (intptr_t) ref;
    sessionp = JDuro_jobj_session(env, dInstance);
    if (sessionp == NULL)
        return;

    txp = Duro_dt_tx(&sessionp->interp);
    if (txp == NULL) {
        envp = Duro_dt_env(&sessionp->interp);
    }

    namestr = (*env)->GetStringUTFChars(env, name, 0);
    if (namestr == NULL)
        return;

    RDB_init_obj(&compval);

    if (JDuro_jobj_to_duro_obj(env, value, &compval, sessionp, &sessionp->ec) != 0) {
        RDB_destroy_obj(&compval, &sessionp->ec);
        return;
    }

    if (RDB_obj_set_property(objp, namestr, &compval, envp, &sessionp->ec, txp) != RDB_OK)
        JDuro_throw_exception_from_error(env, sessionp, "setting property failed",
                &sessionp->ec);
    RDB_destroy_obj(&compval, &sessionp->ec);
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DefaultPossrepObject_dispose(JNIEnv *env, jclass clazz,
        jobject dInstance, jlong ref)
{
    JDuro_session *sessionp;
    RDB_object *objp = (RDB_object *) (intptr_t) ref;
    if (objp == NULL)
        return;
    sessionp = JDuro_jobj_session(env, dInstance);
    if (sessionp == NULL)
        return;
    if (RDB_free_obj(objp, &sessionp->ec) != RDB_OK)
        JDuro_throw_exception_from_error(env, sessionp, "freeing RDB_object failed",
                &sessionp->ec);
}

JNIEXPORT jstring
JNICALL Java_net_sf_duro_DefaultPossrepObject_getTypeName(JNIEnv *env, jclass clazz,
        jlong ref)
{
    RDB_type *typ;
    const char *typename;
    RDB_object *objp = (RDB_object *) (intptr_t) ref;
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

JNIEXPORT jboolean
JNICALL Java_net_sf_duro_DefaultPossrepObject_equals(JNIEnv *env, jclass clazz,
        jlong ref1, jlong ref2, jobject dInstance)
{
    RDB_bool result;
    RDB_object *obj1p, *obj2p;
    RDB_transaction *txp;
    JDuro_session *sessionp = JDuro_jobj_session(env, dInstance);
    if (sessionp == NULL)
        return (jboolean) RDB_FALSE;
    txp = Duro_dt_tx(&sessionp->interp);

    obj1p = (RDB_object *) (intptr_t) ref1;
    if (obj1p == NULL)
        return (jboolean) RDB_FALSE;

    obj2p = (RDB_object *) (intptr_t) ref2;
    if (obj2p == NULL)
        return (jboolean) RDB_FALSE;

    if (RDB_obj_equals(obj1p, obj2p, &sessionp->ec, txp, &result) != RDB_OK) {
        /* If the types differ, return FALSE */
        if (RDB_obj_type(RDB_get_err(&sessionp->ec)) == &RDB_TYPE_MISMATCH_ERROR) {
            return (jboolean) RDB_FALSE;
        }

        JDuro_throw_exception_from_error(env, sessionp, "RDB_obj_equals() failed",
                &sessionp->ec);
        return (jboolean) RDB_FALSE;
    }
    return (jboolean) result;
}

static jobject
possrep_to_jobj(JNIEnv *, const RDB_possrep *, jclass);

static jobject
duro_scalar_type_to_jobj(JNIEnv *env, const RDB_type *typ, jobject session)
{
    jclass scalarTypeClass;
    jmethodID fromStringID;
    const char *typename = RDB_type_name(typ);
    jstring jtypename = (*env)->NewStringUTF(env, typename);

    scalarTypeClass =  (*env)->FindClass(env, "net/sf/duro/ScalarType");
    if (scalarTypeClass == NULL)
        return NULL;

    fromStringID = (*env)->GetStaticMethodID(env, scalarTypeClass, "fromString",
            "(Ljava/lang/String;Lnet/sf/duro/DSession;)Lnet/sf/duro/ScalarType;");
    if (fromStringID == NULL)
        return NULL;

    return (*env)->CallStaticObjectMethod(env, scalarTypeClass, fromStringID,
            jtypename, session);
}

static jobject
duro_type_to_jobj(JNIEnv *, RDB_type *, jobject);

static jobjectArray
attributes_to_vardefs(JNIEnv *env, RDB_type *typ, jobject session)
{
    int i;
    jclass vardefClass;
    jobject attrObject;
    jstring attrName;
    jobject attrTyp;
    jobjectArray attrArray;
    jmethodID vardefConstructorID;
    int attrc;
    RDB_attr *attrv = RDB_type_attrs(typ, &attrc);
    if (attrv == NULL)
        return NULL;

    vardefClass =(*env)->FindClass(env, "net/sf/duro/NameTypePair");
    if (vardefClass == NULL)
        return NULL;

    attrArray = (*env)->NewObjectArray(env, (jsize) attrc, vardefClass, NULL);
    if (attrArray == NULL)
        return NULL;

    vardefConstructorID = (*env)->GetMethodID(env, vardefClass, "<init>",
            "(Ljava/lang/String;Lnet/sf/duro/Type;)V");
    if (vardefConstructorID == NULL)
        return NULL;

    for(i = 0; i < attrc; i++) {
        attrName = (*env)->NewStringUTF(env, attrv[i].name);
        if (attrName == NULL)
            return NULL;
        attrTyp = duro_type_to_jobj(env, attrv[i].typ, session);
        if (attrTyp == NULL)
            return NULL;

        attrObject = (*env)->NewObject(env, vardefClass, vardefConstructorID,
                attrName, attrTyp);
        if (attrObject == NULL)
            return NULL;
        (*env)->SetObjectArrayElement(env, attrArray, (jsize) i, attrObject);
        if ((*env)->ExceptionOccurred(env) != NULL)
            return NULL;
    }
    return attrArray;
}

static jobject
duro_type_to_jobj(JNIEnv *env, RDB_type *typ, jobject session)
{
    jclass typeClass;
    jmethodID constructorId;
    jobject baseTypeObj;

    if (RDB_type_is_scalar(typ)) {
        return duro_scalar_type_to_jobj(env, typ, session);
    }
    if (RDB_type_is_relation(typ) || RDB_type_is_tuple(typ)) {
        jobjectArray attributes = attributes_to_vardefs(env, typ, session);
        if (attributes == NULL)
            return NULL;

        if (RDB_type_is_relation(typ)) {
            typeClass = (*env)->FindClass(env, "net/sf/duro/RelationType");
            if (typeClass == NULL)
                return NULL;
            constructorId = (*env)->GetMethodID(env, typeClass, "<init>",
                    "([Lnet/sf/duro/NameTypePair;)V");
        } else {
            typeClass = (*env)->FindClass(env, "net/sf/duro/TupleType");
            if (typeClass == NULL)
                return NULL;
            constructorId = (*env)->GetMethodID(env, typeClass, "<init>",
                    "([Lnet/sf/duro/NameTypePair;)V");
        }
        if (constructorId == NULL)
            return NULL;
        return (*env)->NewObject(env, typeClass, constructorId, attributes);
    } else if (RDB_type_is_array(typ)) {
        baseTypeObj = duro_type_to_jobj(env, RDB_base_type(typ),
                    session);
        if (baseTypeObj == NULL)
            return NULL;

        typeClass = (*env)->FindClass(env, "net/sf/duro/ArrayType");
        if (typeClass == NULL)
            return NULL;
        constructorId = (*env)->GetMethodID(env, typeClass, "<init>",
                "(Lnet/sf/duro/Type;)V");
        if (constructorId == NULL)
            return NULL;
        return (*env)->NewObject(env, typeClass, constructorId, baseTypeObj);
    }
    /* Should never be reached */
    return NULL;
}

static jobject
possrep_to_jobj(JNIEnv *env, const RDB_possrep *possrep, jobject session)
{
    int i;
    jstring prname;
    jstring compname;
    jobject comptype;
    jobject comp;
    jarray compArray;
    jclass prClass;
    jmethodID vardefConstructorID;
    jmethodID prConstructorID;
    jclass vardefClass =(*env)->FindClass(env, "net/sf/duro/NameTypePair");
    if (vardefClass == NULL)
        return NULL;

    vardefConstructorID = (*env)->GetMethodID(env, vardefClass, "<init>",
            "(Ljava/lang/String;Lnet/sf/duro/Type;)V");
    if (vardefConstructorID == NULL)
        return NULL;

    prClass = (*env)->FindClass(env, "net/sf/duro/Possrep");
    if (prClass == NULL)
        return NULL;

    prConstructorID = (*env)->GetMethodID(env, prClass, "<init>",
            "(Ljava/lang/String;[Lnet/sf/duro/NameTypePair;)V");
    if (prConstructorID == NULL)
        return NULL;

    prname = (*env)->NewStringUTF(env, possrep->name);
    if (prname == NULL)
        return NULL;

    /* Create array which is to be passed to the constructor */
    compArray = (*env)->NewObjectArray(env, (jsize) possrep->compc,
            vardefClass, NULL);
    if (compArray == NULL)
        return NULL;

    /* Fill array */
    for (i = 0; i < possrep->compc; i++) {
        compname = (*env)->NewStringUTF(env, possrep->compv[i].name);
        comptype = duro_type_to_jobj(env, possrep->compv[i].typ, session);

        comp = (*env)->NewObject(env, vardefClass, vardefConstructorID,
                compname, comptype);
        if (comp == NULL)
            return NULL;

        (*env)->SetObjectArrayElement(env, compArray, (jsize) i, comp);
        if ((*env)->ExceptionOccurred(env) != NULL)
            return NULL;
    }

    /* Call constructor */
    return (*env)->NewObject(env, prClass, prConstructorID, prname, compArray);
}

static jobjectArray
typePossreps(JNIEnv *env, RDB_type *typ, jobject session) {
    int repc;
    int i;
    jobjectArray prArray;
    jclass prClass;
    jobject jpossrep;
    RDB_possrep *repv = RDB_type_possreps(typ, &repc);
    if (repv == NULL)
        return NULL;

    prClass = (*env)->FindClass(env, "net/sf/duro/Possrep");
    if (prClass == NULL)
        return NULL;

    prArray = (*env)->NewObjectArray(env, (jsize) repc, prClass, NULL);
    if (prArray == NULL)
        return NULL;

    for (i = 0; i < repc; i++) {
        jpossrep = possrep_to_jobj(env, &repv[i], session);
        if (jpossrep == NULL)
            return NULL;

        (*env)->SetObjectArrayElement(env, prArray, (jsize) i, jpossrep);
    }

    return prArray;
}

JNIEXPORT jobjectArray
JNICALL Java_net_sf_duro_DefaultPossrepObject_getPossreps(JNIEnv *env, jclass clazz,
        jlong ref, jobject session)
{
    RDB_type *typ = RDB_obj_type((RDB_object *) (intptr_t) ref);
    if (typ == NULL) {
        return NULL;
    }

    return typePossreps(env, typ, session);
}

JNIEXPORT jobjectArray
JNICALL Java_net_sf_duro_ScalarType_typePossreps(JNIEnv *env, jclass clazz,
        jstring jtypename, jobject session)
{
    const char *namestr;
    RDB_transaction *txp;
    RDB_type *typ;
    JDuro_session *sessionp = JDuro_jobj_session(env, session);
    if (sessionp == NULL) {
        return NULL;
    }

    txp = Duro_dt_tx(&sessionp->interp);

    namestr = (*env)->GetStringUTFChars(env, jtypename, NULL);
    if (namestr == NULL)
        return NULL;
    typ = RDB_get_type(namestr, &sessionp->ec, txp);
    (*env)->ReleaseStringUTFChars(env, jtypename, namestr);
    if (typ == NULL) {
        JDuro_throw_exception_from_error(env, sessionp, "getting type failed",
                &sessionp->ec);
        return NULL;
    }

    return typePossreps(env, typ, session);
}
