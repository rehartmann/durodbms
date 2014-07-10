package net.sf.duro;

/**
 * This class implements a PossrepObject.
 * @author Rene Hartmann
 *
 */
public class DuroPossrepObject implements PossrepObject {
    private long ref;
    private DuroDSession session;
    private ScalarType type;

    private static native void setProperty(String name, DuroDSession session,
	    long ref, Object value) throws DException;

    private static native Object getProperty(String name, DuroDSession s,
	    long ref) throws DException;

    private static native Object dispose(DuroDSession dInstance);

    private static native boolean equals(long ref1, long ref2,
	    DuroDSession dInstance);

    private static native String getTypeName(long ref);

    private static native Possrep[] getPossreps(long ref, DuroDSession session);

    public String getTypeName() {
	return getType().getName();
    }

    public ScalarType getType() {
	if (type == null) {
	    type = new ScalarType(getTypeName(ref), getPossreps(ref, session));
	}
	return type;
    }

    DuroPossrepObject(long ref, DuroDSession dInstance) {
	if (dInstance == null)
	    throw new NullPointerException();
	
	this.ref = ref;
	this.session = dInstance;
	this.type = null; /* Getting the type is delayed until getType() is called */
    }

    public void setProperty(String name, Object value) throws DException {
	synchronized(DuroDSession.class) {
	    setProperty(name, session, ref, value);
	}
    }

    public Object getProperty(String name) throws DException {
	synchronized(DuroDSession.class) {
	    return getProperty(name, session, ref);
	}
    }

    /**
     * Determines if two PossrepObjects are equal using
     * the DuroDBMS library function RDB_obj_equals().
     */
    public boolean equals(Object obj) {
	DuroPossrepObject probj;
	try {
	    probj = (DuroPossrepObject) obj;
	} catch (ClassCastException ex) {
	    return false;
	}

	synchronized(DuroDSession.class) {
	    return equals(ref, probj.ref, session);
	}
    }

    /**
     * Returns a hash code for this PossrepObject, based on its property values.
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
	Possrep[] possreps = getPossreps(ref, session);
	if (possreps == null)
	    return 0;
	VarDef[] components = possreps[0].getComponents();
	
	/* Use components of 1st possrep to calculate the hash code */
	int code = 0;
	for (int i = 0; i < components.length; i++) {
	    try {
	    	code += getProperty(components[i].getName()).hashCode();
	    } catch (DException ex) {
		throw new UnsupportedOperationException(ex);
	    }
	}
	return code;
    }
    
    public void dispose() {
	synchronized(DuroDSession.class) {
	    if (session != null) {
	        dispose(session);
	        session = null;
	    }
	}
    }
    
    protected void finalize() {
	dispose();
    }
}
