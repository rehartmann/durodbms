package net.sf.duro;

/**
 * This class is the default implementation of the PossrepObject interface.
 * Instances of this class keep a pointer to an RDB_object.
 * 
 * @author Rene Hartmann
 *
 */
public class DefaultPossrepObject implements PossrepObject {
    private long ref;
    private DSession session;
    private ScalarType type;

    private static native void setProperty(String name, DSession session,
	    long ref, Object value) throws DException;

    private static native Object getProperty(String name, DSession s,
	    long ref) throws DException;

    private static native Object dispose(DSession dInstance);

    private static native boolean equals(long ref1, long ref2,
	    DSession dInstance);

    private static native String getTypeName(long ref);

    private static native Possrep[] getPossreps(long ref, DSession session);

    public String getTypeName() {
	return getType().getName();
    }

    public ScalarType getType() {
	if (type == null) {
	    type = new ScalarType(getTypeName(ref), getPossreps(ref, session));
	}
	return type;
    }

    DefaultPossrepObject(long ref, DSession dInstance) {
	if (dInstance == null)
	    throw new NullPointerException();
	
	this.ref = ref;
	this.session = dInstance;
	this.type = null; /* Getting the type is delayed until getType() is called */
    }

    public void setProperty(String name, Object value) throws DException {
	synchronized(DSession.class) {
	    setProperty(name, session, ref, value);
	}
    }

    public Object getProperty(String name) throws DException {
	synchronized(DSession.class) {
	    return getProperty(name, session, ref);
	}
    }

    /**
     * Determines if two PossrepObjects are equal using
     * the DuroDBMS library function RDB_obj_equals().
     */
    public boolean equals(Object obj) {
	DefaultPossrepObject probj;
	try {
	    probj = (DefaultPossrepObject) obj;
	} catch (ClassCastException ex) {
	    return false;
	}

	synchronized(DSession.class) {
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
	synchronized(DSession.class) {
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
