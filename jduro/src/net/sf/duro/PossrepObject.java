package net.sf.duro;

public final class PossrepObject {
    private long ref;
    private DuroDSession dInstance;

    private native void setPropertyI(String name, DuroDSession dInstance, Object value)
	    throws DException;

    private native Object getPropertyI(String name, DuroDSession dInstance)
	    throws DException;

    private native Object disposeI(DuroDSession dInstance);

    public native String getTypeName();

    PossrepObject(long ref, DuroDSession dInstance) {
	if (dInstance == null)
	    throw new NullPointerException();
	
	this.ref = ref;
	this.dInstance = dInstance;
    }

    public void setProperty(String name, Object value) throws DException {
	synchronized(DuroDSession.class) {
	    setPropertyI(name, dInstance, value);
	}
    }

    public Object getProperty(String name) throws DException {
	synchronized(DuroDSession.class) {
	    return getPropertyI(name, dInstance);
	}
    }

    public void dispose() {
	synchronized(DuroDSession.class) {
	    if (dInstance != null) {
	        disposeI(dInstance);
	        dInstance = null;
	    }
	}
    }
    
    protected void finalize() {
	dispose();
    }
}
