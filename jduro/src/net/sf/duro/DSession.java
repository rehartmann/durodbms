package net.sf.duro;

/**
 * This class implements the DSession interface.
 * @author Rene Hartmann
 *
 */
public class DSession {
    static { 
        System.loadLibrary("jduro"); 
    }

    private long interp = 0L; // Contains the pointer to the Duro_interp structure

    private DSession() {}

    native private void initInterp() throws DException;

    native private void destroyInterp() throws DException;

    native private void executeI(String s) throws DException,
            ClassNotFoundException, NoSuchMethodException;

    native private Object evaluateI(String expr) throws DException,
            ClassNotFoundException, NoSuchMethodException;

    native private void setVarI(String name, Object v) throws DException;

    /**
     * Creates a DSession.
     * @return the DSession created.
     * @throws DException	if a Duro error occurs
     */
    static public DSession createSession() throws DException {
	DSession instance = new DSession();
	synchronized(DSession.class) {
	    instance.initInterp();
	}
	return instance;
    }

    public void close() throws DException {
	synchronized(DSession.class) {
	    destroyInterp();
	}
    }

    public void execute(String s) throws DException {
	try {
	    synchronized(DSession.class) {
		executeI(s);
	    }
	} catch (ClassNotFoundException ex) {
	    throw new IllegalStateException(ex);
	} catch (NoSuchMethodException ex) {
	    throw new IllegalStateException(ex);
	}
    }

    public Object evaluate(String expr) throws DException {
	try {
	    synchronized(DSession.class) {
		return evaluateI(expr);
	    }
	} catch (ClassNotFoundException ex) {
	    throw new IllegalStateException(ex);
	} catch (NoSuchMethodException ex) {
	    throw new IllegalStateException(ex);
	}
    }

    public void setVar(String name, Object v) throws DException
    {
	synchronized(DSession.class) {
	    setVarI(name, v);
	}
    }
}
