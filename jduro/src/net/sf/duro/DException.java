package net.sf.duro;

public class DException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = -8161811884817880524L;

    private Object error = null;

    DException() {}

    DException(String message) {
	super(message);
    }

    DException(String message, Object error) {
	super(message);
	this.error = error;
    }

    public Object getError() {
	return error;
    }

    public String toString() {
	StringBuffer buf = new StringBuffer(super.toString());
	if (error != null && error instanceof PossrepObject) {
	    String typename =  ((PossrepObject)error).getTypeName();
	    if (typename != null) {
		buf.append(", ");
		buf.append(typename);
	    }
	    try {
		String msg = (String) ((PossrepObject)error).getProperty("msg");
		if (msg != null) {
	            buf.append(": ");
		    buf.append(msg);
                }
	    } catch (DException ex) {
		System.out.println("" + ex);
	    }
	}
	return buf.toString();
    }
}
