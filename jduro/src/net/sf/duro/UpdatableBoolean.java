package net.sf.duro;

import java.io.Serializable;

public class UpdatableBoolean implements Serializable,
	Comparable<UpdatableBoolean> {

    private static final long serialVersionUID = 1L;

    private boolean value;

    public UpdatableBoolean() {
	value = false;
    }

    public UpdatableBoolean(boolean b) {
	value = b;
    }

    @Override
    public int compareTo(UpdatableBoolean b) {
	return (value ? 1 : 0) - (b.value ? 1 : 0) ;
    }

    public int hashCode() {
	return value ? 1231 : 1237;
    }

    public boolean equals(Object obj) {
	try {
	    return value == ((UpdatableBoolean)obj).value;
	} catch (ClassCastException ex) {
	    return false;
	}
    }

    public String toString() {
	return value ? "true" : "false";
    }

    public boolean booleanValue() {
	return value;
    }

    public void setValue(boolean b) {
	value = b;
    }
}
