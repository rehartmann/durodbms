package net.sf.duro;

import java.io.Serializable;

/**
 * Instances of this class wrap a value of the primitive type
 * boolean in an object. Unlike java.lang.Boolean, the value
 * can be updated. 
 * 
 * @author Rene Hartmann
 *
 */
public class UpdatableBoolean implements Serializable,
	Comparable<UpdatableBoolean> {

    private static final long serialVersionUID = 1L;

    private boolean value;

    /**
     * Constructs a newly allocated UpdatableBoolean object
     * representing the value {@code false}. 
     */
    public UpdatableBoolean() {
	this.value = false;
    }

    /**
     * Constructs a newly allocated UpdatableBoolean object
     * representing the {@code value} argument. 
     */
    public UpdatableBoolean(boolean value) {
	this.value = value;
    }

    /**
     * Compares two UpdatableBoolean objects.
     */
    @Override
    public int compareTo(UpdatableBoolean b) {
	return (value ? 1 : 0) - (b.value ? 1 : 0) ;
    }

    /**
     * Returns a hash code for this object.
     */
    @Override
    public int hashCode() {
	return value ? 1231 : 1237;
    }

    /**
     * Compares this object to the specified object.
     * The result is true if and only if the argument is not null
     * and is an UpdatableBoolean object that contains the same boolean
     * value as this object.
     * 
     * @param obj the object to compare with.
     * 
     * @return true if the objects are the same; false otherwise.
     */
    @Override
    public boolean equals(Object obj) {
	if (obj instanceof UpdatableBoolean) {
            return value == ((UpdatableBoolean)obj).value;
        }
        return false;
    }

    /**
     * Returns a String object representing this UpdatableBoolean's value.
     * If this object represents the value true,
     * a string equal to "true" is returned.
     * Otherwise, a string equal to "false" is returned.
     * 
     *  @return a string representation of this object.
     */
    @Override
    public String toString() {
	return value ? "true" : "false";
    }

    /**
     * Returns the value of this UpdatableBoolean object as a
     * boolean primitive.
     *  
     * @return the boolean value of this object.
     */
    public boolean booleanValue() {
	return value;
    }

    /**
     * Set the boolean value this object represents.
     * @param b
     */
    public void setValue(boolean b) {
	value = b;
    }
}
