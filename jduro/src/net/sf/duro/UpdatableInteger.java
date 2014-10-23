package net.sf.duro;

/**
 * Instances of this class wrap a value of the primitive type int in an object.
 * Unlike with java.lang.Integer, the value can be updated.
 * 
 * @author Rene Hartmann
 *
 */
public class UpdatableInteger extends Number implements
        Comparable<UpdatableInteger> {

    private static final long serialVersionUID = 1L;

    private int value;

    /**
     * Constructs a newly allocated UpdatableInteger object representing the
     * value 0 (zero).
     */
    public UpdatableInteger() {
        value = 0;
    }

    /**
     * Constructs a newly allocated UpdatableInteger object representing the
     * {@code value} argument.
     */
    public UpdatableInteger(int value) {
        this.value = value;
    }

    @Override
    public double doubleValue() {
        return (double) value;
    }

    @Override
    public float floatValue() {
        return (float) value;
    }

    @Override
    public int intValue() {
        return value;
    }

    @Override
    public long longValue() {
        return (long) value;
    }

    /**
     * Compares two UpdatableInteger objects.
     */
    @Override
    public int compareTo(UpdatableInteger n) {
        return value - n.value;
    }

    /**
     * Returns a hash code for this object.
     */
    @Override
    public int hashCode() {
        return value;
    }

    /**
     * Compares this object to the specified object. The result is true if and
     * only if the argument is not null and is an UpdatableInteger object that
     * contains the same value as this object.
     * 
     * @param obj
     *            the object to compare with.
     * 
     * @return true if the objects are the same; false otherwise.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof UpdatableInteger) {
            return value == ((UpdatableInteger) obj).value;
        }
        return false;
    }

    /**
     * Returns a String object representing this Integer's value. The value is
     * converted to signed decimal representation and returned as a string.
     * 
     * @return a string representation of the value of this object in base 10.
     */
    @Override
    public String toString() {
        return Integer.toString(value);
    }

    /**
     * Sets the value wrapped by this object.
     * 
     * @param value
     *            the new value wrapped by this object.
     */
    public void setValue(int value) {
        this.value = value;
    }
}
