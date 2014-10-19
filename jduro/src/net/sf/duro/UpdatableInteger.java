package net.sf.duro;

/**
 * Instances of this class wrap a value of the primitive type
 * int in an object. Unlike java.lang.Inteer, the value
 * can be updated. 
 * 
 * @author Rene Hartmann
 *
 */
public class UpdatableInteger extends Number
	implements Comparable<UpdatableInteger> {

    private static final long serialVersionUID = 1L;

    private int value;

    public UpdatableInteger() {
	value = 0;
    }

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

    @Override
    public int compareTo(UpdatableInteger n) {
	return value - n.value;
    }

    @Override
    public int hashCode() {
	return value;
    }

    @Override
    public boolean equals(Object obj) {
	if (obj instanceof UpdatableInteger) {
            return value == ((UpdatableInteger)obj).value;
        }
        return false;
    }

    @Override
    public String toString() {
	return Integer.toString(value);
    }

    public void setValue(int value) {
	this.value = value;
    }
}
