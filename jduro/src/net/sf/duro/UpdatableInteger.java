package net.sf.duro;

public class UpdatableInteger extends Number
	implements Comparable<UpdatableInteger> {

    /**
     * 
     */
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

    public void setValue(int value) {
	this.value = value;
    }
}
