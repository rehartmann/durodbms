package net.sf.duro;

public class UpdatableDouble extends Number
	implements Comparable<UpdatableDouble> {

    private static final long serialVersionUID = 1L;

    private double value; 

    public UpdatableDouble() {
	value = 0.0;
    }

    public UpdatableDouble(double value) {
	this.value = value;
    }

    @Override
    public double doubleValue() {
	return value;
    }

    @Override
    public float floatValue() {
	return (float) value;
    }

    @Override
    public int intValue() {
	return (int) value;
    }

    @Override
    public long longValue() {
	return (long) value;
    }

    @Override
    public int compareTo(UpdatableDouble o) {
	return Double.compare(value, o.value);
    }

    public void setValue(double value) {
	this.value = value;
    }
}
