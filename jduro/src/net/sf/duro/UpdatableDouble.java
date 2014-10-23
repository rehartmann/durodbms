package net.sf.duro;

/**
 * Instances of this class wrap a value of the primitive type double in an
 * object. Unlike java.lang.Double, the value can be updated.
 * 
 * @author Rene Hartmann
 *
 */
public class UpdatableDouble extends Number implements
        Comparable<UpdatableDouble> {

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

    @Override
    public int hashCode() {
        long bits = Double.doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof UpdatableDouble) {
            return value == ((UpdatableDouble) obj).value;
        }
        return false;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    public void setValue(double value) {
        this.value = value;
    }
}
