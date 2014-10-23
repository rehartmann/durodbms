package net.sf.duro;

/**
 * Instances of this class represent a DuroDBMS array type.
 * 
 * @author Rene Hartmann
 *
 */
public class ArrayType extends Type {
    private static final long serialVersionUID = 1L;

    private Type baseType;

    public ArrayType(Type baseType) {
        this.baseType = baseType;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    public Type getBaseType() {
        return baseType;
    }
}
