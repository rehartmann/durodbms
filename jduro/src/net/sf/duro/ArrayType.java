package net.sf.duro;

public class ArrayType extends Type {

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
