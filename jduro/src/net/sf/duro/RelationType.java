package net.sf.duro;

import java.util.Set;

public class RelationType extends Type {

    private TupleType baseType;

    public RelationType(VarDef[] attributes) {
	baseType = new TupleType(attributes);
    }
    
    @Override
    public String getName() {
	return null;
    }

    @Override
    public boolean isScalar() {
	return false;
    }

    public Set attributeNames() {
	return baseType.attributeNames();
    }
}
