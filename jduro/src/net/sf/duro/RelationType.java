package net.sf.duro;

import java.util.Set;

/**
 * Instances of this class represent a DuroDBMS relation type.
 * 
 * @author Rene Hartmann
 *
 */
public class RelationType extends Type {

    private static final long serialVersionUID = 1L;

    private final TupleType baseType;

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

    public Set<String> attributeNames() {
        return baseType.attributeNames();
    }
}
