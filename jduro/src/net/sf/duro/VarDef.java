package net.sf.duro;

/**
 * Instances of this class represent a name/type pair.
 */
public class VarDef {
    private final String name;

    private final Type type;

    public VarDef(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }
}
