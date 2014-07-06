package net.sf.duro;

public class VarDef {
    private String name;

    private Type type;

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
