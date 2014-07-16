package net.sf.duro;

/**
 * Instances of this class represent a possible representation.
 * Unlike tuple attributes, the order of the properties is important.
 */
public class Possrep {
    private VarDef[] components;

    public Possrep(VarDef[] components) {
	this.components = components;
    }

    public VarDef[] getComponents() {
	return components;
    }
    
    public VarDef getComponent(int i) {
	return components[i];
    }
}
