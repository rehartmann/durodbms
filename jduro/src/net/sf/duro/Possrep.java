package net.sf.duro;

/**
 * Instances of this class represent a possible representation. Unlike tuple
 * attributes, the order of the properties is important.
 */
public class Possrep {
    private NameTypePair[] components;

    public Possrep(NameTypePair[] components) {
        this.components = components;
    }

    public NameTypePair[] getComponents() {
        return components;
    }

    public NameTypePair getComponent(int i) {
        return components[i];
    }
}
