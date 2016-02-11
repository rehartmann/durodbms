package net.sf.duro;

/**
 * Instances of this class represent a possible representation. Unlike tuple
 * attributes, the order of the properties is important.
 */
public class Possrep {
    private String name;
    private NameTypePair[] components;

    public String getName() {
        return name;
    }

    public Possrep(String name, NameTypePair[] components) {
        this.name = name;
        this.components = components;
    }

    public NameTypePair[] getComponents() {
        return components;
    }

    public NameTypePair getComponent(int i) {
        return components[i];
    }
}
