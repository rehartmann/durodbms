package net.sf.duro;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

class PossrepInvocationHandler implements InvocationHandler {
    private PossrepObject po;

    PossrepInvocationHandler(PossrepObject po) {
        this.po = po;
    }
    
    @Override
    public Object invoke(Object proxy, Method meth, Object[] args)
            throws Throwable {
        String methname = meth.getName();
        if (methname.startsWith("get") && args == null) {
            return po.getProperty(methname.substring(3, 4).toLowerCase()
                    + methname.substring(4));
        }
        if (methname.startsWith("set") && args.length == 1) {
            po.setProperty(methname.substring(3, 4).toLowerCase()
                    + methname.substring(4), args[0]);
            return null;
        }
        throw new UnsupportedOperationException(methname);
    }

}
