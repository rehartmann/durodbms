package net.sf.duro.tests;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;

import net.sf.duro.ArrayType;
import net.sf.duro.DException;
import net.sf.duro.DSession;
import net.sf.duro.Possrep;
import net.sf.duro.PossrepObject;
import net.sf.duro.RelationType;
import net.sf.duro.ScalarType;
import net.sf.duro.TupleType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUserDefType {

    private DSession session;

    @Before
    public void setUp() throws Exception {
        session = DSession.createSession();

        session.execute("create_env('dbenv');" + "create_db('D');"
                + "current_db := 'D';");
    }

    @After
    public void tearDown() {
        session.close();

        // Delete environment directory
        File envdir = new File("dbenv");
        for (File f : envdir.listFiles()) {
            f.delete();
        }
        envdir.delete();
    }

    @Test
    public void testScalarProp() {
        session.execute("begin tx;");
        session.execute("type len possrep { l int } constraint l >= 0 init len(0);");
        session.execute("implement type len; end implement;");
        session.execute("var l len;" + "l := len(5);");
        PossrepObject l = (PossrepObject) session.evaluate("l");
        Object lint = session.evaluate("the_l(l)");
        session.execute("var arrl array len;" + "length(arrl) := 1;"
                + "arrl[0] := len(5);");
        Object arrl = session.evaluate("arrl");

        assertEquals(lint, Integer.valueOf(5));

        assertEquals(session.evaluate("l"), session.evaluate("len(5)"));

        assertFalse(session.evaluate("l").equals(session.evaluate("len(6)")));

        assertArrayEquals(new PossrepObject[] { l }, (PossrepObject[]) arrl);

        assertEquals(session.evaluate("l").hashCode(),
                session.evaluate("len(5)").hashCode());

        session.execute("commit;");
    }

    @Test
    public void testNonscalarProp() {
        session.execute("begin tx;");
        session.execute("type t possrep { a array int, b tup { attr int }, c rel { attr int } }"
                + " init t (array int(), tup { attr 0 }, rel { tup { attr 0 } });");
        session.execute("implement type t; end implement;");

        ScalarType t = ScalarType.fromString("t", session);

        Possrep pr = t.getPossreps()[0];
        assertEquals(pr.getComponent(0).getType().getClass(), ArrayType.class);
        assertEquals(pr.getComponent(1).getType().getClass(), TupleType.class);
        assertEquals(pr.getComponent(2).getType().getClass(),
                RelationType.class);

        session.execute("commit;");
    }
}
