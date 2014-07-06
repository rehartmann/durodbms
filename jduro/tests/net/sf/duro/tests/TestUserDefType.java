package net.sf.duro.tests;

import static org.junit.Assert.*;

import java.io.File;

import net.sf.duro.DException;
import net.sf.duro.DSession;
import net.sf.duro.DuroDSession;
import net.sf.duro.PossrepObject;
import net.sf.duro.ScalarType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUserDefType {

    private DSession session;

    @Before
    public void setUp() throws Exception {
	session = DuroDSession.createSession();

	session.execute("create_env('dbenv');"
		      + "create_db('D');"
		      + "current_db := 'D';");
    }

    @After
    public void tearDown() throws DException {
	session.close();

	// Delete environment directory
	File envdir = new File("dbenv");
	for (File f: envdir.listFiles()) {
	    f.delete();
	}
	envdir.delete();
    }

    @Test
    public void test() throws DException {
        session.execute("begin tx;");
        session.execute("type len possrep { l int } constraint l >= 0 init len(0);");
	session.execute("implement type len; end implement;");
	session.execute("var l len;"
		      + "l := len(5);");
        PossrepObject l = (PossrepObject) session.evaluate("l");
	Object lint = session.evaluate("the_l(l)");
	session.execute("var arrl array len;"
		      + "length(arrl) := 1;"
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

}
