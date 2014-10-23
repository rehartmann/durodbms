package net.sf.duro.tests;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import net.sf.duro.ByteArray;
import net.sf.duro.DException;
import net.sf.duro.DSession;
import net.sf.duro.DSession;
import net.sf.duro.PossrepObject;
import net.sf.duro.Tuple;
import net.sf.duro.UpdatableBoolean;
import net.sf.duro.UpdatableDouble;
import net.sf.duro.UpdatableInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUserDefOp {

    private DSession session;

    @Before
    public void setUp() throws Exception {
        session = DSession.createSession();

        session.execute("create_env('dbenv');" + "create_db('D');"
                + "current_db := 'D';");
    }

    @After
    public void tearDown() throws Exception {
        session.close();

        // Delete environment directory
        File envdir = new File("dbenv");
        for (File f : envdir.listFiles()) {
            f.delete();
        }
        envdir.delete();
    }

    // Implements the user-defined operator test1
    @SuppressWarnings("unused")
    private static String test1() {
        return "Hello";
    }

    @Test
    public void testReadonly1() throws DException {
        session.execute("begin tx;");

        session.execute("operator test1() returns string extern 'Java'"
                + " 'net.sf.duro.tests.TestUserDefOp.test1';"
                + " end operator;");
        assertEquals("Hello", session.evaluate("test1()"));

        session.execute("commit;");
    }

    @Test
    public void testReadonlyNoMethod() throws DException {
        session.execute("begin tx;");

        session.execute("operator test1() returns string extern 'Java'"
                + " 'net.sf.duro.tests.TestUserDefOp.doesNotExist';"
                + " end operator;");
        try {
            session.evaluate("test1()");
            fail("invocation of operator implemented by non-existing method succeeded");
        } catch (DException ex) {
        }

        session.execute("commit;");
    }

    // Implements the user-defined operator test2
    @SuppressWarnings("unused")
    private static String test2(String arg1, Integer arg2, Double arg3,
            Boolean arg4, byte[] arg5, Tuple arg6, Set<Tuple> arg7,
            String[] arg8) {
        return arg1 + arg2 + arg3 + arg4 + (int) arg5[0] + (int) arg5[1]
                + arg6.getAttribute("s")
                + arg7.iterator().next().getAttribute("n") + arg8[0];
    }

    @Test
    public void testReadonlyOp2() throws DException {
        session.execute("begin tx;");

        session.execute("operator test2(p1 string, p2 int, p3 float,"
                + "p4 boolean, p5 binary, p6 tuple { s string }, "
                + "p7 rel { n int }, p8 array string)"
                + " returns string extern 'Java'"
                + " 'net.sf.duro.tests.TestUserDefOp.test2';"
                + " end operator;");
        assertEquals(
                "yo24.2true15x33tt",
                session.evaluate("test2('yo', 2, 4.2, true, X'0105',"
                        + " tuple { s 'x' }, rel { tup { n 33 } }, array('tt'))"));

        // Create a virtual table that uses the operator
        session.execute("var r real relation { n int } init rel { tup { n 1 }, tup { n 2 }};");
        session.execute("var v virtual"
                + " r where test2('', n, 1.1, true, X'0909', tup { s '' }, rel { tup { n 7 } }, array('')) = '21.1true997';");
        assertEquals(Integer.valueOf(2), session.evaluate("(tuple from v).n"));

        session.execute("commit;");
    }

    @SuppressWarnings("unused")
    private static String testDException() throws DException {
        DSession session = DSession.createSession();
        try {
            throw new DException(session.evaluate("system_error('bar')"));
        } finally {
            session.close();
        }
    }

    @Test
    public void testReadonlyDExc() throws DException {
        session.execute("begin tx;");

        session.execute("operator test_dex()" + " returns string extern 'Java'"
                + " 'net.sf.duro.tests.TestUserDefOp.testDException';"
                + " end operator;");
        try {
            session.evaluate("test_dex()");
            fail("no exception");
        } catch (DException ex) {
            PossrepObject errobj = (PossrepObject) ex.getError();
            assertEquals("system_error", errobj.getTypeName());
            assertEquals("bar", errobj.getProperty("msg"));
        }
        session.execute("commit;");
    }

    // Implements the user-defined operator test4u
    @SuppressWarnings("unused")
    private static void test1u(StringBuilder au, String a, UpdatableInteger bu,
            Integer b, UpdatableBoolean cu, Boolean c, UpdatableDouble du,
            Double d, ByteArray eu, byte[] e, Tuple fu, Tuple f, Set<Tuple> gu,
            Set<Tuple> g, ArrayList<Tuple> hu, Tuple[] h) {
        au.append(a);
        bu.setValue(bu.intValue() + b.intValue());
        cu.setValue(c.booleanValue());
        du.setValue(du.doubleValue() + d.doubleValue());

        int oldlen = eu.getLength();
        eu.setLength(eu.getLength() + e.length);
        System.arraycopy(e, 0, eu.getBytes(), oldlen, e.length);

        fu.setAttribute("s",
                (String) fu.getAttribute("s") + (String) f.getAttribute("s"));

        gu.addAll(g);

        hu.addAll(0, Arrays.asList(h));
    }

    @Test
    public void testUpdateOp1() throws DException {
        session.execute("begin tx;");

        session.execute("operator test1u "
                + "(au string, a string, bu int, b int, cu bool, c bool,"
                + "du float, d float, eu binary, e binary,"
                + " fu tup { s string}, f tup { s string},"
                + " gu rel { x float }, g rel { x float },"
                + " hu array tup { a int, s string }, h array tup { a int, s string })"
                + " updates { au, bu, cu, du, eu, fu, gu, hu }"
                + " extern 'Java' 'net.sf.duro.tests.TestUserDefOp.test1u';"
                + " end operator;");
        session.execute("var au init 'Hello';" + "var bu init 5;"
                + "var cu init false;" + "var du init 2.0;"
                + "var eu init X'01ff';" + "var fu init tup { s 'Du' };"
                + "var gu init rel { tup { x 1.0 } };"
                + "var hu init array (tup {a 1, s 'yo'});"
                + "test1u(au, 'Goodbye', bu, 14, cu, true, du, 3.0,"
                + "eu, X'02ef', fu, tup { s 'ro' }, "
                + "gu, rel { tup { x 7.0 } },"
                + "hu, array (tup {a 2, s 'Joe'}));");
        assertEquals("HelloGoodbye", session.evaluate("au"));
        assertEquals(19, session.evaluate("bu"));
        assertEquals(true, session.evaluate("cu"));
        assertEquals(5.0, session.evaluate("du"));
        assertArrayEquals(new byte[] { (byte) 1, (byte) 0x0ff, (byte) 2,
                (byte) 0x0ef }, (byte[]) session.evaluate("eu"));
        Tuple t = new Tuple();
        t.setAttribute("s", "Duro");
        assertEquals(t, session.evaluate("fu"));

        Set<Tuple> relSet = new HashSet<Tuple>();
        t = new Tuple();
        t.setAttribute("x", 1.0);
        relSet.add(t);
        t = new Tuple();
        t.setAttribute("x", 7.0);
        relSet.add(t);
        assertEquals(relSet, session.evaluate("gu"));

        t = new Tuple();
        t.setAttribute("a", Integer.valueOf(2));
        t.setAttribute("s", "Joe");
        Tuple t2 = new Tuple();
        t2.setAttribute("a", Integer.valueOf(1));
        t2.setAttribute("s", "yo");
        assertArrayEquals(new Tuple[] { t, t2 },
                (Object[]) session.evaluate("hu"));

        session.execute("commit;");
    }

    // Implements the user-defined operator test4u
    @SuppressWarnings("unused")
    private static void testException(StringBuilder a) {
        throw new UnsupportedOperationException();
    }

    @Test
    public void testUpdateOpException() throws DException {
        session.execute("begin tx;");

        session.execute("operator testuex (a string) updates { a }"
                + " extern 'Java' 'net.sf.duro.tests.TestUserDefOp.testException';"
                + " end operator;");
        try {
            session.execute("var a init 'yxy';" + "testuex(a);");
            fail("operator call succeeded despite Java exception being thrown");
        } catch (DException ex) {
        }

        session.execute("commit;");
    }

    @SuppressWarnings("unused")
    private static void testUpdateDException(StringBuilder buf)
            throws DException {
        DSession session = DSession.createSession();
        try {
            throw new DException(session.evaluate("system_error('bar')"));
        } finally {
            session.close();
        }
    }

    @Test
    public void testUpdateDExc() throws DException {
        session.execute("begin tx;");

        session.execute("operator test_udex(s string) updates { s }"
                + " extern 'Java' 'net.sf.duro.tests.TestUserDefOp.testUpdateDException';"
                + " end operator;" + "var s string;");
        try {
            session.execute("test_udex(s);");
            fail("no exception");
        } catch (DException ex) {
            PossrepObject errobj = (PossrepObject) ex.getError();
            assertEquals("system_error", errobj.getTypeName());
            assertEquals("bar", errobj.getProperty("msg"));
        }
        session.execute("commit;");
    }

}
