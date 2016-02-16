package net.sf.duro.tests;

import static org.junit.Assert.*;

import java.io.File;

import net.sf.duro.DException;
import net.sf.duro.DSession;
import net.sf.duro.ScalarType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestTypeImpl {

    private DSession session;

    public static class Implementor {
        private int n;
        private double x;
        private boolean b;
        private String s;

        public int getN() {
            return n;
        }
        public void setN(int n) {
            this.n = n;
        }
        public double getX() {
            return x;
        }
        public void setX(double x) {
            this.x = x;
        }
        public boolean getB() {
            return b;
        }
        public void setB(boolean b) {
            this.b = b;
        }
        public String getS() {
            return s;
        }
        public void setS(String s) {
            this.s = s;
        }
    }
    
    @Before
    public void setUp() throws Exception {
        session = DSession.createSession();
        session.execute("create_env('dbenv');"
                + "create_db('D');");
    }

    @After
    public void tearDown() throws Exception {
        try {
            session.execute("DROP TYPE test;");
        } catch (Exception e) {
            // swallow
        }
        session.close();
        File envdir = new File("dbenv");
        for (File f : envdir.listFiles()) {
            f.delete();
        }
        envdir.delete();
    }

    @Test
    public void testImplementType() {
        session.execute("connect('dbenv');"
                + "current_db := 'D';");
        session.execute("begin tx;");

        try {
            session.execute("TYPE test POSSREP { n int, x float, b boolean, s string } "
                    + " INIT test(0, 0.0, FALSE, '');");

            ScalarType testType = ScalarType.fromString("test", session);
            session.implementType(testType, Implementor.class);

            session.execute("commit;");
        } catch (DException e) {
            // Print DException when there is still a session available
            System.err.println("" + e);
            session.execute("rollback;");
            throw e;
        }

        session.execute("begin tx;");
        try {
            session.execute("var p0 test;");
            assertEquals(Integer.valueOf(0), (Integer) session.evaluate("p0.n"));
            assertEquals(Double.valueOf(0.0), (Double) session.evaluate("p0.x"));
            assertEquals(Boolean.valueOf(false), (Boolean) session.evaluate("p0.b"));
            assertEquals("", (String) session.evaluate("p0.s"));

            session.execute("var p init test(1, 2.0, true, 'yo');");
            assertEquals(Integer.valueOf(1), (Integer) session.evaluate("p.n"));
            assertEquals(Double.valueOf(2.0), (Double) session.evaluate("p.x"));
            assertEquals(Boolean.valueOf(true), (Boolean) session.evaluate("p.b"));
            assertEquals("yo", (String) session.evaluate("p.s"));

            session.execute("p.n := 5;");
            assertEquals(Integer.valueOf(5), (Integer) session.evaluate("p.n"));
            assertEquals(Double.valueOf(2.0), (Double) session.evaluate("p.x"));

            session.execute("p.x := 66.0;");
            session.execute("p.b := false;");
            session.execute("p.s := 'durodbms';");
            assertEquals(Double.valueOf(66.0), (Double) session.evaluate("p.x"));
            assertEquals(Boolean.valueOf(false), (Boolean) session.evaluate("p.b"));
            assertEquals("durodbms", (String) session.evaluate("p.s"));

            session.execute("commit;");
        } catch (DException e) {
            // Print DException when there is still a session available
            System.err.println("" + e);
            session.execute("rollback;");
            throw e;
        }
    }
}
