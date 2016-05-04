package net.sf.duro.tests;

import static org.junit.Assert.*;

import java.io.File;

import net.sf.duro.DException;
import net.sf.duro.DSession;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUserClass {

    private DSession session;

    public static class DataImpl {
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

    static interface Data {
        int getN();
        void setN(int n);
        double getX();
        void setX(double x);
        boolean getB();
        void setB(boolean b);
        String getS();
        void setS(String s);
    };

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
    public void testEvaluateToClass() throws Exception {
        session.execute("connect('dbenv');"
                + "current_db := 'D';");
        session.execute("begin tx;");

        try {
            session.execute("TYPE test POSSREP { n int, x float, b boolean, s string } "
                    + " INIT test(0, 0.0, FALSE, '');"
                    + "IMPLEMENT TYPE test;"
                    + "END IMPLEMENT;");

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
            DataImpl dest = new DataImpl();
            session.evaluate("p0", dest);
            assertEquals(0, dest.getN());
            assertEquals(0.0, dest.getX(), 0.0);
            assertEquals(false, dest.getB());
            assertEquals("", dest.getS());

            session.execute("var p init test(1, 2.0, true, 'yo');");
            session.evaluate("p", dest);
            assertEquals(1, dest.getN());
            assertEquals(2.0, dest.getX(), 0.0);
            assertEquals(true, dest.getB());
            assertEquals("yo", dest.getS());

            session.execute("p.n := 5;");
            session.evaluate("p", dest);
            assertEquals(5, dest.getN());
            assertEquals("yo", dest.getS());

            session.execute("p.x := 66.0;");
            session.execute("p.b := false;");
            session.execute("p.s := 'durodbms';");
            session.evaluate("p", dest);
            assertEquals(66.0, dest.getX(), 0.0);
            assertEquals(false, dest.getB());
            assertEquals("durodbms", dest.getS());

            dest = session.evaluate("p", DataImpl.class);
            assertEquals(66.0, dest.getX(), 0.0);
            assertEquals(false, dest.getB());
            assertEquals("durodbms", dest.getS());

            Data desti = session.evaluate("p", Data.class);
            assertEquals(66.0, desti.getX(), 0.0);
            assertEquals(false, desti.getB());
            assertEquals("durodbms", desti.getS());

            session.execute("commit;");
        } catch (DException e) {
            // Print DException when there is still a session available
            System.err.println("" + e);
            session.execute("rollback;");
            throw e;
        }
    }
}
