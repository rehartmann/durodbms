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

    public static class Point {
        private int x;

        private int y;

        public Point() { };

        public Integer getX() {
            return x;
        }

        public void setX(Integer x) {
            this.x = x;
        }

        public Integer getY() {
            return y;
        }

        public void setY(Integer y) {
            this.y = y;
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
            session.execute("DROP TYPE point;");
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
            session.execute("type point possrep { x int, y int } init point(0,0);");

            ScalarType pointType = ScalarType.fromString("point", session);
            session.implementType(pointType, Point.class);

            session.execute("commit;");
        } catch (DException e) {
            // Print DException when there is still a session available
            System.err.println("" + e);
            session.execute("rollback;");
            throw e;
        }            

        session.execute("begin tx;");
        try {
            session.execute("var p init point(1, 2);");
            assertEquals(Integer.valueOf(1), (Integer) session.evaluate("p.x"));
            assertEquals(Integer.valueOf(2), (Integer) session.evaluate("p.y"));
            session.execute("p.x := 5;");
            assertEquals(Integer.valueOf(5), (Integer) session.evaluate("p.x"));
            session.execute("p.y := 66;");
            assertEquals(Integer.valueOf(66), (Integer) session.evaluate("p.y"));
            assertEquals(Integer.valueOf(5), (Integer) session.evaluate("p.x"));
            session.execute("commit;");
        } catch (DException e) {
            // Print DException when there is still a session available
            System.err.println("" + e);
            session.execute("rollback;");
            throw e;
        }        
    }
}
