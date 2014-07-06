import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import net.sf.duro.DSession;
import net.sf.duro.DuroDSession;
import net.sf.duro.PossrepObject;

public class UserDefType {

    /**
     * @param args
     */
    public static void main(String[] args) {
	DSession session = null;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            session = DuroDSession.createSession();

            session.execute("create_env('dbenv');"
        	    + "create_db('D');"
        	    + "current_db := 'D';");
            session.execute("begin tx;");
            session.execute("type len possrep { l int } constraint l >= 0 init len(0);");
            session.execute("implement type len; end implement;");
            session.execute("var l len;"
        	    + "l := len(5);");
            PossrepObject l = (PossrepObject) session.evaluate("l");

            System.out.println(session.evaluate("l").equals(session.evaluate("l")));

            session.execute("commit;");

        } catch (Exception ex) {
            System.out.println("Error: " + ex);
        }
        if (session != null) {
            try {
        	session.close();
            } catch (Exception ex) {
                System.out.println("Error destroying DInstance: " + ex);
            }
        }            
    }

}
