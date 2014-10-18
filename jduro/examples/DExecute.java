import java.io.BufferedReader;
import java.io.InputStreamReader;

import net.sf.duro.DSession;
import net.sf.duro.DSession;

/**
 * Command line interface for JDuro.
 * Input in parentheses is passed to DSession.evaluate(),
 * other input is passed to DSession.execute().
 * 
 * @author rene
 *
 */
public class DExecute {

    /**
     * The main function. Command-line arguments are ignored.
     */
    public static void main(String[] args) {
	DSession dInstance = null;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            dInstance = DSession.createSession();

            System.out.println("Input in Parentheses will be evaluated, other input is executed");
            
            for (;;) {
                System.out.print("> ");
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                try {
                    if (line.startsWith("(")) {
                	System.out.println(dInstance.evaluate(line));
                    } else {
                        dInstance.execute(line);
                    }
                } catch (Exception ex) {
                    System.out.println("Error: " + ex);
                    ex.printStackTrace();
                }
            }
        } catch (Exception ex) {
            System.out.println("Error: " + ex);
        }
        if (dInstance != null) {
            try {
        	dInstance.close();
            } catch (Exception ex) {
                System.out.println("Error destroying DInstance: " + ex);
            }
        }            
    }

}
