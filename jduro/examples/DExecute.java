import java.io.BufferedReader;
import java.io.InputStreamReader;

import net.sf.duro.DSession;
import net.sf.duro.DuroDSession;

public class DExecute {

    /**
     * @param args
     */
    public static void main(String[] args) {
	DSession dInstance = null;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            dInstance = DuroDSession.createSession();

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
