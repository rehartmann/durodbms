package net.sf.duro;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class DEvaluator {

    /**
     * @param args
     */
    public static void main(String[] args) {
	DSession dInstance = null;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            dInstance = DuroDSession.createSession();

            for (;;) {
                System.out.print("> ");
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                try {
                    System.out.println(dInstance.evaluate(line));
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
