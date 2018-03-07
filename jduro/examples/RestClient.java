import java.net.MalformedURLException;
import java.net.URL;

import net.sf.duro.DSession;


public class RestClient {

    /**
     * @param args
     * @throws MalformedURLException 
     */
    public static void main(String[] args) {
        try {
            DSession session = DSession.createSession(new URL("http", "localhost", 8888, "/D"));
            Object result = session.evaluate("t");
            if (result.getClass().isArray()) {
                Object[] array = (Object[]) result;

                for (int i = 0; i < array.length; i++) {
                    System.out.println(array[i].toString());
                }
            } else {
                System.out.println(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
