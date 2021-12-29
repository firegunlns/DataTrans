import org.junit.Test;

public class Test1 {
    @Test
    public void test1(){
        if (System.console() != null){
            String line = System.console().readLine();
            System.out.println(line);
        }
    }
}
