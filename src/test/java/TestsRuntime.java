import org.junit.jupiter.api.Test;

import java.io.IOException;
public class TestsRuntime {

    @Test
    public void test() throws IOException {
        Runtime.getRuntime().exec(new String[]{"cmd"}).getOutputStream();
    }
}
