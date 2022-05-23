import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class Tets {

    @Test
    public void test() throws IOException {
        try (OutputStream outputStream = Runtime.getRuntime().exec("cmd").getOutputStream("echo rg".getBytes(StandardCharsets.UTF_8))) {
        }
    }

}
