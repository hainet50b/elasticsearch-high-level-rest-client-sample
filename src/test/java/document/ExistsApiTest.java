package document;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExistsApiTest {

    private Logger log = LoggerFactory.getLogger(ExistsApiTest.class);

    @Test
    public void existsTest() {
        final ExistsApi api = new ExistsApi();

        final boolean exists = api.exists(
                "my_index",
                "Change me!"
        );

        if (exists) {
            log.info("The document exists.");
        } else {
            log.info("The document doesn't exists.");
        }
    }
}
