package document;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MultiGetApiTest {

    private Logger log = LoggerFactory.getLogger(MultiGetApiTest.class);

    @Test
    public void multiGetTest() {
        final MultiGetApi api = new MultiGetApi();

        final MultiGetResponse responses = api.multiGet(List.of(
                Map.of("index", "my_index", "id", "Change me!"),
                Map.of("index", "my_index", "id", "Change me!")
        ));

        responses.forEach(response -> {
            final GetResponse getResponse = response.getResponse();

            if (getResponse != null) {
                log.info("Index: {}", getResponse.getIndex());
                log.info("Id: {}", getResponse.getId());

                if (getResponse.isExists()) {
                    log.info("Version: {}", getResponse.getVersion());
                    log.info("Document: {}", getResponse.getSourceAsMap());
                } else {
                    log.info("The document doesn't exists.");
                }
            } else {
                log.info("The index doesn't exists.");
            }
        });
    }
}
