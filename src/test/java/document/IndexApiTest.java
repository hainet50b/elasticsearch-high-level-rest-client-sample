package document;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class IndexApiTest {

    private Logger log = LoggerFactory.getLogger(IndexApiTest.class);

    @Test
    public void indexSyncTest() {
        final IndexApi api = new IndexApi();

        final IndexResponse response = api.indexSync(
                "my_index",
                Map.of(
                        "timestamp", Instant.now(),
                        "uuid", UUID.randomUUID().toString()
                )
        );

        log.info("Index: {}", response.getIndex());
        log.info("Id: {}", response.getId());

        final DocWriteResponse.Result result = response.getResult();
        if (result == DocWriteResponse.Result.CREATED) {
            log.info("The document was created.");
        } else if (result == DocWriteResponse.Result.UPDATED) {
            log.info("The document was updated.");
        }
    }

    @Test
    public void indexAsyncTest() throws InterruptedException {
        final IndexApi api = new IndexApi();

        api.indexAsync(
                "my_index",
                Map.of(
                        "timestamp", Instant.now(),
                        "uuid", UUID.randomUUID().toString()
                )
        );

        log.info("Check your index.");
    }
}
