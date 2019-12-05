package document;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class BulkApiTest {

    private Logger log = LoggerFactory.getLogger(BulkApiTest.class);

    @Test
    public void bulkTest() {
        final BulkApi api = new BulkApi();

        api.bulk(
                "my_index",
                UUID.randomUUID().toString(),
                Map.of(
                        "timestamp", Instant.now(),
                        "uuid", UUID.randomUUID().toString()
                )
        ).forEach(bulkItemResponse -> {
            final DocWriteResponse response = bulkItemResponse.getResponse();

            switch (bulkItemResponse.getOpType()) {
                case INDEX:
                case CREATE:
                    log.info("The document was created.");
                    final IndexResponse indexResponse = (IndexResponse) response;
                    break;
                case UPDATE:
                    log.info("The document was updated.");
                    final UpdateResponse updateResponse = (UpdateResponse) response;
                    break;
                case DELETE:
                    log.info("The document was deleted.");
                    final DeleteResponse deleteResponse = (DeleteResponse) response;
                    break;
            }
        });
    }
}
