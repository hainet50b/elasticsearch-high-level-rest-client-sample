package document;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class UpdateApiTest {

    private Logger log = LoggerFactory.getLogger(UpdateApiTest.class);

    @Test
    public void updateTest() {
        final UpdateApi api = new UpdateApi();

        try {
            final UpdateResponse response = api.update(
                    "my_index",
                    "Change me!",
                    Map.of("uuid", UUID.randomUUID().toString())
            );

            log.info("Index: {}", response.getIndex());
            log.info("Id: {}", response.getId());
            log.info("Version: {}", response.getVersion());

            if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                log.info("The document was updated.");
            } else if (response.getResult() == DocWriteResponse.Result.NOOP) {
                log.info("The document was not impacted.");
            }
        } catch (final ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.info("The index or document doesn't exists.");
            }
        }
    }

    @Test
    public void upsertTest() {
        final UpdateApi api = new UpdateApi();

        final UpdateResponse response = api.upsert(
                "my_index",
                UUID.randomUUID().toString(),
                Map.of(
                        "timestamp", Instant.now(),
                        "uuid", UUID.randomUUID().toString()
                )
        );

        log.info("Index: {}", response.getIndex());
        log.info("Id: {}", response.getId());
        log.info("Version: {}", response.getVersion());

        if (response.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("The document was created.");
        }
    }
}
