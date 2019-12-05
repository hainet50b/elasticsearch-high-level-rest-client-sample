package document;

import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DeleteByQueryApiTest {

    private Logger log = LoggerFactory.getLogger(DeleteByQueryApiTest.class);

    @Test
    public void deleteByQueryTest() {
        final DeleteByQueryApi api = new DeleteByQueryApi();

        final BulkByScrollResponse response = api.deleteByQuery(
                new String[]{"my_index"},
                Map.of("field", "_id", "value", "Change me!")
        );

        log.info("Tool: {}", response.getTook());
        log.info("Timed out: {}", response.isTimedOut());
        log.info("Total docs: {}", response.getTotal());
        log.info("Deleted docs: {}", response.getDeleted());
        log.info("Batches: {}", response.getBatches());
        log.info("Noops: {}", response.getNoops());
        log.info("Version conflicts: {}", response.getVersionConflicts());
        log.info("Bulk retries: {}", response.getBulkRetries());
        log.info("Search retries: {}", response.getSearchRetries());
        log.info("Throttled millis: {}", response.getStatus().getThrottled());
        log.info("Throttled until millis: {}", response.getStatus().getThrottledUntil());
        log.info("Bulk failures: {}", response.getBulkFailures());
        log.info("Search failures: {}", response.getSearchFailures());
    }
}
