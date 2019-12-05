package document;

import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReindexApiTest {

    private Logger log = LoggerFactory.getLogger(ReindexApiTest.class);

    @Test
    public void reindexTest() {
        final ReindexApi api = new ReindexApi();

        final BulkByScrollResponse response = api.reindex(
                new String[]{"my_index"},
                "dest_index"
        );

        log.info("Tool: {}", response.getTook());
        log.info("Timed out: {}", response.isTimedOut());
        log.info("Total docs: {}", response.getTotal());
        log.info("Created docs: {}", response.getCreated());
        log.info("Updated docs: {}", response.getUpdated());
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
