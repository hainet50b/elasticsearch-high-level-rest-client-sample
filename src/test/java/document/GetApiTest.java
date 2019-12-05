package document;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetApiTest {

    private Logger log = LoggerFactory.getLogger(GetApiTest.class);

    @Test
    public void getTest() {
        final GetApi api = new GetApi();

        try {
            final GetResponse response = api.get(
                    "my_index",
                    "Change me!"
            );

            log.info("Index: {}", response.getIndex());
            log.info("Id: {}", response.getId());

            if (response.isExists()) {
                log.info("Version: {}", response.getVersion());
                log.info("Document: {}", response.getSourceAsMap());
            } else {
                log.info("The document doesn't exists.");
            }
        } catch (final ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.info("The index doesn't exists.");
            }
        }
    }
}
