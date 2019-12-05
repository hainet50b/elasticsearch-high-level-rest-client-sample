package document;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteApiTest {

    private Logger log = LoggerFactory.getLogger(DeleteApiTest.class);

    @Test
    public void deleteTest() {
        final DeleteApi api = new DeleteApi();

        final DeleteResponse response = api.delete(
                "my_index",
                "Change me!"
        );

        log.info("Index: {}", response.getIndex());
        log.info("Id: {}", response.getId());
        log.info("Version: {}", response.getVersion());

        if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            log.info("The document doesn't exists.");
        }
    }
}
