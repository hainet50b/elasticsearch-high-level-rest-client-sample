package document;

import client.LocalhostClient;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class DeleteApi {

    public DeleteResponse delete(final String index, final String id) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final DeleteRequest request = new DeleteRequest(index, id);

            return client.delete(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
