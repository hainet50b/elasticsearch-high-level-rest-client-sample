package document;

import client.LocalhostClient;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Map;

public class UpdateApi {

    public UpdateResponse update(
            final String index,
            final String id,
            final Map<String, Object> document) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final UpdateRequest request = new UpdateRequest(index, id)
                    .doc(document);

            return client.update(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public UpdateResponse upsert(
            final String index,
            final String id,
            final Map<String, Object> document) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final UpdateRequest request = new UpdateRequest(index, id)
                    .doc(document)
                    .upsert(document);

            return client.update(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
