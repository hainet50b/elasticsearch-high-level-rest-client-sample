package document;

import client.LocalhostClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

import java.io.IOException;
import java.util.Map;

public class UpdateByQueryApi {

    public BulkByScrollResponse updateByQuery(
            final String[] indices,
            final Map<String, String> query) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final UpdateByQueryRequest request = new UpdateByQueryRequest(indices)
                    .setQuery(new TermQueryBuilder(query.get("field"), query.get("value")));

            return client.updateByQuery(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
