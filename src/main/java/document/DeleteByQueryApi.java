package document;

import client.LocalhostClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import java.io.IOException;
import java.util.Map;

public class DeleteByQueryApi {

    public BulkByScrollResponse deleteByQuery(
            final String[] indices,
            final Map<String, String> query) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final DeleteByQueryRequest request = new DeleteByQueryRequest(indices)
                    .setQuery(new TermQueryBuilder(query.get("field"), query.get("value")));

            return client.deleteByQuery(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
