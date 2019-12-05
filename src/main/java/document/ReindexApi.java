package document;

import client.LocalhostClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;

import java.io.IOException;

public class ReindexApi {

    public BulkByScrollResponse reindex(final String[] sourceIndices, final String destIndex) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final ReindexRequest request = new ReindexRequest()
                    .setSourceIndices(sourceIndices)
                    .setDestIndex(destIndex);

            return client.reindex(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
