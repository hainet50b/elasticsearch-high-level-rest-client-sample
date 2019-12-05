package document;

import client.LocalhostClient;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

public class ExistsApi {

    public boolean exists(final String index, final String id) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final GetRequest request = new GetRequest(index, id)
                    .fetchSourceContext(new FetchSourceContext(false))
                    .storedFields("_none_");

            return client.exists(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
