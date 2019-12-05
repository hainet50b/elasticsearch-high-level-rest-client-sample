package document;

import client.LocalhostClient;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class GetApi {

    public GetResponse get(final String index, final String id) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final GetRequest request = new GetRequest(index, id);

            return client.get(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
