package document;

import client.LocalhostClient;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MultiGetApi {

    public MultiGetResponse multiGet(List<Map<String, String>> documents) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final MultiGetRequest request = new MultiGetRequest();
            documents.forEach(document -> request.add(new MultiGetRequest.Item(
                    document.get("index"),
                    document.get("id")
            )));

            return client.mget(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
