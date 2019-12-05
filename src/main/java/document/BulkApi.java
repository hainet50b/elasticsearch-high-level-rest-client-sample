package document;

import client.LocalhostClient;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Map;

public class BulkApi {

    public BulkResponse bulk(
            final String index,
            final String id,
            final Map<String, Object> document) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final BulkRequest request = new BulkRequest()
                    .add(new IndexRequest(index).id(id).source(document))
                    .add(new UpdateRequest(index, id).doc(document))
                    .add(new DeleteRequest(index, id));

            return client.bulk(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
