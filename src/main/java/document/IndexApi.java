package document;

import client.LocalhostClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IndexApi {

    public IndexResponse indexSync(
            final String index,
            final Map<String, Object> document) {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final IndexRequest request = new IndexRequest(index)
                    .source(document);

            return client.index(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void indexAsync(
            final String index,
            final Map<String, Object> document) throws InterruptedException {
        try (final RestHighLevelClient client = LocalhostClient.create()) {
            final IndexRequest request = new IndexRequest(index)
                    .source(document);

            client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<>() {
                        @Override
                        public void onResponse(final IndexResponse indexResponse) {
                            // Let's do something!
                        }

                        @Override
                        public void onFailure(final Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
            );

            // JUnitで起動すると非同期処理の完了前にスレッドが終了してしまうためスリープを付与している。
            // 非同期処理を利用する場合はスレッドのライフサイクルに気をつけること。
            TimeUnit.SECONDS.sleep(1);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
