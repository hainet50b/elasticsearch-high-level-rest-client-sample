package com.hainet.elasticsearch.rest.high.level.client.sample;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class IndexApiTest {

    @Test
    public void indexApiSyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        )) {
            // Map
            final Map<String, Object> map = new HashMap<>();
            map.put("user", "hainet");
            map.put("message", "elasticsearch-rest-high-level-client-sample");

            // XContentBuilder
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user", "hainet");
                builder.field("message", "elasticsearch-rest-high-level-client-sample");
            }
            builder.endObject();

            final IndexRequest request = new IndexRequest()
                    .index("index")
                    .type("logs")
                    .id("id")
                    .timeout(TimeValue.timeValueMinutes(2))
                    // Map
                    .source(map);
                    // XContentBUilder
                    // .source(builder);
                    // Object key-pairs
                    // .source("user", "hainet",
                    //         "message", "elasticsearch-rest-high-level-client-sample")

            final IndexResponse response = client.index(request);

            assertThat(response.getIndex(), is("index"));
            assertThat(response.getType(), is("logs"));
            assertThat(response.getId(), is("id"));

            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                assertThat(response.getVersion(), is(1L));
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                assertThat(response.getVersion(), is(greaterThan(1L)));
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void indexApiAsyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        )) {
            // Map
            final Map<String, Object> map = new HashMap<>();
            map.put("user", "hainet");
            map.put("message", "elasticsearch-rest-high-level-client-sample");

            final IndexRequest request = new IndexRequest()
                    .index("index")
                    .type("logs")
                    .id("id")
                    .timeout(TimeValue.timeValueMinutes(2))
                    .source(map);

            client.indexAsync(request, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(final IndexResponse response) {
                    assertThat(response.getIndex(), is("index"));
                    assertThat(response.getType(), is("logs"));
                    assertThat(response.getId(), is("id"));

                    if (response.getResult() == DocWriteResponse.Result.CREATED) {
                        assertThat(response.getVersion(), is(1L));
                    } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                        assertThat(response.getVersion(), is(greaterThan(1L)));
                    }
                }

                @Override
                public void onFailure(final Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
