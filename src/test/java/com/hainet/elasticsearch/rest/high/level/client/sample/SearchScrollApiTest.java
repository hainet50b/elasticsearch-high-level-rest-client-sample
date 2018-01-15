package com.hainet.elasticsearch.rest.high.level.client.sample;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.io.IOException;

public class SearchScrollApiTest {

    @Test
    public void searchScrollApiTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        )) {
            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(2));
            final SearchSourceBuilder builder = new SearchSourceBuilder()
                    .query(QueryBuilders.matchAllQuery())
                    .size(10);

            final SearchRequest request = new SearchRequest()
                    .scroll(scroll)
                    .source(builder);

            SearchResponse response = client.search(request);
            String scrollId = response.getScrollId();
            SearchHit[] hits = response.getHits().getHits();

            while (hits != null && hits.length > 0) {
                final SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId)
                        .scroll(scroll);

                response = client.searchScroll(scrollRequest);

                scrollId = response.getScrollId();
                hits = response.getHits().getHits();

                for (final SearchHit hit : hits) {
                    System.out.println(hit.getSourceAsString());
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
