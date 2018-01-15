package com.hainet.elasticsearch.rest.high.level.client.sample;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.io.IOException;

public class SearchApiTest {

    @Test
    public void searchApiSyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        )) {
            final SearchSourceBuilder builder = new SearchSourceBuilder()
                    .query(QueryBuilders.matchAllQuery())
                    .from(0)
                    .size(5)
                    .timeout(TimeValue.timeValueMinutes(2));

            final SearchRequest request = new SearchRequest()
                    .source(builder);

            final SearchResponse response = client.search(request);

            System.out.println("=== HTTP Request ===");
            System.out.println("status: " + response.status());
            System.out.println("took: " + response.getTook());
            System.out.println("timed_out: " + response.isTimedOut());

            System.out.println("\n=== Hits ===");
            final SearchHits hits = response.getHits();
            System.out.println("total_hits: " + hits.getTotalHits());

            for (SearchHit hit : hits.getHits()) {
                System.out.println("\n=== Documents ===");
                System.out.println("index: " + hit.getIndex());
                System.out.println("type: " + hit.getType());
                System.out.println("id: " + hit.getId());
                System.out.println("source: " + hit.getSourceAsString());
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void searchApiAsyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        )) {
            final SearchSourceBuilder builder = new SearchSourceBuilder()
                    .query(QueryBuilders.matchAllQuery());

            final SearchRequest request = new SearchRequest()
                    .source(builder);

            client.searchAsync(request, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    // do something
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
