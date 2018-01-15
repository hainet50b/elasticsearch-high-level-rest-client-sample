package com.hainet.elasticsearch.rest.high.level.client.sample;

import org.apache.http.HttpHost;
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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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
                    .indices("index")
                    .types("logs")
                    .source(builder);

            final SearchResponse response = client.search(request);

            System.out.println(response.status());
            System.out.println(response.getTook());
            System.out.println(response.isTimedOut());

            final SearchHits hits = response.getHits();
            System.out.println(hits.getTotalHits());
            System.out.println(hits.getMaxScore());

            for (SearchHit hit : hits.getHits()) {
                System.out.println(hit.getIndex());
                System.out.println(hit.getType());
                System.out.println(hit.getId());
                System.out.println(hit.getScore());

                System.out.println(hit.getSourceAsString());
                System.out.println(hit.getSourceAsMap().get("key"));
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
