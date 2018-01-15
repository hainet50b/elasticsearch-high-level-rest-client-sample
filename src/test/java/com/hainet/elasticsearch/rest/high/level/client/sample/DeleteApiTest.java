package com.hainet.elasticsearch.rest.high.level.client.sample;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DeleteApiTest {

    @Test
    public void deleteApiSyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        )) {
            final DeleteRequest request = new DeleteRequest()
                    .index("index")
                    .type("logs")
                    .id("id")
                    .timeout(TimeValue.timeValueMinutes(2));

            final DeleteResponse response = client.delete(request);

            assertThat(response.getIndex(), is("index"));
            assertThat(response.getType(), is("logs"));
            assertThat(response.getId(), is("id"));

            System.out.println(response.getResult());
            System.out.println(response.status());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void deleteApiAsyncTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        )) {
            final DeleteRequest request = new DeleteRequest()
                    .index("index")
                    .type("logs")
                    .id("id")
                    .timeout(TimeValue.timeValueMinutes(2));

            final CompletableFuture<String> future = new CompletableFuture<>();
            client.deleteAsync(request, new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse response) {
                    assertThat(response.getIndex(), is("index"));
                    assertThat(response.getType(), is("logs"));
                    assertThat(response.getId(), is("id"));

                    System.out.println(response.getResult());
                    System.out.println(response.status());

                    future.complete("ok");
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException(e);
                }
            });

            assertThat(future.get(), is("ok"));
        } catch (final IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
