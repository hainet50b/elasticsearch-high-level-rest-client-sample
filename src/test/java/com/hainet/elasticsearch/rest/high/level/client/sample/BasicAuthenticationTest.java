package com.hainet.elasticsearch.rest.high.level.client.sample;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;

public class BasicAuthenticationTest {

    @Test
    public void basicAuthenticationTest() {
        try (final RestHighLevelClient client = new RestHighLevelClient(RestClient
                .builder(new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    final BasicCredentialsProvider provider = new BasicCredentialsProvider();
                    provider.setCredentials(
                            AuthScope.ANY,
                            new UsernamePasswordCredentials(
                                    "username",
                                    "password"
                            )
                    );

                    return httpClientBuilder.setDefaultCredentialsProvider(provider);
                })
        )) {
            // Do something.
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
