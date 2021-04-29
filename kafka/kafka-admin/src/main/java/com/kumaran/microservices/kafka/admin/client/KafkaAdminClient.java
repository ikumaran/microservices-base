package com.kumaran.microservices.kafka.admin.client;

import com.kumaran.microservices.kafka.admin.exception.KafkaClientException;
import com.kumaran.microservices.twitter.to.kafka.config.KafkaConfigData;
import com.kumaran.microservices.twitter.to.kafka.config.RetryConfigData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@Component
public class KafkaAdminClient {

    private final KafkaConfigData kafkaConfigData;

    private final RetryConfigData retryConfigData;

    private final AdminClient adminClient;

    private final RetryTemplate retryTemplate;

    private final WebClient webClient;

    public KafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData, AdminClient adminClient, RetryTemplate retryTemplate, WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    public void createTopic() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doRetryCallback);
        } catch (Throwable e) {
            throw new KafkaClientException("Reached max retries for creating kafka topic", e);
        }
        checkTopicsCreated();
    }

    private CreateTopicsResult doRetryCallback(RetryContext retryContext) {
        List<String> topicNamesToCreate = kafkaConfigData.getTopicNamesToCreate();
        log.info("Creating {} topics with retry of {}", topicNamesToCreate.size(), retryContext.getRetryCount());
        List<NewTopic> newTopics = topicNamesToCreate.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).collect(Collectors.toList());
        return adminClient.createTopics(newTopics);
    }

    public void checkTopicsCreated() {
        Collection<TopicListing> topicListings = getTopics();
        int retryCount = 1;
        int maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        long sleepTime = retryConfigData.getSleepTimeMs();
        for (String topicName : kafkaConfigData.getTopicNamesToCreate()) {
            while (!isTopicCreated(topicListings, topicName)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTime);
                sleepTime *= multiplier;
                topicListings = getTopics();
            }
        }
    }

    public void checkSchemaRegistry() {
        int retryCount = 1;
        int maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        long sleepTime = retryConfigData.getSleepTimeMs();
        while (!isSchemaRegistryAvailable().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTime);
            sleepTime *= multiplier;
        }
    }

    private HttpStatus isSchemaRegistryAvailable() {
        try {
            return webClient.method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            throw new KafkaClientException("Thread sleep interrupted");
        }
    }

    private void checkMaxRetry(int retry, int maxRetry) {
        if (retry > maxRetry) {
            throw new KafkaClientException("Max retry completed");
        }
    }

    private boolean isTopicCreated(Collection<TopicListing> topicListings, String topicName) {
        if (topicListings == null) {
            return false;
        }
        return topicListings.stream().anyMatch(topicListing -> topicListing.name().equals(topicName));
    }

    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topicListings;
        try {
            topicListings = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable e) {
            throw new KafkaClientException("Unable to fetch topics", e);
        }
        return topicListings;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        log.info("Creating {} topics with retry of {}", kafkaConfigData.getTopicNamesToCreate().size(), retryContext.getRetryCount());
        Collection<TopicListing> topicListings = adminClient.listTopics().listings().get();
        topicListings.forEach(topicListing -> {
            log.info("Fetched Topic name {}", topicListing.name());
        });
        return topicListings;
    }
}
