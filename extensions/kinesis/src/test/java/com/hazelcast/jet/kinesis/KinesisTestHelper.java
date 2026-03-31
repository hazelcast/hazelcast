/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.kinesis.impl.source.RangeMonitor;
import com.hazelcast.jet.retry.IntervalFunction;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.ExpiredNextTokenException;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class KinesisTestHelper {

    static final RetryStrategy RETRY_STRATEGY = RetryStrategies.custom()
            .maxAttempts(30)
            .intervalFunction(IntervalFunction.exponentialBackoffWithCap(250L, 2.0, 2000L))
            .build();

    private final KinesisAsyncClient kinesis;
    private final String stream;

    private final ILogger logger = Logger.getLogger(KinesisIntegrationTest.class);

    public KinesisTestHelper(KinesisAsyncClient kinesis, String stream) {
        this.kinesis = kinesis;
        this.stream = stream;
    }

    public boolean streamExists() {
        Set<String> streams = new HashSet<>(callSafely(this::listStreams, "stream listing"));
        return streams.contains(stream);
    }

    public void waitForStreamToActivate() {
        int attempt = 0;
        while (true) {
            StreamStatus status = callSafely(this::getStreamStatus, "stream status");
            switch (status) {
                case ACTIVE:
                    return;
                case CREATING:
                case UPDATING:
                    wait(++attempt, "stream activation");
                    break;
                case DELETING:
                    throw new JetException("Stream is being deleted");
                default:
                    throw new JetException("Programming error, unhandled stream status: " + status);
            }
        }
    }

    public void waitForStreamToDisappear() {
        int attempt = 0;
        while (true) {
            List<String> streams = callSafely(this::listStreams, "stream disappearance");
            if (streams.contains(stream)) {
                wait(++attempt, "stream disappearance");
            } else {
                return;
            }
        }
    }

    public void createStream(int shardCount) {
        if (streamExists()) {
            throw new IllegalStateException("Stream already exists");
        }

        callSafely(() -> {
            CreateStreamRequest.Builder request = CreateStreamRequest.builder();
            request.shardCount(shardCount);
            request.streamName(stream);
            return kinesis.createStream(request.build()).get();
        }, "stream creation");

        waitForStreamToActivate();
    }

    public void deleteStream() {
        if (streamExists()) {
            callSafely(() -> kinesis.deleteStream(DeleteStreamRequest.builder().streamName(stream).build()).get(), "stream deletion");
            waitForStreamToDisappear();
        }
    }

    public List<Shard> listOpenShards(Predicate<? super Shard> filter) {
        return callSafely(this::listOpenShards, "open shard listing").stream()
                .filter(filter)
                .collect(Collectors.toList());
    }

    public PutRecordsResponse putRecords(List<Map.Entry<String, String>> messages) {
        PutRecordsRequest request = PutRecordsRequest.builder().streamName(stream).records(messages.stream()
                .map(entry -> {
                    PutRecordsRequestEntry.Builder putEntry = PutRecordsRequestEntry.builder();
                    putEntry.partitionKey(entry.getKey());
                    putEntry.data(SdkBytes.fromString(entry.getValue(), StandardCharsets.UTF_8));
                    return putEntry.build();
                })
                .collect(Collectors.toList())).build();
        return callSafely(() -> putRecords(request), "put records");
    }

    private PutRecordsResponse putRecords(PutRecordsRequest request) throws InterruptedException, ExecutionException {
        return kinesis.putRecords(request).get();
    }

    private List<String> listStreams() throws InterruptedException, ExecutionException {
        return kinesis.listStreams().get().streamNames();
    }

    private StreamStatus getStreamStatus() throws InterruptedException, ExecutionException {
        DescribeStreamSummaryRequest request = DescribeStreamSummaryRequest.builder().streamName(stream).build();

        StreamDescriptionSummary description = kinesis.describeStreamSummary(request).get().streamDescriptionSummary();

        return description.streamStatus();
    }

    private List<Shard> listOpenShards() throws InterruptedException, ExecutionException {
        List<Shard> shards = new ArrayList<>();
        String nextToken = null;
        do {
            ShardFilterType filterType = ShardFilterType.AT_LATEST; //only the currently open shards
            ListShardsRequest request = RangeMonitor.listAllShardsRequest(stream, nextToken, filterType);
            ListShardsResponse response = kinesis.listShards(request).get();
            shards.addAll(response.shards());
            nextToken = response.nextToken();
        } while (nextToken != null);
        return shards;
    }

    private <T> T callSafely(Callable<T> callable, String action) {
        int attempt = 0;
        while (true) {
            try {
                return callable.call();
            } catch (LimitExceededException lee) {
                String message = "The requested resource exceeds the maximum number allowed, or the number of " +
                        "concurrent stream requests exceeds the maximum number allowed. Will retry.";
                logger.warning(message, lee);
            } catch (ExpiredNextTokenException ente) {
                String message = "The pagination token passed to the operation is expired. Will retry.";
                logger.warning(message, ente);
            } catch (ResourceInUseException riue) {
                String message = "The resource is not available for this operation. For successful operation, the " +
                        "resource must be in the ACTIVE state. Will retry.";
                logger.warning(message, riue);
            } catch (ResourceNotFoundException rnfe) {
                String message = "The requested resource could not be found. The stream might not be specified correctly.";
                throw new JetException(message, rnfe);
            } catch (InvalidArgumentException iae) {
                String message = "A specified parameter exceeds its restrictions, is not supported, or can't be used.";
                throw new JetException(message, iae);
            } catch (SdkException se) {
                String message = "Amazon SDK failure, ignoring and retrying.";
                logger.warning(message, se);
            } catch (Exception e) {
                throw rethrow(e);
            }

            wait(++attempt, action);
        }
    }

    private void wait(int attempt, String action) {
        if (attempt > RETRY_STRATEGY.getMaxAttempts()) {
            throw new JetException(String.format("Abort waiting for %s, too many attempts", action));
        }

        logger.info(String.format("Waiting for %s ...", action));
        long duration = RETRY_STRATEGY.getIntervalFunction().waitAfterAttempt(attempt);
        try {
            TimeUnit.MILLISECONDS.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException(String.format("Waiting for %s interrupted", action));
        }
    }

}
