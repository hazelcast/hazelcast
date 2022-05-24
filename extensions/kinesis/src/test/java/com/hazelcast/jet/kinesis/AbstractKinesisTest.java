/*
 * Copyright 2021 Hazelcast Inc.
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

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.SplitShardRequest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.kinesis.impl.source.HashRange;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.MapUtil.entry;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class AbstractKinesisTest extends JetTestSupport {

    protected static final int KEYS = 250;
    protected static final int MEMBER_COUNT = 2;
    protected static final int MESSAGES = 25_000;
    protected static final String STREAM = "TestStream";
    protected static final String RESULTS = "Results";

    protected IMap<String, List<String>> results;

    private final AwsConfig awsConfig;
    private final AmazonKinesisAsync kinesis;
    private final KinesisTestHelper helper;

    private HazelcastInstance[] cluster;

    AbstractKinesisTest(AwsConfig awsConfig, AmazonKinesisAsync kinesis, KinesisTestHelper helper) {
        this.awsConfig = awsConfig;
        this.kinesis = kinesis;
        this.helper = helper;
    }

    @Before
    public void before() {
        helper.deleteStream();

        cluster = createHazelcastInstances(MEMBER_COUNT);
        results = hz().getMap(RESULTS);
    }

    @After
    public void after() {
        if (cluster != null) {
            cleanUpCluster(cluster);
        }

        helper.deleteStream();

        if (results != null) {
            results.clear();
            results.destroy();
        }
    }

    protected HazelcastInstance hz() {
        return cluster[0];
    }

    protected Pipeline getPipeline(StreamSource<Map.Entry<String, byte[]>> source) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withNativeTimestamps(0)
                .rebalance(Map.Entry::getKey)
                .map(e -> entry(e.getKey(), Collections.singletonList(new String(e.getValue()))))
                .writeTo(Sinks.mapWithMerging(results, Map.Entry::getKey, Map.Entry::getValue, (l1, l2) -> {
                    ArrayList<String> list = new ArrayList<>();
                    list.addAll(l1);
                    list.addAll(l2);
                    return list;
                }));
        return pipeline;
    }

    protected Map<String, List<String>> expectedMessages(int fromInclusive, int toExclusive) {
        return toMap(messages(fromInclusive, toExclusive));
    }

    protected Map<String, List<String>> sendMessages() {
        return sendMessages(MESSAGES);
    }

    protected Map<String, List<String>> sendMessages(int count) {
        List<Map.Entry<String, String>> msgEntryList = messages(0, count);

        BatchSource<Map.Entry<String, byte[]>> source = TestSources.items(msgEntryList.stream()
                .map(e1 -> entry(e1.getKey(), e1.getValue().getBytes()))
                .collect(toList()));
        Sink<Map.Entry<String, byte[]>> sink = kinesisSink();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .writeTo(sink);

        hz().getJet().newJob(pipeline);

        return toMap(msgEntryList);
    }

    @Nonnull
    protected List<Map.Entry<String, String>> messages(int fromInclusive, int toExclusive) {
        return IntStream.range(fromInclusive, toExclusive)
                .boxed()
                .map(i -> entry(Integer.toString(i % KEYS), i))
                .map(e -> entry(e.getKey(), String.format("%s: msg %09d", e.getKey(), e.getValue())))
                .collect(toList());
    }

    protected void assertMessages(Map<String, List<String>> expected, boolean checkOrder, boolean deduplicate) {
        assertTrueEventually(() -> {
            assertEquals("Key sets differ!",
                    expected.keySet().stream().map(Integer::parseInt).collect(Collectors.toCollection(TreeSet::new)),
                    results.keySet().stream().map(Integer::parseInt).collect(Collectors.toCollection(TreeSet::new)));

            for (Map.Entry<String, List<String>> entry : expected.entrySet()) {
                String key = entry.getKey();
                List<String> expectedMessages = entry.getValue();

                List<String> actualMessages = results.get(key);
                if (deduplicate) {
                    actualMessages = actualMessages.stream().distinct().collect(toList());
                }
                if (!checkOrder) {
                    actualMessages = new ArrayList<>(actualMessages);
                    actualMessages.sort(String::compareTo);
                }
                assertEquals(getMessagesDifferDescription(key, expectedMessages, actualMessages),
                        expectedMessages, actualMessages);
            }
        });
    }

    protected KinesisSources.Builder<Map.Entry<String, byte[]>> kinesisSource() {
        return KinesisSources.kinesis(STREAM)
                .withEndpoint(awsConfig.getEndpoint())
                .withRegion(awsConfig.getRegion())
                .withCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey());
    }

    protected Sink<Map.Entry<String, byte[]>> kinesisSink() {
        return KinesisSinks.kinesis(STREAM)
                .withEndpoint(awsConfig.getEndpoint())
                .withRegion(awsConfig.getRegion())
                .withCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey())
                .withRetryStrategy(RetryStrategies.indefinitely(250))
                .build();
    }

    protected List<Shard> listOpenShards() {
        return helper.listOpenShards(AbstractKinesisTest::shardActive);
    }

    protected void mergeShards(Shard shard1, Shard shard2) {
        MergeShardsRequest request = new MergeShardsRequest();
        request.setStreamName(STREAM);
        request.setShardToMerge(shard1.getShardId());
        request.setAdjacentShardToMerge(shard2.getShardId());

        System.out.println("Merging " + shard1.getShardId() + " with " + shard2.getShardId());
        kinesis.mergeShards(request);
    }

    protected void splitShard(Shard shard) {
        HashRange range = HashRange.range(shard.getHashKeyRange());
        BigInteger middle = range.getMinInclusive().add(range.getMaxExclusive()).divide(BigInteger.valueOf(2));

        SplitShardRequest request = new SplitShardRequest();
        request.setStreamName(STREAM);
        request.setShardToSplit(shard.getShardId());
        request.setNewStartingHashKey(middle.toString());

        System.out.println("Splitting " + shard.getShardId());
        kinesis.splitShard(request);
    }

    protected static Tuple2<Shard, Shard> findAdjacentPair(Shard shard, List<Shard> allShards) {
        HashRange shardRange = HashRange.range(shard.getHashKeyRange());
        for (Shard examinedShard : allShards) {
            HashRange examinedRange = HashRange.range(examinedShard.getHashKeyRange());
            if (shardRange.isAdjacent(examinedRange)) {
                if (shardRange.getMinInclusive().compareTo(examinedRange.getMinInclusive()) <= 0) {
                    return Tuple2.tuple2(shard, examinedShard);
                } else {
                    return Tuple2.tuple2(examinedShard, shard);
                }
            }
        }
        throw new IllegalStateException("There must be an adjacent shard");
    }

    private static String getMessagesDifferDescription(String key, List<String> expected, List<String> actual) {
        StringBuilder sb = new StringBuilder()
                .append("Messages for key ").append(key).append(" differ!")
                .append("\n\texpected: ").append(expected.size())
                .append("\n\t  actual: ").append(actual.size());

        for (int i = 0; i < min(expected.size(), actual.size()); i++) {
            if (!expected.get(i).equals(actual.get(i))) {
                sb.append("\n\tfirst difference at index: ").append(i);
                sb.append("\n\t\texpected: ");
                for (int j = max(0, i - 2); j < min(i + 5, expected.size()); j++) {
                    sb.append(j).append(": ").append(expected.get(j)).append(", ");
                }
                sb.append("\n\t\t  actual: ");
                for (int j = max(0, i - 2); j < min(i + 5, actual.size()); j++) {
                    sb.append(j).append(": ").append(actual.get(j)).append(", ");
                }
                break;
            }
        }

        return sb.toString();
    }

    private static Map<String, List<String>> toMap(List<Map.Entry<String, String>> entryList) {
        return entryList.stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> Collections.singletonList(e.getValue()),
                        (l1, l2) -> {
                            ArrayList<String> retList = new ArrayList<>();
                            retList.addAll(l1);
                            retList.addAll(l2);
                            return retList;
                        }
                ));
    }

    public static boolean shardActive(@Nonnull Shard shard) {
        String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
        return endingSequenceNumber == null;
        //need to rely on this hack, because shard filters don't seem to work, on the mock at least ...
    }
}
