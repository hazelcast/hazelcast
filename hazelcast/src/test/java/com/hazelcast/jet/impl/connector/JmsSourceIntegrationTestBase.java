/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.connector;

import com.hazelcast.collection.IList;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.JmsSourceBuilder;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.function.PredicateEx.alwaysFalse;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.TestProcessors.MapWatermarksToString.mapWatermarksToString;
import static com.hazelcast.jet.impl.connector.JmsTestUtil.consumeMessages;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static javax.jms.Session.DUPS_OK_ACKNOWLEDGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class JmsSourceIntegrationTestBase extends SimpleTestInClusterSupport {

    private static final int MESSAGE_COUNT = 100;
    private static final FunctionEx<Message, String> TEXT_MESSAGE_FN = m -> ((TextMessage) m).getText();
    private static volatile List<Long> lastListInStressTest;
    private static int counter;

    private String destinationName = "dest" + counter++;

    private Pipeline p = Pipeline.create();
    private IList<Object> sinkList;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Before
    public void before() {
        sinkList = instance().getList("sink-" + counter++);
    }

    @Test
    public void sourceQueue() throws JMSException {
        p.readFrom(Sources.jmsQueue(destinationName, getConnectionFactory()))
         .withoutTimestamps()
         .map(TEXT_MESSAGE_FN)
         .writeTo(Sinks.list(sinkList));

        startJob();

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);
    }

    @Test
    public void sourceTopic() throws JMSException {
        p.readFrom(Sources.jmsTopic(destinationName, getConnectionFactory()))
         .withoutTimestamps()
         .map(TEXT_MESSAGE_FN)
         .writeTo(Sinks.list(sinkList));

        startJob();
        waitForTopicConsumption();

        List<Object> messages = sendMessages(false);
        assertTrueEventually(() -> assertContainsAll(sinkList, messages));
    }

    private void waitForTopicConsumption() {
        // the source starts consuming messages some time after the job is running. Send some
        // messages first until we see they're consumed
        assertTrueEventually(() -> {
            sendMessages(false, 1);
            assertFalse("nothing in sink", sinkList.isEmpty());
        });
    }

    @Test
    public void sourceQueue_whenBuilder() throws JMSException {
        StreamSource<Message> source = Sources.jmsQueueBuilder(getConnectionFactory())
                                              .destinationName(destinationName)
                                              .build();

        p.readFrom(source)
         .withoutTimestamps()
         .map(TEXT_MESSAGE_FN)
         .writeTo(Sinks.list(sinkList));

        startJob();

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);
    }

    @Test
    public void sourceQueue_whenBuilder_withFunctions() throws JMSException {
        String queueName = destinationName;
        StreamSource<String> source = Sources.jmsQueueBuilder(getConnectionFactory())
                                             .connectionFn(ConnectionFactory::createConnection)
                                             .consumerFn(session -> session.createConsumer(session.createQueue(queueName)))
                                             .messageIdFn(m -> {
                                                 throw new UnsupportedOperationException();
                                             })
                                             .build(TEXT_MESSAGE_FN);

        p.readFrom(source).withoutTimestamps().writeTo(Sinks.list(sinkList));

        startJob();

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);
    }

    @Test
    public void when_messageIdFn_then_used() throws JMSException {
        StreamSource<String> source = Sources.jmsQueueBuilder(getConnectionFactory())
                                             .destinationName(destinationName)
                                             .messageIdFn(m -> {
                                                 throw new RuntimeException("mock exception");
                                             })
                                             .build(TEXT_MESSAGE_FN);
        p.readFrom(source).withoutTimestamps().writeTo(Sinks.logger());

        Job job = instance().getJet().newJob(p, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE));
        sendMessages(true);
        try {
            job.join();
            fail("job didn't fail");
        } catch (Exception e) {
            assertContains(e.toString(), "mock exception");
        }
    }

    @Test
    public void when_exactlyOnceTopicDefaultConsumer_then_noGuaranteeUsed() {
        SupplierEx<ConnectionFactory> mockSupplier = () -> {
            ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
            Connection mockConn = mock(Connection.class);
            Session mockSession = mock(Session.class);
            MessageConsumer mockConsumer = mock(MessageConsumer.class);
            when(mockConnectionFactory.createConnection()).thenReturn(mockConn);
            when(mockConn.createSession(anyBoolean(), anyInt())).thenReturn(mockSession);
            when(mockSession.createConsumer(any())).thenReturn(mockConsumer);
            // throw, if commit is called
            doThrow(new AssertionError("commit must not be called")).when(mockSession).commit();
            return (ConnectionFactory) mockConnectionFactory;
        };

        p.readFrom(Sources.jmsTopic(destinationName, mockSupplier))
         .withoutTimestamps()
         .writeTo(Sinks.logger());

        Job job = instance().getJet().newJob(p, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE).setSnapshotIntervalMillis(10));
        assertJobStatusEventually(job, RUNNING);
        JobRepository jr = new JobRepository(instance());
        waitForFirstSnapshot(jr, job.getId(), 5, true);
        assertTrueAllTheTime(() -> assertEquals(RUNNING, job.getStatus()), 1);
    }

    @Test
    public void sourceTopic_withNativeTimestamps() throws Exception {
        p.readFrom(Sources.jmsTopic(destinationName, getConnectionFactory()))
         .withNativeTimestamps(0)
         .map(Message::getJMSTimestamp)
         .window(tumbling(1))
         .aggregate(counting())
         .writeTo(Sinks.list(sinkList));

        startJob();
        waitForTopicConsumption();
        sendMessages(false);

        assertTrueEventually(() -> {
            // There's no way to see the JetEvent's timestamp by the user code. In order to check
            // the native timestamp, we aggregate the events into tumbling(1) windows and check
            // the timestamps of the windows: we assert that it is around the current time.
            @SuppressWarnings("unchecked")
            long avgTime = (long) sinkList.stream().mapToLong(o -> ((WindowResult<Long>) o).end())
                                          .average().orElse(0);
            long oneMinute = MINUTES.toMillis(1);
            long now = System.currentTimeMillis();
            assertTrue("Time too much off: " + Instant.ofEpochMilli(avgTime).atZone(ZoneId.systemDefault()),
                    avgTime > now - oneMinute && avgTime < now + oneMinute);
        }, 10);
    }

    @Test
    public void sourceTopic_whenBuilder() throws JMSException {
        StreamSource<String> source = Sources.jmsTopicBuilder(getConnectionFactory())
                                             .destinationName(destinationName)
                                             .build(TEXT_MESSAGE_FN);

        p.readFrom(source).withoutTimestamps().writeTo(Sinks.list(sinkList));

        startJob();
        waitForTopicConsumption();

        List<Object> messages = sendMessages(false);
        assertTrueEventually(() -> assertContainsAll(sinkList, messages));
    }

    @Test
    public void stressTest_exactlyOnce_forceful() throws Exception {
        stressTest(false, EXACTLY_ONCE, false);
    }

    @Test
    public void stressTest_exactlyOnce_graceful() throws Exception {
        stressTest(true, EXACTLY_ONCE, false);
    }

    @Test
    public void stressTest_atLeastOnce_forceful() throws Exception {
        stressTest(false, AT_LEAST_ONCE, false);
    }

    @Test
    public void stressTest_noGuarantee_forceful() throws Exception {
        stressTest(false, NONE, false);
    }

    @Test
    public void stressTest_exactlyOnce_forceful_durableTopic() throws Exception {
        stressTest(false, EXACTLY_ONCE, true);
    }

    private void stressTest(boolean graceful, ProcessingGuarantee maxGuarantee, boolean useTopic)
            throws Exception {
        lastListInStressTest = null;
        final int MESSAGE_COUNT = 4_000;
        Pipeline p = Pipeline.create();
        String destName = "queue-" + counter++;
        JmsSourceBuilder sourceBuilder;
        if (useTopic) {
            sourceBuilder = Sources.jmsTopicBuilder(getConnectionFactory())
                    .sharedConsumer(true)
                    .consumerFn(s -> s.createSharedDurableConsumer(s.createTopic(destName), "foo-consumer"));
            // create the durable subscriber now so that it doesn't lose the initial messages
            try (Connection conn = getConnectionFactory().get().createConnection()) {
                conn.setClientID("foo-client-id");
                try (Session sess = conn.createSession(false, DUPS_OK_ACKNOWLEDGE)) {
                    sess.createDurableSubscriber(sess.createTopic(destName), "foo-consumer");
                }
            }
        } else {
            sourceBuilder = Sources.jmsQueueBuilder(getConnectionFactory())
                    .destinationName(destName);
        }
        p.readFrom(sourceBuilder
                          .maxGuarantee(maxGuarantee)
                          .build(msg -> Long.parseLong(((TextMessage) msg).getText())))
         .withoutTimestamps()
         .peek()
         .mapStateful(CopyOnWriteArrayList<Long>::new,
                 (list, item) -> {
                     lastListInStressTest = list;
                     list.add(item);
                     return null;
                 })
         .writeTo(Sinks.logger());

        Job job = instance().getJet().newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50));
        assertJobStatusEventually(job, RUNNING);

        // start a producer that will produce MESSAGE_COUNT messages on the background to the queue, 1000 msgs/s
        @SuppressWarnings("rawtypes")
        Future producerFuture = spawn(() -> {
            try (
                    Connection connection = getConnectionFactory().get().createConnection();
                    Session session = connection.createSession(false, AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(
                            useTopic ? session.createTopic(destName) : session.createQueue(destName))
            ) {
                long startTime = System.nanoTime();
                for (int i = 0; i < MESSAGE_COUNT; i++) {
                    producer.send(session.createTextMessage(String.valueOf(i)));
                    Thread.sleep(Math.max(0, i - NANOSECONDS.toMillis(System.nanoTime() - startTime)));
                }
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        });

        int iteration = 0;
        JobRepository jr = new JobRepository(instance());
        waitForFirstSnapshot(jr, job.getId(), 20, true);
        while (!producerFuture.isDone()) {
            Thread.sleep(ThreadLocalRandom.current().nextInt(200));
            // Ensure progress was made to avoid the pathological case when we never get to commit anything.
            // We also do it before the first restart to workaround https://issues.apache.org/jira/browse/ARTEMIS-2546
            if (iteration++ % 3 == 0) {
                waitForNextSnapshot(jr, job.getId(), 20, true);
            }
            ((JobProxy) job).restart(graceful);
            assertJobStatusEventually(job, RUNNING);
        }
        producerFuture.get(); // call for the side-effect of throwing if the producer failed

        assertTrueEventually(() -> {
            Map<Long, Long> counts = lastListInStressTest.stream()
                    .collect(Collectors.groupingBy(Function.identity(), TreeMap::new, Collectors.counting()));
            for (long i = 0; i < MESSAGE_COUNT; i++) {
                counts.putIfAbsent(i, 0L);
            }
            String countsStr = "counts: " + counts;
            if (maxGuarantee == NONE) {
                // we don't assert anything and only wait little more and check that the job didn't fail
                sleepSeconds(1);
            } else {
                // in EXACTLY_ONCE the list must have each item exactly once
                // in AT_LEAST_ONCE the list must have each item at least once
                assertTrue(countsStr,
                        counts.values().stream().allMatch(cnt -> maxGuarantee == EXACTLY_ONCE ? cnt == 1 : cnt > 0));
            }
            logger.info(countsStr);
        }, 30);
        assertEquals(job.getStatus(), RUNNING);
    }

    @Test
    public void when_noMessages_then_idle() throws Exception {
        // Design of this test:
        // We'll have 2 processors and only one message. One of the processors will receive
        // it. If the processor doesn't become idle, the watermark will never be emitted. We
        // assert that it is.
        sendMessages(true, 1);

        int idleTimeout = 2000;
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jmsQueue(destinationName, getConnectionFactory())
                          .setPartitionIdleTimeout(idleTimeout))
         .withNativeTimestamps(0)
         .setLocalParallelism(2)
         .filter(alwaysFalse())
         .customTransform("map", mapWatermarksToString(true))
         .setLocalParallelism(1)
         .writeTo(Sinks.list(sinkList));

        long endTime = System.nanoTime() + MILLISECONDS.toNanos(idleTimeout);
        instance().getJet().newJob(p);
        for (;;) {
            boolean empty = sinkList.isEmpty();
            if (System.nanoTime() >= endTime) {
                break;
            }
            assertTrue("sink not empty before the idle timeout elapsed: " + new ArrayList<>(sinkList), empty);
        }

        assertTrueEventually(() -> assertFalse("wm not received in the sink", sinkList.isEmpty()));
        assertStartsWith("wm(", (String) sinkList.get(0));
    }

    @Test
    public void when_jobCancelled_then_rollsBackNonPreparedTransactions_xa() throws Exception {
        when_jobCancelled_then_rollsBackNonPreparedTransactions(true);
    }

    @Test
    public void when_jobCancelled_then_rollsBackNonPreparedTransactions_nonXa() throws Exception {
        when_jobCancelled_then_rollsBackNonPreparedTransactions(false);
    }

    private void when_jobCancelled_then_rollsBackNonPreparedTransactions(boolean xa) throws Exception {
        sendMessages(true, MESSAGE_COUNT);

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jmsQueue(destinationName, getConnectionFactory()))
         .withoutTimestamps()
         .peek()
         .writeTo(Sinks.noop());

        Job job = instance().getJet().newJob(p, new JobConfig()
                .setProcessingGuarantee(xa ? EXACTLY_ONCE : AT_LEAST_ONCE)
                .setSnapshotIntervalMillis(100_000_000));

        assertJobStatusEventually(job, RUNNING);
        sleepSeconds(1); // give the job some more time to consume the message

        // When
        ditchJob(job, instance());

        // Then
        // if the transaction was rolled back, we'll see the messages. If not, they will be blocked
        // (we configured a long transaction timeout for Artemis)
        List<Object> messages = consumeMessages(getConnectionFactory().get(), destinationName, true, MESSAGE_COUNT);
        assertEqualsEventually(messages::size, MESSAGE_COUNT);
    }

    private List<Object> sendMessages(boolean isQueue) throws JMSException {
        return sendMessages(isQueue, MESSAGE_COUNT);
    }

    private List<Object> sendMessages(boolean isQueue, int messageCount) throws JMSException {
        return JmsTestUtil.sendMessages(getConnectionFactory().get(), destinationName, isQueue, messageCount);
    }

    private void startJob() {
        Job job = instance().getJet().newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING, 10);
    }

    protected abstract SupplierEx<ConnectionFactory> getConnectionFactory();
}
