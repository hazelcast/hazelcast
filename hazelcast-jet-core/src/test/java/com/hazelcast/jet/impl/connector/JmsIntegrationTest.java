/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.PipelineTestSupport;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
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

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JmsIntegrationTest extends PipelineTestSupport {

    @ClassRule
    public static EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    private static final int MESSAGE_COUNT = 100;
    private static final DistributedFunction<Message, String> TEXT_MESSAGE_FN = m -> ((TextMessage) m).getText();

    private String destinationName = randomString();
    private Job job;

    @Test
    public void sourceQueue() {
        p.drawFrom(Sources.jmsQueue(() -> broker.createConnectionFactory(), destinationName))
         .withoutTimestamps()
         .map(TEXT_MESSAGE_FN)
         .drainTo(sink);

        startJob(true);

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);

        cancelJob();
    }

    @Test
    public void sourceTopic() {
        p.drawFrom(Sources.jmsTopic(() -> broker.createConnectionFactory(), destinationName))
         .withoutTimestamps()
         .map(TEXT_MESSAGE_FN)
         .drainTo(sink);

        startJob(true);
        sleepSeconds(1);

        List<Object> messages = sendMessages(false);
        assertEqualsEventually(sinkList::size, MESSAGE_COUNT);
        assertContainsAll(sinkList, messages);

        cancelJob();
    }

    @Test
    public void sinkQueue() throws JMSException {
        populateList();

        p.drawFrom(Sources.list(srcList.getName()))
         .drainTo(Sinks.jmsQueue(() -> broker.createConnectionFactory(), destinationName));

        List<Object> messages = consumeMessages(true);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic() throws JMSException {
        populateList();

        p.drawFrom(Sources.list(srcList.getName()))
         .drainTo(Sinks.jmsTopic(() -> broker.createConnectionFactory(), destinationName));

        List<Object> messages = consumeMessages(false);
        sleepSeconds(1);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sourceQueue_whenBuilder() {
        StreamSource<Message> source = Sources.jmsQueueBuilder(() -> broker.createConnectionFactory())
                                              .destinationName(destinationName)
                                              .build();

        p.drawFrom(source)
         .withoutTimestamps()
         .map(TEXT_MESSAGE_FN)
         .drainTo(sink);

        startJob(true);

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);

        cancelJob();
    }

    @Test
    public void sourceQueue_whenBuilder_withFunctions() {
        String queueName = destinationName;
        StreamSource<String> source = Sources.<String>jmsQueueBuilder(() -> broker.createConnectionFactory())
                .connectionFn(ConnectionFactory::createConnection)
                .sessionFn(connection -> connection.createSession(false, AUTO_ACKNOWLEDGE))
                .consumerFn(session -> session.createConsumer(session.createQueue(queueName)))
                .flushFn(DistributedConsumer.noop())
                .build(TEXT_MESSAGE_FN);

        p.drawFrom(source).withoutTimestamps().drainTo(sink);

        startJob(true);

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);

        cancelJob();
    }

    @Test
    public void sourceTopic_withNativeTimestamps() throws Exception {
        p.drawFrom(Sources.jmsTopic(() -> broker.createConnectionFactory(), destinationName))
         .withNativeTimestamps(0)
         .map(Message::getJMSTimestamp)
         .window(tumbling(1))
         .aggregate(counting())
         .drainTo(sink);

        startJob(true);
        sendMessages(false);
        // sleep some time and emit a flushing message, that won't make it to the output, because
        // the messages with the highest timestamp are not emitted
        sleepMillis(500);
        sendMessage(destinationName, false);

        assertTrueEventually(() -> {
            long countSum = sinkList.stream().mapToLong(o -> ((TimestampedItem<Long>) o).item()).sum();
            assertEquals(MESSAGE_COUNT, countSum);

            // There's no way to see the JetEvent's timestamp by the user code. In order to check
            // the native timestamp, we aggregate the events into tumbling(1) windows and check
            // the timestamps of the windows: we assert that it is around the current time.
            long avgTime = (long) sinkList.stream().mapToLong(o -> ((TimestampedItem<Long>) o).timestamp())
                                          .average().orElse(0);
            long tenMinutes = MINUTES.toMillis(1);
            long now = System.currentTimeMillis();
            assertTrue("Time too much off: " + Instant.ofEpochMilli(avgTime).atZone(ZoneId.systemDefault()),
                    avgTime > now - tenMinutes && avgTime < now + tenMinutes);
        }, 10);

        cancelJob();
    }

    @Test
    public void sourceTopic_whenBuilder() {
        StreamSource<String> source = Sources.<String>jmsTopicBuilder(() -> broker.createConnectionFactory())
                .destinationName(destinationName)
                .build(TEXT_MESSAGE_FN);

        p.drawFrom(source).withoutTimestamps().drainTo(sink);

        startJob(true);
        sleepSeconds(1);

        List<Object> messages = sendMessages(false);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);

        cancelJob();
    }

    @Test
    public void sourceTopic_whenBuilder_withParameters() {
        StreamSource<String> source = Sources.<String>jmsTopicBuilder(() -> broker.createConnectionFactory())
                .connectionParams(null, null)
                .sessionParams(false, AUTO_ACKNOWLEDGE)
                .destinationName(destinationName)
                .build(TEXT_MESSAGE_FN);

        p.drawFrom(source).withoutTimestamps().drainTo(sink);

        startJob(true);
        sleepSeconds(1);

        List<Object> messages = sendMessages(false);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);

        cancelJob();
    }

    @Test
    public void sinkQueue_whenBuilder() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsQueueBuilder(() -> broker.createConnectionFactory())
                .destinationName(destinationName)
                .build();

        p.drawFrom(Sources.<String>list(srcList.getName()))
         .drainTo(sink);

        List<Object> messages = consumeMessages(true);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkQueue_whenBuilder_withFunctions() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsQueueBuilder(() -> broker.createConnectionFactory())
                .connectionFn(ConnectionFactory::createConnection)
                .sessionFn(connection -> connection.createSession(false, AUTO_ACKNOWLEDGE))
                .messageFn(Session::createTextMessage)
                .sendFn(MessageProducer::send)
                .flushFn(DistributedConsumer.noop())
                .destinationName(destinationName)
                .build();

        p.drawFrom(Sources.<String>list(srcList.getName()))
         .drainTo(sink);

        List<Object> messages = consumeMessages(true);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic_whenBuilder() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsTopicBuilder(() -> broker.createConnectionFactory())
                .destinationName(destinationName)
                .build();

        p.drawFrom(Sources.<String>list(srcList.getName()))
         .drainTo(sink);

        List<Object> messages = consumeMessages(false);
        sleepSeconds(1);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic_whenBuilder_withParameters() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsTopicBuilder(() -> broker.createConnectionFactory())
                .connectionParams(null, null)
                .sessionParams(false, AUTO_ACKNOWLEDGE)
                .destinationName(destinationName)
                .build();

        p.drawFrom(Sources.<String>list(srcList.getName()))
         .drainTo(sink);

        List<Object> messages = consumeMessages(false);
        sleepSeconds(1);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    private List<Object> consumeMessages(boolean isQueue) throws JMSException {
        ActiveMQConnectionFactory connectionFactory = broker.createConnectionFactory();
        Connection connection = connectionFactory.createConnection();
        connection.start();

        List<Object> messages = synchronizedList(new ArrayList<>());
        spawn(() -> uncheckRun(() -> {
            Session session = connection.createSession(false, AUTO_ACKNOWLEDGE);
            Destination dest = isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName);
            MessageConsumer consumer = session.createConsumer(dest);
            int count = 0;
            while (count < MESSAGE_COUNT) {
                messages.add(((TextMessage) consumer.receive()).getText());
                count++;
            }
            consumer.close();
            session.close();
            connection.close();
        }));
        return messages;
    }

    private List<Object> sendMessages(boolean isQueue) {
        return range(0, MESSAGE_COUNT)
                .mapToObj(i -> uncheckCall(() -> sendMessage(destinationName, isQueue)))
                .collect(toList());
    }

    private void populateList() {
        range(0, MESSAGE_COUNT).mapToObj(i -> randomString()).forEach(srcList::add);
    }

    private void startJob(boolean waitForRunning) {
        job = start();
        // batch jobs can be completed before we observe RUNNING status
        if (waitForRunning) {
            assertJobStatusEventually(job, JobStatus.RUNNING, 10);
        }
    }

    private void cancelJob() {
        job.cancel();
        assertJobStatusEventually(job, JobStatus.FAILED, 10);
    }

    private String sendMessage(String destinationName, boolean isQueue) throws Exception {
        String message = randomString();

        ActiveMQConnectionFactory connectionFactory = broker.createConnectionFactory();
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, AUTO_ACKNOWLEDGE);
        Destination destination = isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName);
        MessageProducer producer = session.createProducer(destination);
        TextMessage textMessage = session.createTextMessage(message);
        producer.send(textMessage);
        session.close();
        connection.close();
        return message;
    }
}
