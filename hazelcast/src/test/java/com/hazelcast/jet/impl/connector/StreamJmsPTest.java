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

import com.hazelcast.cluster.Address;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQResource;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;

import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamJmsPTest extends JetTestSupport {

    @ClassRule
    public static EmbeddedActiveMQResource resource = new EmbeddedActiveMQResource();

    @SuppressWarnings("rawtypes")
    private StreamJmsP processor;
    private TestOutbox outbox;
    private Connection processorConnection;

    @After
    public void stopProcessor() throws Exception {
        if (processor != null) {
            processor.close();
        }
        if (processorConnection != null) {
            processorConnection.close();
        }
    }

    @Test
    public void when_queue() throws Exception {
        String queueName = randomString();
        logger.info("using queue: " + queueName);
        String message1 = sendMessage(queueName, true);
        String message2 = sendMessage(queueName, true);

        initializeProcessor(queueName, true, null);
        Queue<Object> queue = outbox.queue(0);

        // Even though both messages are in queue, the processor might not see them
        // because it uses `consumer.receiveNoWait()`, so if they are not available immediately,
        // it doesn't block and items should be available later.
        // See https://github.com/hazelcast/hazelcast-jet/issues/1010
        List<Object> actualOutput = new ArrayList<>();
        assertTrueEventually(() -> {
            outbox.reset();
            processor.complete();
            Object item = queue.poll();
            if (item != null) {
                actualOutput.add(item);
            }
            assertEquals(asList(message1, message2), actualOutput);
        });
    }

    @Test
    public void when_topic() throws Exception {
        String topicName = randomString();
        logger.info("using topic: " + topicName);
        sendMessage(topicName, false);
        initializeProcessor(topicName, false, null);
        processor.complete(); // the consumer is created here
        sleepSeconds(1);
        String message2 = sendMessage(topicName, false);

        Queue<Object> queue = outbox.queue(0);

        assertTrueEventually(() -> {
            processor.complete();
            assertEquals(message2, queue.poll());
        });
    }

    @Test
    public void when_projectionToNull_then_filteredOut() throws Exception {
        String queueName = randomString();
        logger.info("using queue: " + queueName);
        String message1 = sendMessage(queueName, true);
        String message2 = sendMessage(queueName, true);

        initializeProcessor(queueName, true, m -> {
            String msg = ((TextMessage) m).getText();
            return msg.equals(message1) ? null : msg;
        });
        Queue<Object> queue = outbox.queue(0);

        List<Object> actualOutput = new ArrayList<>();
        assertTrueEventually(() -> {
            outbox.reset();
            processor.complete();
            Object item = queue.poll();
            if (item != null) {
                actualOutput.add(item);
            }
            assertEquals(singletonList(message2), actualOutput);
        });
    }

    @Test
    public void when_sharedConsumer_then_twoProcessorsUsed() throws Exception {
        String topicName = randomString();
        logger.info("using topic: " + topicName);
        StreamSource<Message> source = Sources.jmsTopicBuilder(StreamJmsPTest::getConnectionFactory)
                                              // When
                                              .destinationName("foo")
                                              .sharedConsumer(true)
                                              .build();
        ProcessorMetaSupplier metaSupplier =
                ((StreamSourceTransform<Message>) source).metaSupplierFn.apply(noEventTime());
        Address address1 = new Address("127.0.0.1", 1);
        Address address2 = new Address("127.0.0.1", 2);
        Function<? super Address, ? extends ProcessorSupplier> function = metaSupplier.get(asList(address1, address2));

        // Then
        // assert that processors for both addresses are actually StreamJmsP, not a noopP
        assertInstanceOf(StreamJmsP.class, function.apply(address1).get(1).iterator().next());
        assertInstanceOf(StreamJmsP.class, function.apply(address2).get(1).iterator().next());
    }

    private void initializeProcessor(
            String destinationName,
            boolean isQueue,
            FunctionEx<Message, String> projectionFn
    ) throws Exception {
        processorConnection = getConnectionFactory().createConnection();
        processorConnection.start();

        FunctionEx<Session, MessageConsumer> consumerFn = s ->
                s.createConsumer(isQueue ? s.createQueue(destinationName) : s.createTopic(destinationName));
        if (projectionFn == null) {
            projectionFn = m -> ((TextMessage) m).getText();
        }
        processor = new StreamJmsP<>(
                processorConnection, consumerFn, Message::getJMSMessageID, projectionFn, noEventTime(), NONE);
        outbox = new TestOutbox(1);
        processor.init(outbox, new TestProcessorContext());
    }

    private String sendMessage(String destinationName, boolean isQueue) throws Exception {
        String message = randomString();

        Connection connection = getConnectionFactory().createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName);
        MessageProducer producer = session.createProducer(destination);
        TextMessage textMessage = session.createTextMessage(message);
        producer.send(textMessage);
        logger.info("sent message " + message + " to " + destinationName);
        session.close();
        connection.close();
        return message;
    }

    private static ConnectionFactory getConnectionFactory() {
        return new ActiveMQConnectionFactory(resource.getVmURL());
    }
}
