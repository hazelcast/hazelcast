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

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Enumeration;
import java.util.Queue;

import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class StreamJmsPTest extends JetTestSupport {

    @ClassRule
    public static EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    private StreamJmsP processor;
    private TestOutbox outbox;
    private Connection processorConnection;

    @After
    public void stopProcessor() throws Exception {
        processor.close();
        processorConnection.close();
    }

    @Test
    public void when_queue() throws Exception {
        String queueName = randomString();
        logger.info("using queue: " + queueName);
        initializeProcessor(queueName, true);
        String message1 = sendMessage(queueName, true);
        String message2 = sendMessage(queueName, true);

        assertTrueEventually(() -> assertEquals(2, queueSize(queueName)));

        Queue<Object> queue = outbox.queue(0);

        // Even though both messages are in queue, the processor might not see them
        // because it uses `consumer.receiveNoWait()`, so if it's not yet available, it doesn't
        // block and it should be available later.
        // See https://github.com/hazelcast/hazelcast-jet/issues/1010
        assertTrueEventually(() -> {
            processor.complete();
            assertEquals(message1, queue.poll());
        });
        outbox.reset();
        assertTrueEventually(() -> {
            processor.complete();
            assertEquals(message2, queue.poll());
        });
    }

    @Test
    public void when_topic() throws Exception {
        String topicName = randomString();
        logger.info("using topic: " + topicName);
        sendMessage(topicName, false);
        initializeProcessor(topicName, false);
        sleepSeconds(1);
        String message2 = sendMessage(topicName, false);

        Queue<Object> queue = outbox.queue(0);

        assertTrueEventually(() -> {
            processor.complete();
            assertEquals(message2, queue.poll());
        });
    }

    private void initializeProcessor(String destinationName, boolean isQueue) throws Exception {
        processorConnection = broker.createConnectionFactory().createConnection();
        processorConnection.start();

        DistributedFunction<Connection, Session> sessionFn = c -> c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        DistributedFunction<Session, MessageConsumer> consumerFn = s ->
                s.createConsumer(isQueue ? s.createQueue(destinationName) : s.createTopic(destinationName));
        DistributedFunction<Message, String> textMessageFn = m -> ((TextMessage) m).getText();
        processor = new StreamJmsP<>(
                processorConnection, sessionFn, consumerFn, DistributedConsumer.noop(), textMessageFn, noEventTime());
        outbox = new TestOutbox(1);
        Context ctx = new TestProcessorContext().setLogger(Logger.getLogger(StreamJmsP.class));
        processor.init(outbox, ctx);
    }

    private String sendMessage(String destinationName, boolean isQueue) throws Exception {
        String message = randomString();

        ActiveMQConnectionFactory connectionFactory = broker.createConnectionFactory();
        Connection connection = connectionFactory.createConnection();
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

    private int queueSize(String queueName) throws Exception {
        ActiveMQConnectionFactory connectionFactory = broker.createConnectionFactory();
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        QueueBrowser browser = session.createBrowser(session.createQueue(queueName));
        Enumeration enumeration = browser.getEnumeration();
        int size = 0;
        while (enumeration.hasMoreElements()) {
            enumeration.nextElement();
            size++;
        }
        session.close();
        connection.close();
        return size;
    }
}
