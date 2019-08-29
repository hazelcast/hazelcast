/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.jms;

import com.hazelcast.jet.impl.util.ExceptionUtil;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Utility class to produce messages to the given destination
 */
public final class JmsMessageProducer {

    private final Thread producerThread;

    JmsMessageProducer(String destinationName, DestinationType destinationType) {
        producerThread = new Thread(() -> {
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQBroker.BROKER_URL);
            Connection connection = null;
            try {
                connection = connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination;
                if (destinationType == DestinationType.QUEUE) {
                    destination = session.createQueue(destinationName);
                } else {
                    destination = session.createTopic(destinationName);
                }

                javax.jms.MessageProducer producer = session.createProducer(destination);
                int count = 0;
                while (true) {
                    TextMessage textMessage = session.createTextMessage("Message-" + count++);
                    producer.send(textMessage);
                    SECONDS.sleep(1);
                }
            } catch (JMSException e) {
                throw ExceptionUtil.rethrow(e);
            } catch (InterruptedException ignored) {
            } finally {
                if (connection != null) {
                    uncheckRun(connection::close);
                }
            }
        });
    }

    public void start() {
        producerThread.start();
    }

    public void stop() {
        try {
            producerThread.interrupt();
            producerThread.join();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    enum DestinationType {
        QUEUE, TOPIC
    }
}
