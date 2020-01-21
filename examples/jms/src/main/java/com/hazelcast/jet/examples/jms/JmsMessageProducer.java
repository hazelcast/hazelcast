/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import javax.jms.MessageProducer;
import javax.jms.Session;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Utility class to produce messages to the given destination
 */
public final class JmsMessageProducer {

    private final Thread producerThread;

    JmsMessageProducer(String destinationName, boolean isQueue) {
        producerThread = new Thread(() -> {
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQBroker.BROKER_URL);
            try (
                    Connection connection = connectionFactory.createConnection();
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(
                            isQueue ? session.createQueue(destinationName)
                                    : session.createTopic(destinationName))
            ) {
                for (int count = 0; ; count++) {
                    producer.send(session.createTextMessage("Message-" + count));
                    SECONDS.sleep(1);
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
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
}
