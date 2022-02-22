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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static java.util.Collections.synchronizedList;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;

final class JmsTestUtil {
    private JmsTestUtil() { }

    static List<Object> sendMessages(ConnectionFactory cf, String destinationName, boolean isQueue, int count)
            throws JMSException {
        try (
                Connection conn = cf.createConnection();
                Session session = conn.createSession(false, AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(
                        isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName))
        ) {
            List<Object> res = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String message = "msg-" + i;
                producer.send(session.createTextMessage(message));
                res.add(message);
            }
            return res;
        }
    }

    static List<Object> consumeMessages(ConnectionFactory cf, String destinationName, boolean isQueue, int expectedCount)
            throws JMSException {
        Connection connection = cf.createConnection();
        connection.start();

        List<Object> messages = synchronizedList(new ArrayList<>());
        spawn(() -> {
            try (Session session = connection.createSession(false, AUTO_ACKNOWLEDGE);
                 MessageConsumer consumer = session.createConsumer(
                         isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName))
            ) {
                int count = 0;
                while (count < expectedCount) {
                    messages.add(((TextMessage) consumer.receive()).getText());
                    count++;
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw sneakyThrow(e);
            }
        });
        return messages;
    }
}
