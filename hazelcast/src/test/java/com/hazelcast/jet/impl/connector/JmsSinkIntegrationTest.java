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
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.connector.JmsTestUtil.consumeMessages;
import static java.util.stream.IntStream.range;
import static javax.jms.Session.DUPS_OK_ACKNOWLEDGE;

@Category({SlowTest.class, ParallelJVMTest.class})
public class JmsSinkIntegrationTest extends SimpleTestInClusterSupport {

    @ClassRule
    public static EmbeddedActiveMQResource broker = new EmbeddedActiveMQResource();

    private static final int MESSAGE_COUNT = 100;

    private static int counter;

    private String destinationName = "dest" + counter++;
    private IList<Object> srcList = instance().getList("src-" + counter++);

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Test
    public void sinkQueue() throws JMSException {
        populateList();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(srcList.getName()))
         .writeTo(Sinks.jmsQueue(destinationName, JmsSinkIntegrationTest::getConnectionFactory));

        List<Object> messages = consumeMessages(getConnectionFactory(), destinationName, true, MESSAGE_COUNT);

        instance().getJet().newJob(p);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic() throws JMSException {
        populateList();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(srcList.getName()))
         .writeTo(Sinks.jmsTopic(destinationName, JmsSinkIntegrationTest::getConnectionFactory));

        List<Object> messages = consumeMessages(getConnectionFactory(), destinationName, false, MESSAGE_COUNT);
        sleepSeconds(1);

        instance().getJet().newJob(p);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkQueue_whenBuilder() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsQueueBuilder(JmsSinkIntegrationTest::getConnectionFactory)
                .destinationName(destinationName)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list(srcList.getName()))
         .writeTo(sink);

        List<Object> messages = consumeMessages(getConnectionFactory(), destinationName, true, MESSAGE_COUNT);

        instance().getJet().newJob(p);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkQueue_whenBuilder_withFunctions() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsQueueBuilder(JmsSinkIntegrationTest::getConnectionFactory)
                .connectionFn(ConnectionFactory::createConnection)
                .messageFn(Session::createTextMessage)
                .destinationName(destinationName)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list(srcList.getName()))
         .writeTo(sink);

        List<Object> messages = consumeMessages(getConnectionFactory(), destinationName, true, MESSAGE_COUNT);

        instance().getJet().newJob(p);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic_whenBuilder() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsTopicBuilder(JmsSinkIntegrationTest::getConnectionFactory)
                .destinationName(destinationName)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list(srcList.getName()))
         .writeTo(sink);

        List<Object> messages = consumeMessages(getConnectionFactory(), destinationName, false, MESSAGE_COUNT);
        sleepSeconds(1);

        instance().getJet().newJob(p);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic_whenBuilder_withParameters() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsTopicBuilder(JmsSinkIntegrationTest::getConnectionFactory)
                .connectionParams(null, null)
                .destinationName(destinationName)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list(srcList.getName()))
         .writeTo(sink);

        List<Object> messages = consumeMessages(getConnectionFactory(), destinationName, false, MESSAGE_COUNT);
        sleepSeconds(1);

        instance().getJet().newJob(p);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void test_transactional_withRestarts_graceful_exOnce() throws Exception {
        test_transactional_withRestarts(true, true);
    }

    @Test
    public void test_transactional_withRestarts_forceful_exOnce() throws Exception {
        test_transactional_withRestarts(false, true);
    }

    @Test
    public void test_transactional_withRestarts_graceful_atLeastOnce() throws Exception {
        test_transactional_withRestarts(true, false);
    }

    @Test
    public void test_transactional_withRestarts_forceful_atLeastOnce() throws Exception {
        test_transactional_withRestarts(false, false);
    }

    private void test_transactional_withRestarts(boolean graceful, boolean exactlyOnce) throws Exception {
        String destinationName = randomString();
        Sink<Integer> sink = Sinks
                .<Integer>jmsQueueBuilder(() -> exactlyOnce
                        ? new ActiveMQXAConnectionFactory(broker.getVmURL())
                        : new ActiveMQConnectionFactory(broker.getVmURL()))
                .destinationName(destinationName)
                .exactlyOnce(exactlyOnce)
                .build();

        try (
                Connection connection = new ActiveMQConnectionFactory(broker.getVmURL()).createConnection();
                Session session = connection.createSession(false, DUPS_OK_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(session.createQueue(destinationName))
        ) {
            connection.start();
            List<Integer> actualSinkContents = new ArrayList<>();
            SinkStressTestUtil.test_withRestarts(instance(), logger, sink, graceful, exactlyOnce, () -> {
                for (Message msg; (msg = consumer.receiveNoWait()) != null; ) {
                    actualSinkContents.add(Integer.valueOf(((TextMessage) msg).getText()));
                }
                return actualSinkContents;
            });
        }
    }

    private void populateList() {
        range(0, MESSAGE_COUNT).mapToObj(i -> randomString()).forEach(srcList::add);
    }

    private static ConnectionFactory getConnectionFactory() {
        return new ActiveMQXAConnectionFactory(broker.getVmURL());
    }
}
