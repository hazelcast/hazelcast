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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.TextMessage;
import java.util.concurrent.CancellationException;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A pipeline which streams messages from a JMS queue, filters them according
 * to the priority and writes a new message with modified properties to another
 * JMS queue.
 */
public class JmsQueueSample {

    private static final String INPUT_QUEUE = "inputQueue";
    private static final String OUTPUT_QUEUE = "outputQueue";

    private ActiveMQBroker broker;
    private JmsMessageProducer producer;
    private JetInstance jet;

    private Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jmsQueue(INPUT_QUEUE, () -> new ActiveMQConnectionFactory(ActiveMQBroker.BROKER_URL)))
         .withoutTimestamps()
         .filter(message -> message.getJMSPriority() > 3)
         .map(message -> (TextMessage) message)
         // print the message text to the log
         .peek(TextMessage::getText)
         .writeTo(Sinks.<TextMessage>jmsQueueBuilder(() -> new ActiveMQConnectionFactory(ActiveMQBroker.BROKER_URL))
                 .destinationName(OUTPUT_QUEUE)
                 .messageFn((session, message) -> {
                     // create new text message with the same text and few additional properties
                     TextMessage textMessage = session.createTextMessage(message.getText());
                     textMessage.setBooleanProperty("isActive", true);
                     textMessage.setJMSPriority(8);
                     return textMessage;
                 })
                 .build());
        return p;
    }

    public static void main(String[] args) throws Exception {
        new JmsQueueSample().go();
    }

    private void go() throws Exception {
        try {
            setup();
            Job job = jet.newJob(buildPipeline());
            SECONDS.sleep(10);
            job.cancel();
            try {
                job.join();
            } catch (CancellationException ignored) {
            }
        } finally {
            cleanup();
        }
    }

    private void setup() throws Exception {
        broker = new ActiveMQBroker();
        producer = new JmsMessageProducer(INPUT_QUEUE, true);
        jet = Jet.bootstrappedInstance();
    }

    private void cleanup() throws Exception {
        producer.stop();
        Jet.shutdownAll();
        broker.stop();
    }
}
