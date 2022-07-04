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

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamKafkaP_StandaloneKafkaTest extends JetTestSupport {

    @Test
    public void when_cancelledAfterBrokerDown_then_cancelsPromptly() throws IOException {
        KafkaTestSupport kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
        kafkaTestSupport.createTopic("topic", 1);
        DAG dag = new DAG();
        dag.newVertex("src", KafkaProcessors.streamKafkaP(getProperties(kafkaTestSupport.getBrokerConnectionString()),
                FunctionEx.identity(), EventTimePolicy.noEventTime(), "topic"))
           .localParallelism(1);

        Job job = createHazelcastInstance().getJet().newJob(dag);
        assertJobStatusEventually(job, RUNNING);
        sleepSeconds(1);
        kafkaTestSupport.shutdownKafkaCluster();
        sleepSeconds(3);
        long start = System.nanoTime();
        job.cancel();
        try {
            job.join();
        } catch (CancellationException ignored) { }
        // There was an issue claimed that when the broker was down, job did not cancel.
        // Let's assert the cancellation didn't take too long.
        long durationSeconds = NANOSECONDS.toSeconds(System.nanoTime() - start);
        assertTrue("durationSeconds=" + durationSeconds, durationSeconds < 10);
        logger.info("Job cancelled in " + durationSeconds + " seconds");
    }

    public static Properties getProperties(String brokerConnectionString) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerConnectionString);
        properties.setProperty("key.deserializer", IntegerDeserializer.class.getCanonicalName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }
}
