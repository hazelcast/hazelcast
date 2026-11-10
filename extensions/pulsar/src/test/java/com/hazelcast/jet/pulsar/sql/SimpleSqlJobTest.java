/*
 * Copyright 2026 Hazelcast Inc.
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

package com.hazelcast.jet.pulsar.sql;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pulsar.PulsarTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.NightlyTest;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.jet.pulsar.PulsarDataConnection.pulsarDataConnectionConf;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@SerialTest
@NightlyTest
public class SimpleSqlJobTest extends PulsarTestSupport {
    private static final int ITEM_COUNT = 1_00;
    private final List<Closeable> closeables = new ArrayList<>();

    private HazelcastInstance[] instances;
    private SqlService sql;

    @BeforeEach
    public void setup() {
        instances = createHazelcastInstances(config(), 2);
        sql = instances[0].getSql();
    }

    @AfterEach
    void closeCloseables() {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception e) {
                logger.warning("Exception while closing resources", e);
            }
        }
        closeables.clear();
    }

    @Test
    @Timeout(value = 20, unit = MINUTES)
    public void test() throws Exception {
        String inputTopic = RandomStringUtils.insecure().nextAlphabetic(10);
        String resultTopic = RandomStringUtils.insecure().nextAlphabetic(10);
        createTopic(inputTopic, true);
        createTopic(resultTopic, true);

        new SqlTestSupport.SqlMapping("resultTopic", "testPulsar").externalName(resultTopic)
            .fields("__key VARCHAR", "this VARCHAR")
            .options(OPTION_KEY_FORMAT, "varchar", OPTION_VALUE_FORMAT, "varchar")
            .createOrReplace(instances[0]);
        new SqlTestSupport.SqlMapping("inputTopic", "testPulsar").externalName(inputTopic)
            .fields("__key VARCHAR", "this VARCHAR")
            .options(OPTION_KEY_FORMAT, "varchar", OPTION_VALUE_FORMAT, "varchar")
            .createOrReplace(instances[0]);

        sql.executeUpdate("""
                CREATE JOB IF NOT EXISTS bounceTest
                OPTIONS (
                        'processingGuarantee' = 'exactlyOnce',
                        'snapshotIntervalMillis' = '1000'
                ) AS
                INSERT INTO resultTopic
                SELECT * FROM inputTopic
                """);
        Producer<String> producer = getClient().newProducer(Schema.STRING)
                                               .topic(inputTopic)
                                               .sendTimeout(10, SECONDS)
                                               .create();
        closeables.add(producer);
        //noinspection resource
        ExecutorService executor = Executors.newSingleThreadExecutor();
        closeables.add(executor::shutdownNow);
        Future<Integer> publisher = executor.submit(() -> {
            int published = 0;
            for (int i = 0; i < ITEM_COUNT; i++) {
                producer.send("hello-pulsar-" + published);
                published++;
                sleepMillis(300);
            }
            return published;
        });

        int published = publisher.get(10, MINUTES);
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < published; i++) {
            String message = "hello-pulsar-" + i;
            expected.add(message);
        }
        Lock collectionLock = new ReentrantLock();
        List<String> result = new ArrayList<>();
        try (var topicReader = getClient().newReader(Schema.STRING)
                                          .startMessageId(MessageId.earliest)
                                          .subscriptionName(randomName())
                                          .topic(resultTopic)
                                          .readerListener((reader, msg) -> {
                                              String value = msg.getValue();
                                              collectionLock.lock();
                                              try {
                                                  result.add(value);
                                              } finally {
                                                  collectionLock.unlock();
                                              }
                                          })
                                          .create()) {
            closeables.add(topicReader);
            topicReader.hasMessageAvailable();

            assertTrueEventually(() -> {
                collectionLock.lock();
                try {
                    assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
                } finally {
                    collectionLock.unlock();
                }
                assertThat(result).hasSize(expected.size());
            });
            logger.info("Pulsar source churn: all " + published + " published messages received");
        }
    }

    private static Config config() {
        Config conf = smallInstanceConfig();
        conf.addMapConfig(new MapConfig("*").setBackupCount(1));
        conf.addDataConnectionConfig(pulsarDataConnectionConf("testPulsar", getServiceUrl(), pulsarContainer.getHttpServiceUrl(),
                                                              true));
        return conf;
    }

}
