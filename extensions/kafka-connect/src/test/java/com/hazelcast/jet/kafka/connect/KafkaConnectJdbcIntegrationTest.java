/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.connect;

import com.hazelcast.config.Config;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.apache.kafka.connect.data.Values;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.MySQLContainer;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static com.hazelcast.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({SlowTest.class, ParallelJVMTest.class})
public class KafkaConnectJdbcIntegrationTest extends JetTestSupport {
    public static final String USERNAME = "mysql";
    public static final String PASSWORD = "mysql";

    @ClassRule
    public static final MySQLContainer<?> mysql = new MySQLContainer<>().withUsername(USERNAME).withPassword(PASSWORD);

    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");

    private static final int ITEM_COUNT = 10_000;

    private static final String CONNECTOR_URL = "https://repository.hazelcast.com/download"
            + "/tests/confluentinc-kafka-connect-jdbc-10.6.3.zip";



    @Test
    public void testReadFromJdbcConnector() throws Exception {
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "confluentinc-kafka-connect-jdbc");
        randomProperties.setProperty("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        randomProperties.setProperty("mode", "incrementing");
        randomProperties.setProperty("tasks.max", "1");
        String connectionUrl = mysql.getJdbcUrl();
        randomProperties.setProperty("connection.url", connectionUrl);
        randomProperties.setProperty("connection.user", USERNAME);
        randomProperties.setProperty("connection.password", PASSWORD);
        randomProperties.setProperty("incrementing.column.name", "id");
        try (Connection conn = DriverManager.getConnection(connectionUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(10))");
            for (int i = 0; i < ITEM_COUNT; i++) {
                stmt.execute(String.format("INSERT INTO items VALUES(%d, 'name-%d')", i, i));
            }
        }

        Pipeline pipeline = Pipeline.create();
        StreamStage<String> streamStage = pipeline.readFrom(KafkaConnectSources.connect(randomProperties))
                .withoutTimestamps()
                .map(record -> Values.convertToString(record.valueSchema(), record.value()));
        streamStage.writeTo(Sinks.logger());
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(new URL(CONNECTOR_URL));

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }


}
