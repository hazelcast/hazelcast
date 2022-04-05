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

package com.hazelcast.jet.cdc;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.dockerjava.api.DockerClient;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.map.IMap;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({IgnoreInJenkinsOnWindows.class})
public class AbstractCdcIntegrationTest extends JetTestSupport {

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void beforeClassCheckDocker() {
        assumeDockerEnabled();
    }

    @Nonnull
    protected static List<String> mapResultsToSortedList(IMap<?, ?> map) {
        return map.entrySet().stream()
                .map(e -> e.getKey() + ":" + e.getValue())
                .sorted().collect(Collectors.toList());
    }

    @Nonnull
    protected static void assertMatch(List<String> expectedPatterns, List<String> actualValues) {
        assertEquals(expectedPatterns.size(), actualValues.size());
        for (int i = 0; i < expectedPatterns.size(); i++) {
            String pattern = expectedPatterns.get(i);
            String value = actualValues.get(i);
            assertTrue(value.matches(pattern));
        }
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    protected static ProcessorMetaSupplier filterTimestampsProcessorSupplier() {
        /* Trying to make sure that items on the stream have native
         * timestamps. All records should be processed in a short amount
         * of time by Jet, so there is no reason why the difference
         * between their event times and the current time on processing
         * should be significantly different. It is a hack, but it does
         * help detect cases when we don't set useful timestamps at all.*/
        SupplierEx<Processor> supplierEx = Processors.filterP(o -> {
            long timestamp = ((JetEvent<Integer>) o).timestamp();
            long diff = System.currentTimeMillis() - timestamp;
            return diff < TimeUnit.SECONDS.toMillis(3);
        });
        return ProcessorMetaSupplier.preferLocalParallelismOne(supplierEx);
    }

    protected static class TableRow {

        @JsonProperty("id")
        public int id;

        @JsonProperty("value_1")
        public String value1;

        @JsonProperty("value_2")
        public String value2;

        @JsonProperty("value_3")
        public String value3;

        TableRow() {
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, value1, value2, value3);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TableRow other = (TableRow) obj;
            return id == other.id
                    && Objects.equals(value1, other.value1)
                    && Objects.equals(value2, other.value2)
                    && Objects.equals(value3, other.value3);
        }

        @Override
        public String toString() {
            return "TableRow {id=" + id + ", value1=" + value1 + ", value2=" + value2 + ", value3=" + value3 + '}';
        }
    }

    protected static int fixPortBinding(GenericContainer<?> container, Integer defaultPort) {
        int port = container.getMappedPort(defaultPort);
        container.setPortBindings(Collections.singletonList(port + ":" + defaultPort));
        return port;
    }

    /**
     * Stops a container via the `docker stop` command. Necessary because the
     * {@code stop()} method of test containers is implemented via `docker kill`
     * and this matters for some tests.
     */
    protected static void stopContainer(GenericContainer<?> container) {
        DockerClient dockerClient = DockerClientFactory.instance().client();
        dockerClient.stopContainerCmd(container.getContainerId()).exec();

        container.stop(); //allow the resource reaper to clean its registries
    }

    protected <T> T namedTestContainer(GenericContainer<?> container) {
        if (container instanceof JdbcDatabaseContainer) {
            container = ((JdbcDatabaseContainer) container)
                    .withConnectTimeoutSeconds(300)
                    .withStartupTimeoutSeconds(300);
        }
        return (T) container
                .withStartupAttempts(5)
                .withCreateContainerCmdModifier(createContainerCmd -> {
            String source = AbstractCdcIntegrationTest.this.getClass().getSimpleName() + "." + testName.getMethodName()
                .replaceAll("\\[|\\]|\\/| ", "_");
            createContainerCmd.withName(source + "___" + randomName());
        });
    }

    protected static Connection getMySqlConnection(String url, String user, String password) throws SQLException {
        Properties properties = new Properties();
        properties.put("user", user);
        properties.put("password", password);
        properties.put("useSSL", "false");

        return DriverManager.getConnection(url, properties);
    }

    protected static Connection getPostgreSqlConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

}
