/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.collection.IList;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.map.IMap;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.cdc.Operation.INSERT;
import static com.hazelcast.jet.cdc.Operation.SYNC;
import static com.hazelcast.jet.cdc.Operation.UNSPECIFIED;
import static com.hazelcast.jet.cdc.Operation.UPDATE;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.assertj.core.api.Assertions.assertThat;
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

    protected static void assertMatch(List<String> expectedPatterns, List<String> actualValues) {
        assertEquals(expectedPatterns.size(), actualValues.size());
        for (int i = 0; i < expectedPatterns.size(); i++) {
            String pattern = expectedPatterns.get(i);
            String value = actualValues.get(i);
            assertTrue("\n" + value + " \nmust match \n" + pattern, value.matches(pattern));
        }
    }

    protected static @NotNull Pipeline getPipeline(StreamSource<ChangeRecord> source, IList<CustomerInfo> results) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withIngestionTimestamps()
                .filter(changeRecord -> changeRecord.operation() != UNSPECIFIED)
                .map(changeRecord -> {
                    Operation operation = changeRecord.operation();
                    RecordPart value = changeRecord.value();
                    Customer customer = value.toObject(Customer.class);
                    return new CustomerInfo(operation, customer);
                })
                .writeTo(Sinks.list(results));
        pipeline.setPreserveOrder(true);
        return pipeline;
    }

    protected static @NotNull List<CustomerInfo> addedRecords() {
        return List.of(
                new CustomerInfo(UPDATE, new Customer(1004, "Anne Marie", "Kretchmar", "annek@noanswer.org")),
                new CustomerInfo(INSERT, new Customer(1005, "Jason", "Bourne", "jason@bourne.org")),
                new CustomerInfo(DELETE, new Customer(1005, "Jason", "Bourne", "jason@bourne.org"))
        );
    }

    protected static void assertContainsAddedRecords(IList<CustomerInfo> results) {
        var expectedRecords = addedRecords();
        assertTrueEventually(() -> {
            var subset = results.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset).containsExactlyInAnyOrderElementsOf(expectedRecords);
        });
    }

    protected static void assertContainsAddedRecordsAfterRestart(IList<CustomerInfo> results) {
        var expectedRecords = new ArrayList<>(addedRecords());
        expectedRecords.add(new CustomerInfo(INSERT,
                new Customer(1007, "Darth", "Vader", "vader@empire.com")));
        assertTrueEventually(() -> {
            var subset = results.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset).containsExactlyInAnyOrderElementsOf(expectedRecords);
        });
    }

    protected record CustomerInfo(Operation op, Customer cust) {
    }

    protected static class Customer {

        @JsonProperty("id")
        public int id;

        @JsonProperty("first_name")
        public String firstName;

        @JsonProperty("last_name")
        public String lastName;

        @JsonProperty("email")
        public String email;

        Customer() {
        }

        Customer(int id, String firstName, String lastName, String email) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.email = email;
        }

        @Override
        public int hashCode() {
            return Objects.hash(email, firstName, id, lastName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Customer other = (Customer) obj;
            return id == other.id
                    && Objects.equals(firstName, other.firstName)
                    && Objects.equals(lastName, other.lastName)
                    && Objects.equals(email, other.email);
        }

        @Override
        public String toString() {
            return "Customer {id=" + id + ", firstName=" + firstName + ", lastName=" + lastName + ", email=" + email + '}';
        }

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
        //noinspection resource
        DockerClient dockerClient = DockerClientFactory.instance().client();
        dockerClient.stopContainerCmd(container.getContainerId()).exec();

        container.stop(); //allow the resource reaper to clean its registries
    }

    protected <T> T namedTestContainer(GenericContainer<?> container) {
        GenericContainer<?> cont = container;
        if (container instanceof JdbcDatabaseContainer<?> jdbcCont) {
            cont = jdbcCont
                    .withConnectTimeoutSeconds(300)
                    .withStartupTimeoutSeconds(300);
        }
        //noinspection unchecked
        return (T) cont
                .withStartupAttempts(5)
                .withCreateContainerCmdModifier(createContainerCmd -> {
                    final String methodName = testName.getMethodName().replaceAll("[\\[\\]/ ]", "_");
                    final String source = AbstractCdcIntegrationTest.this.getClass().getSimpleName() + "." + methodName;
                    createContainerCmd.withName(source + "___" + randomName());
                });
    }

}
