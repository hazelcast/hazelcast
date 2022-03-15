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

package com.hazelcast.jet.cdc.mysql;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.RecordPart;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.Util.entry;

public class MySqlCdcIntegrationTest extends AbstractMySqlCdcIntegrationTest {

    @Test
    @Category(QuickTest.class)
    public void customers() throws Exception {
        // given
        List<String> expectedRecords = Arrays.asList(
                "1001/0:INSERT:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                "1002/0:INSERT:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                "1003/0:INSERT:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                "1004/0:INSERT:Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}",
                "1004/1:UPDATE:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}",
                "1005/0:INSERT:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}",
                "1005/1:DELETE:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}"
        );

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source("customers"))
                .withNativeTimestamps(0)
                .<ChangeRecord>customTransform("filter_timestamps", filterTimestampsProcessorSupplier())
                .groupingKey(record -> (Integer) record.key().toMap().get("id"))
                .mapStateful(
                        LongAccumulator::new,
                        (accumulator, customerId, record) -> {
                            long count = accumulator.get();
                            accumulator.add(1);
                            Operation operation = record.operation();
                            RecordPart value = record.value();
                            Customer customer = value.toObject(Customer.class);
                            return entry(customerId + "/" + count, operation + ":" + customer);
                        })
                .setLocalParallelism(1)
                .writeTo(Sinks.map("results"));

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);

        //then
        assertEqualsEventually(() -> hz.getMap("results").size(), 4);

        //when
        try (Connection connection = getConnection(mysql, "inventory")) {
            Statement statement = connection.createStatement();
            statement.addBatch("UPDATE customers SET first_name='Anne Marie' WHERE id=1004");
            statement.addBatch("INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')");
            statement.addBatch("DELETE FROM customers WHERE id=1005");
            statement.executeBatch();
        }

        //then
        try {
            assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("results")), expectedRecords);
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    @Test
    @Category(NightlyTest.class)
    public void orders() {
        // given
        List<String> expectedRecords = Arrays.asList(
                "10001/0:INSERT:Order {orderNumber=10001, orderDate=" + new Date(1452902400000L) +
                        ", quantity=1, productId=102}",
                "10002/0:INSERT:Order {orderNumber=10002, orderDate=" + new Date(1452988800000L) +
                        ", quantity=2, productId=105}",
                "10003/0:INSERT:Order {orderNumber=10003, orderDate=" + new Date(1455840000000L) +
                        ", quantity=2, productId=106}",
                "10004/0:INSERT:Order {orderNumber=10004, orderDate=" + new Date(1456012800000L) +
                        ", quantity=1, productId=107}"
        );

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source("orders"))
                .withoutTimestamps()
                .groupingKey(MySqlCdcIntegrationTest::getOrderNumber)
                .mapStateful(
                        LongAccumulator::new,
                        (accumulator, orderId, record) -> {
                            long count = accumulator.get();
                            accumulator.add(1);
                            Operation operation = record.operation();
                            RecordPart value = record.value();
                            Order order = value.toObject(Order.class);
                            return entry(orderId + "/" + count, operation + ":" + order);
                        })
                .setLocalParallelism(1)
                .writeTo(Sinks.map("results"));

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);

        //then
        try {
            assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("results")), expectedRecords);
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    @Test
    @Category(NightlyTest.class)
    public void restart() throws Exception {
        // given
        List<String> expectedRecords = Arrays.asList(
                "1004/1:UPDATE:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}",
                "1005/0:INSERT:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}",
                "1005/1:DELETE:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}"
        );

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source("customers"))
                .withNativeTimestamps(0)
                .<ChangeRecord>customTransform("filter_timestamps", filterTimestampsProcessorSupplier())
                .groupingKey(record -> (Integer) record.key().toMap().get("id"))
                .mapStateful(
                        LongAccumulator::new,
                        (accumulator, customerId, record) -> {
                            long count = accumulator.get();
                            accumulator.add(1);
                            Operation operation = record.operation();
                            RecordPart value = record.value();
                            Customer customer = value.toObject(Customer.class);
                            return entry(customerId + "/" + count, operation + ":" + customer);
                        })
                .setLocalParallelism(1)
                .writeTo(Sinks.map("results"));


        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        JobConfig jobConfig = new JobConfig().setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        Job job = hz.getJet().newJob(pipeline, jobConfig);
        JetTestSupport.assertJobStatusEventually(job, JobStatus.RUNNING);
        assertEqualsEventually(() -> hz.getMap("results").size(), 4);

        //then
        hz.getMap("results").destroy();

        //when
        assertEqualsEventually(() -> hz.getMap("results").size(), 0);

        //then
        job.restart();

        //when
        JetTestSupport.assertJobStatusEventually(job, JobStatus.RUNNING);

        //then update a record
        try (Connection connection = getConnection(mysql, "inventory")) {
            Statement statement = connection.createStatement();
            statement.addBatch("UPDATE customers SET first_name='Anne Marie' WHERE id=1004");
            statement.addBatch("INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')");
            statement.addBatch("DELETE FROM customers WHERE id=1005");
            statement.executeBatch();
        }

        //then
        try {
            assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("results")), expectedRecords);
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    @Test
    @Category(NightlyTest.class)
    public void cdcMapSink() throws Exception {
        // given
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source("customers"))
                .withNativeTimestamps(0)
                .writeTo(CdcSinks.map("cache",
                        r -> r.key().toMap().get("id"),
                        r -> r.value().toObject(Customer.class).toString()));


        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        JobConfig jobConfig = new JobConfig().setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        Job job = hz.getJet().newJob(pipeline, jobConfig);
        JetTestSupport.assertJobStatusEventually(job, JobStatus.RUNNING);
        //then
        assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("cache")),
                Arrays.asList(
                        "1001:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                        "1002:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                        "1003:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                        "1004:Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}"
                )
        );

        //when
        job.restart();
        JetTestSupport.assertJobStatusEventually(job, JobStatus.RUNNING);
        try (Connection connection = getConnection(mysql, "inventory")) {
            Statement statement = connection.createStatement();
            statement.addBatch("UPDATE customers SET first_name='Anne Marie' WHERE id=1004");
            statement.addBatch("INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')");
            statement.executeBatch();
        }
        //then
        assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("cache")),
                Arrays.asList(
                        "1001:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                        "1002:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                        "1003:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                        "1004:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}",
                        "1005:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}"
                )
        );

        //when
        try (Connection connection = getConnection(mysql, "inventory")) {
            connection.createStatement().execute("DELETE FROM customers WHERE id=1005");
        }
        //then
        assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("cache")),
                Arrays.asList(
                        "1001:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                        "1002:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                        "1003:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                        "1004:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}"
                )
        );
    }

    @Nonnull
    private StreamSource<ChangeRecord> source(String tableName) {
        return sourceBuilder(tableName)
                .setDatabaseWhitelist("inventory")
                .setTableWhitelist("inventory." + tableName)
                .build();
    }

    private static int getOrderNumber(ChangeRecord record) throws ParsingException {
        //pick random method for extracting ID in order to test all code paths
        boolean primitive = ThreadLocalRandom.current().nextBoolean();
        if (primitive) {
            return (Integer) record.key().toMap().get("order_number");
        } else {
            return record.key().toObject(OrderPrimaryKey.class).id;
        }
    }

    private static class Customer {

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

    private static class Order {

        @JsonProperty("order_number")
        public int orderNumber;

        @JsonProperty("order_date")
        public Date orderDate;

        @JsonProperty("quantity")
        public int quantity;

        @JsonProperty("product_id")
        public int productId;

        Order() {
        }

        public void setOrderDate(Date orderDate) {
            long days = orderDate.getTime();
            this.orderDate = new Date(TimeUnit.DAYS.toMillis(days));
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderNumber, orderDate, quantity, productId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Order other = (Order) obj;
            return orderNumber == other.orderNumber
                    && Objects.equals(orderDate, other.orderDate)
                    && Objects.equals(quantity, other.quantity)
                    && Objects.equals(productId, other.productId);
        }

        @Override
        public String toString() {
            return "Order {orderNumber=" + orderNumber + ", orderDate=" + orderDate + ", quantity=" + quantity +
                    ", productId=" + productId + '}';
        }

    }

    private static class OrderPrimaryKey {

        @JsonProperty("order_number")
        public int id;

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            OrderPrimaryKey other = (OrderPrimaryKey) obj;
            return id == other.id;
        }

        @Override
        public String toString() {
            return "OrderPrimaryKey {id=" + id + '}';
        }
    }
}
