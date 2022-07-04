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

package com.hazelcast.jet.cdc.postgres;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.RecordPart;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

@Category(NightlyTest.class)
public class MultiTableCacheIntegrationTest extends AbstractPostgresCdcIntegrationTest {

    private static final int MAX_CONCURRENT_OPERATIONS = 1;
    private static final String CACHE = "cache";
    private static final int REPEATS = 1000;

    @Test
    public void ordersOfCustomers() throws Exception {
        StreamSource<ChangeRecord> source = sourceBuilder("source")
                .setTableWhitelist("inventory.customers", "inventory.orders")
                .build();

        Pipeline pipeline = Pipeline.create();
        StreamStage<ChangeRecord> allRecords = pipeline.readFrom(source)
                .withNativeTimestamps(0);

        allRecords.filter(r -> r.table().equals("customers"))
                .apply(this::fixOrdering)
                .writeTo(Sinks.mapWithEntryProcessor(MAX_CONCURRENT_OPERATIONS, CACHE,
                        record -> (Integer) record.key().toMap().get("id"),
                        CustomerEntryProcessor::new
                ));

        allRecords.filter(r -> r.table().equals("orders"))
                .apply(this::fixOrdering)
                .writeTo(Sinks.mapWithEntryProcessor(MAX_CONCURRENT_OPERATIONS, CACHE,
                        record -> (Integer) record.value().toMap().get("purchaser"),
                        OrderEntryProcessor::new
                ));

        // when
        HazelcastInstance hz = createHazelcastInstances(1)[0];
        Job job = hz.getJet().newJob(pipeline);
        //then
        Map<Integer, OrdersOfCustomer> expected = toMap(
        new OrdersOfCustomer(
                new Customer(1001, "Sally", "Thomas", "sally.thomas@acme.com"),
                new Order(10001, new Date(1452902400000L), 1001, 1, 102)),
        new OrdersOfCustomer(
                new Customer(1002, "George", "Bailey", "gbailey@foobar.com"),
                new Order(10002, new Date(1452988800000L) , 1002, 2, 105),
                new Order(10003, new Date(1455840000000L), 1002, 2, 106)),
        new OrdersOfCustomer(
                new Customer(1003, "Edward", "Walker", "ed@walker.com"),
                new Order(10004, new Date(1456012800000L), 1003, 1, 107)),
        new OrdersOfCustomer(
                new Customer(1004, "Anne", "Kretchmar", "annek@noanswer.org")));
        assertEqualsEventually(() -> getIMapContent(hz, CACHE), expected);

        //when
        List<String> batch = new ArrayList<>();
        for (int i = 1; i <= REPEATS; i++) {
            batch.add("UPDATE customers SET first_name='Anne" + i + "' WHERE id=1004");

            batch.add("INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')");
            batch.add("DELETE FROM customers WHERE id=1005");

            batch.add("UPDATE orders SET quantity='" + i + "' WHERE id=10004");

            batch.add("DELETE FROM orders WHERE id=10003");
            batch.add("INSERT INTO orders VALUES (10003, '2016-02-19', 1002, 2, 106)");
        }
        executeBatch(batch.toArray(new String[0]));

        //then
        expected = toMap(
                new OrdersOfCustomer(
                        new Customer(1001, "Sally", "Thomas", "sally.thomas@acme.com"),
                        new Order(10001, new Date(1452902400000L), 1001, 1, 102)),
                new OrdersOfCustomer(
                        new Customer(1002, "George", "Bailey", "gbailey@foobar.com"),
                        new Order(10002, new Date(1452988800000L) , 1002, 2, 105),
                        new Order(10003, new Date(1455840000000L), 1002, 2, 106)),
                new OrdersOfCustomer(
                        new Customer(1003, "Edward", "Walker", "ed@walker.com"),
                        new Order(10004, new Date(1456012800000L), 1003, REPEATS, 107)),
                new OrdersOfCustomer(
                        new Customer(1004, "Anne" + REPEATS, "Kretchmar", "annek@noanswer.org")));
        expected.put(1005, new OrdersOfCustomer());
        assertEqualsEventually(() -> getIMapContent(hz, CACHE), expected);
    }

    private StreamStage<ChangeRecord> fixOrdering(StreamStage<ChangeRecord> input) {
        return input
                .groupingKey(ChangeRecord::key)
                .mapStateful(
                        TimeUnit.SECONDS.toMillis(10),
                        () -> new Sequence(0, 0),
                        (lastSequence, key, record) -> {
                            long source = record.sequenceSource();
                            long sequence = record.sequenceValue();
                            if (lastSequence.update(source, sequence)) {
                                return record;
                            }
                            return null;
                        },
                        (TriFunction<Sequence, RecordPart, Long, ChangeRecord>) (sequence, recordPart, aLong) -> null);
    }

    @Nonnull
    private static Map<Integer, OrdersOfCustomer> getIMapContent(HazelcastInstance hz, String name) {
        return new HashMap<>(hz.getMap(name));
    }

    @Nonnull
    private static Map<Integer, OrdersOfCustomer> toMap(OrdersOfCustomer... ordersOfCustomers) {
        return Arrays.stream(ordersOfCustomers).collect(Collectors.toMap(
                orders -> orders.getCustomer().getId(), Function.identity()));
    }

    private static class Sequence {

        private long source;
        private long sequence;

        Sequence(long source, long sequence) {
            this.source = source;
            this.sequence = sequence;
        }

        boolean update(long source, long sequence) {
            if (this.source != source) { //sequence source changed for key
                this.source = source;
                this.sequence = sequence;
                return true;
            }

            if (this.sequence < sequence) { //sequence is newer than previous for key
                this.sequence = sequence;
                return true;
            }

            return false;
        }

        @Override
        public String toString() {
            return "source=" + source + ", sequence=" + sequence;
        }
    }

    private static class OrdersOfCustomer implements Serializable {

        private final Map<Integer, Order> orders;
        private Customer customer;

        OrdersOfCustomer() {
            this.customer = null;
            this.orders = new HashMap<>();
        }

        OrdersOfCustomer(Customer customer, Order... orders) {
            this.customer = customer;
            this.orders = Arrays.stream(orders).collect(Collectors.toMap(Order::getOrderNumber, Function.identity()));
        }

        public Customer getCustomer() {
            return customer;
        }

        public void setCustomer(Customer customer) {
            this.customer = customer;
        }

        public void deleteOrder(Order order) {
            orders.remove(order.getOrderNumber());
        }

        public void addOrUpdateOrder(Order order) {
            orders.put(order.getOrderNumber(), order);
        }

        @Override
        public int hashCode() {
            return Objects.hash(customer, orders);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            OrdersOfCustomer other = (OrdersOfCustomer) obj;
            return Objects.equals(customer, other.customer)
                    && Objects.equals(orders, other.orders);
        }

        @Override
        public String toString() {
            return String.format("Customer: %s, Orders: %s", customer, orders);
        }
    }

    private static class CustomerEntryProcessor implements EntryProcessor<Integer, OrdersOfCustomer, Object> {

        private final ChangeRecord record;

        CustomerEntryProcessor(ChangeRecord record) {
            this.record = record;
        }

        @Override
        public Object process(Entry<Integer, OrdersOfCustomer> entry) {
            try {
                boolean deletion = Operation.DELETE.equals(record.operation());
                OrdersOfCustomer value = entry.getValue();
                if (deletion) {
                    if (value != null) {
                        value.setCustomer(null);
                    }
                } else {
                    if (value == null) {
                        value = new OrdersOfCustomer();
                    }
                    value.setCustomer(record.value().toObject(Customer.class));
                }
                entry.setValue(value);
            } catch (ParsingException e) {
                throw rethrow(e);
            }
            return null;
        }
    }

    private static class OrderEntryProcessor implements EntryProcessor<Integer, OrdersOfCustomer, Object> {

        private final ChangeRecord record;

        OrderEntryProcessor(ChangeRecord record) {
            this.record = record;
        }

        @Override
        public Object process(Entry<Integer, OrdersOfCustomer> entry) {
            try {
                boolean deletion = Operation.DELETE.equals(record.operation());
                OrdersOfCustomer value = entry.getValue();
                if (deletion) {
                    if (value != null) {
                        value.deleteOrder(record.value().toObject(Order.class));
                    }
                } else {
                    if (value == null) {
                        value = new OrdersOfCustomer();
                    }
                    value.addOrUpdateOrder(record.value().toObject(Order.class));
                }
                entry.setValue(value);
            } catch (ParsingException e) {
                throw rethrow(e);
            }
            return null;
        }
    }

}
