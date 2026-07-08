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
package com.hazelcast.jet.cdc;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.config.Config;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastTestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hazelcast.jet.cdc.Operation.UNSPECIFIED;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static org.apache.commons.lang3.StringUtils.substring;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);
    private TestUtils() {
    }

    public static Config smallInstanceConfigWithCompactSerialization() {
        Config cfg = HazelcastTestSupport.smallInstanceConfig();
        addAllTypesAsCompactSerializable(cfg);
        return cfg;
    }

    public static void addAllTypesAsCompactSerializable(Config config) {
        HazelcastTestSupport.addToCompactSerializationAllowList(config, Customer.class, CustomerInfo.class,
                CustomerWithDesignation.class, CustomerWithDesignationInfo.class, Product.class, ProductInfo.class, Address.class,
                AddressInfo.class, Transaction.class, TransactionInfo.class, StronglyTypedTransactionInfo.class, Operation.class);
    }

    public static void executeSql(String jdbcUrl, String username, String password,
            String... s) {
        try (var connection = DriverManager.getConnection(jdbcUrl, username, password);
             var statement = connection.createStatement()) {
            for (String sql : s) {
                //noinspection SqlSourceToSinkFlow
                statement.addBatch(sql);
                System.out.println("Executing SQL: " + sql);
            }
            statement.executeBatch();
        } catch (SQLException e) {
            for (Throwable throwable : e) {
                LOG.error("Exception while executing sql statement", throwable);
            }
            throw new RuntimeException(e);
        }
    }

    public static @Nonnull Pipeline getPipeline(StreamSource<ChangeRecord> source,
                                                FunctionEx<ChangeRecord, String> tableFn) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withIngestionTimestamps()
                .peek(ChangeRecord::toJson)
                .filter(changeRecord -> changeRecord.operation() != UNSPECIFIED)
                .map(changeRecord -> {
                    assertThat(tableFn.apply(changeRecord)).isEqualToIgnoringCase("customers");
                    Operation operation = changeRecord.operation();
                    RecordPart value = changeRecord.value();
                    assert value != null;
                    Customer customer = value.toObject(Customer.class);
                    return new CustomerInfo(operation, customer);
                })
                .writeTo(Sinks.list("results"));
        pipeline.setPreserveOrder(true);
        return pipeline;
    }

    public static @Nonnull Pipeline getPipelineWithNewColumn(StreamSource<ChangeRecord> source,
            FunctionEx<ChangeRecord, String> tableFn) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withIngestionTimestamps()
                .filter(changeRecord -> changeRecord.operation() != UNSPECIFIED)
                .peek(ChangeRecord::toJson)
                .map(changeRecord -> {
                    assertThat(tableFn.apply(changeRecord)).isEqualTo("customers");
                    Operation operation = changeRecord.operation();
                    RecordPart value = changeRecord.value();
                    assert value != null;
                    CustomerWithDesignation customer = value.toObject(CustomerWithDesignation.class);
                    return new CustomerWithDesignationInfo(operation, customer);
                })
                .writeTo(Sinks.list("results"));
        pipeline.setPreserveOrder(true);
        return pipeline;
    }

    public static @Nonnull JobConfig standardConf() {
        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(300);
        config.setProcessingGuarantee(EXACTLY_ONCE);
        return config;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static Map<String, Object> mapFrom(Object o) throws IOException {
        if (o instanceof Map m) {
            return m;
        }
        return JsonUtil.mapFrom(o);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Customer {

        @JsonProperty("id")
        public int id;

        @JsonProperty("first_name")
        public String firstName;

        @JsonProperty("last_name")
        public String lastName;

        @JsonProperty("email")
        public String email;

        // ser-de
        @SuppressWarnings("unused")
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

    public record CustomerInfo(Operation op, Customer cust) implements Comparable<CustomerInfo> {
        static Comparator<CustomerInfo> comparator = Comparator.comparing((CustomerInfo i) -> {
                                                                   assert i.op.code() != null;
                                                                   return i.op.code();
                                                               })
                                                               .thenComparing(i -> i.cust.id);
        @Override
        public int compareTo(@Nonnull TestUtils.CustomerInfo o) {
            return comparator.compare(this, o);
        }
    }

    public static class CustomerWithDesignation extends Customer {

        @JsonProperty("designation")
        public String designation;

        // ser-de
        @SuppressWarnings("unused")
        CustomerWithDesignation() {
        }

        CustomerWithDesignation(int id, String firstName, String lastName, String email, String designation) {
            super(id, firstName, lastName, email);
            this.designation = designation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(email, firstName, id, lastName, designation);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            CustomerWithDesignation other = (CustomerWithDesignation) obj;
            return id == other.id
                    && Objects.equals(firstName, other.firstName)
                    && Objects.equals(lastName, other.lastName)
                    && Objects.equals(email, other.email)
                    && Objects.equals(designation, other.designation);
        }

        @Override
        public String toString() {
            return "CustomerWithDesignation {id=" + id + ", firstName=" + firstName
                    + ", lastName=" + lastName + ", email=" + email + ", designation=" + designation + '}';
        }

    }

    public record CustomerWithDesignationInfo(Operation op, CustomerWithDesignation cust) {
    }

    public static class Product {

        @JsonProperty("id")
        public int id;

        @JsonProperty("name")
        public String name;

        @JsonProperty("description")
        public String description;

        @JsonProperty("weight")
        public double weight;

        // ser-de
        @SuppressWarnings("unused")
        Product() {
        }

        Product(int id, String name, String description, double weight) {
            this.id = id;
            this.name = name;
            this.description = description;
            this.weight = weight;
        }

        @Override
        public int hashCode() {
            return Objects.hash(weight, name, id, description);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Product other = (Product) obj;
            return id == other.id
                    && Objects.equals(name, other.name)
                    && Objects.equals(description, other.description)
                    && Objects.equals(weight, other.weight);
        }

        @Override
        public String toString() {
            return "Product {id=" + id + ", name=" + name + ", description=" + description + ", weight=" + weight + '}';
        }

    }

    public record ProductInfo(Operation op, Product prod) {

    }

    public static class Address {

        @JsonProperty("id")
        public int id;

        @JsonProperty("customer_id")
        public int customerId;

        @JsonProperty("street")
        public String street;

        @JsonProperty("city")
        public String city;

        @JsonProperty("state")
        public String state;

        @JsonProperty("zip")
        public String zip;

        @JsonProperty("type")
        public String type;

        // ser-de
        @SuppressWarnings("unused")
        Address() {
        }

        Address(int id, int customerId, String street, String city, String state,
                String zip, String type) {
            this.id = id;
            this.customerId = customerId;
            this.street = street;
            this.city = city;
            this.state = state;
            this.zip = zip;
            this.type = type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, street, id, customerId, city, zip, type);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Address other = (Address) obj;
            return id == other.id
                    && Objects.equals(customerId, other.customerId)
                    && Objects.equals(street, other.street)
                    && Objects.equals(city, other.city)
                    && Objects.equals(state, other.state)
                    && Objects.equals(zip, other.zip)
                    && Objects.equals(type, other.type);
        }

        @Override
        public String toString() {
            return "Address {id=" + id + ", customerId=" + customerId
                    + ", street=" + street + ", city=" + city + ", state=" + state
                    + ", zip=" + zip + ", type=" + type + '}';
        }

    }

    public record AddressInfo(Operation op, Address addr) {

    }
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Transaction implements Comparable<Transaction> {
        private static final int EXAMPLE_PRECISION = "2024-08-04T01:30:00.777".length();
        private static final int EXAMPLE_DATE_PRECISION = "2024-08-04".length();

        public static final Comparator<Transaction> COMPARATOR = Comparator
                .comparing((Transaction t) -> t.tokenId)
                .thenComparing(t -> substring(t.txnDatetime, 0, EXAMPLE_PRECISION))
                .thenComparing(t -> substring(t.datetimeWithPrecision, 0, EXAMPLE_PRECISION))
                .thenComparing(t -> substring(t.processingDate, 0, EXAMPLE_DATE_PRECISION))
                .thenComparing(t -> t.time)
                .thenComparing(t -> t.createDatetime.substring(0, 10));

        @JsonProperty("ID")
        public int tokenId;

        @JsonProperty("TXN_DATETIME")
        public String txnDatetime;

        @JsonProperty("DATETIME_WITH_PRECISION")
        public String datetimeWithPrecision;

        @JsonProperty("DATE")
        public String processingDate;

        @JsonProperty("TIME")
        public int time;

        @JsonProperty("CREATE_DATETIME")
        public String createDatetime;

        // ser-de
        @SuppressWarnings("unused")
        Transaction() {
        }

        public Transaction(int tokenId, String txnDatetime, String datetimeWithPrecision, String processingDate,
                           int time, String createDatetime) {
            this.tokenId = tokenId;
            this.txnDatetime = txnDatetime;
            this.datetimeWithPrecision = datetimeWithPrecision;
            this.processingDate = processingDate;
            this.time = time;
            this.createDatetime = createDatetime;
        }

        @Override
        public int hashCode() {
            return Objects.hash(datetimeWithPrecision, time, processingDate, tokenId, txnDatetime);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Transaction other = (Transaction) obj;
            return tokenId == other.tokenId
                    && Objects.equals(txnDatetime, other.txnDatetime)
                    && Objects.equals(datetimeWithPrecision, other.datetimeWithPrecision)
                    && Objects.equals(processingDate, other.processingDate)
                    && Objects.equals(time, other.time);
        }

        @Override
        public String toString() {
            return "Transaction {tokenId=" + tokenId
                    + ", txnDatetime=" + substring(txnDatetime, 0, EXAMPLE_PRECISION)
                    + ", datetimeWithPrecision=" + substring(datetimeWithPrecision, 0, EXAMPLE_PRECISION)
                    + ", processingDate=" + processingDate
                    + ", time=" + time
                    + ", createDatetime=" + substring(createDatetime, 0, 10) + '}';
        }

        @Override
        public int compareTo(@Nonnull TestUtils.Transaction o) {
            return COMPARATOR.compare(this, o);
        }
    }

    public record TransactionInfo(Operation op, Transaction txn)  implements Comparable<TransactionInfo> {
        public static final Comparator<TransactionInfo> COMPARATOR = Comparator
                .comparing((TransactionInfo t) -> {
                    assert t.op.code() != null;
                    return t.op.code();
                })
                .thenComparing(TransactionInfo::txn);
        @Override
        public int compareTo(@Nonnull TestUtils.TransactionInfo o) {
            return COMPARATOR.compare(this, o);
        }

        public static void validateCreationDates(List<TransactionInfo> list) {
            assertThat(list).extracting(e -> e.txn.createDatetime).isNotNull();
            var map = list.stream().collect(Collectors.groupingBy(e -> e.txn().tokenId));
            for (Integer id : map.keySet()) {
                long distinctSize = map.get(id).stream().map(e -> e.txn().createDatetime).distinct().count();
                assertThat(distinctSize).isEqualTo(1L);
            }
        }
    }

    public record StronglyTypedTransactionInfo(Operation op, TransactionStronglyTyped txn) implements Comparable<StronglyTypedTransactionInfo> {
        public static final Comparator<StronglyTypedTransactionInfo> COMPARATOR = Comparator
                .comparing((StronglyTypedTransactionInfo t) -> {
                    assert t.op.code() != null;
                    return t.op.code();
                })
                .thenComparing(StronglyTypedTransactionInfo::txn);

        @Override
        public int compareTo(@Nonnull TestUtils.StronglyTypedTransactionInfo o) {
            return COMPARATOR.compare(this, o);
        }

        public static void validateCreationDates(List<StronglyTypedTransactionInfo> list) {
            assertThat(list).extracting(e -> e.txn.createDatetime).isNotNull();
            var map = list.stream().collect(Collectors.groupingBy(e -> e.txn().tokenId));
            for (Integer id : map.keySet()) {
                long distinctSize = map.get(id).stream().map(e -> e.txn().createDatetime).distinct().count();
                assertThat(distinctSize).isEqualTo(1L);
            }
        }
    }

}
