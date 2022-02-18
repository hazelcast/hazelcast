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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.jet.cdc.AbstractCdcIntegrationTest;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

@Category({ParallelJVMTest.class, IgnoreInJenkinsOnWindows.class})
@RunWith(HazelcastSerialClassRunner.class)
public abstract class AbstractPostgresCdcIntegrationTest extends AbstractCdcIntegrationTest {

    public static final DockerImageName DOCKER_IMAGE = DockerImageName.parse("debezium/example-postgres:1.3")
            .asCompatibleSubstituteFor("postgres");

    protected static final String DATABASE_NAME = "postgres";
    protected static final String REPLICATION_SLOT_NAME = "debezium";

    private static final String SCHEMA = "inventory";

    @Rule
    public PostgreSQLContainer<?> postgres = namedTestContainer(
            new PostgreSQLContainer<>(DOCKER_IMAGE)
                    .withDatabaseName("postgres")
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .withConnectTimeoutSeconds(300)
                    .withStartupTimeoutSeconds(300)
    );

    protected PostgresCdcSources.Builder sourceBuilder(String name) {
        return PostgresCdcSources.postgres(name)
                .setDatabaseAddress(postgres.getContainerIpAddress())
                .setDatabasePort(postgres.getMappedPort(POSTGRESQL_PORT))
                .setDatabaseUser("postgres")
                .setDatabasePassword("postgres")
                .setDatabaseName(DATABASE_NAME)
                .setReconnectBehavior(RetryStrategies.indefinitely(1000));
    }

    protected final void createSchema(String schema) throws SQLException {
        try (Connection connection = getConnection(postgres)) {
            connection.createStatement().execute("CREATE SCHEMA " + schema);
        }
    }

    protected final void executeBatch(String... sqlCommands) throws SQLException {
        try (Connection connection = getConnection(postgres)) {
            connection.setSchema(SCHEMA);
            Statement statement = connection.createStatement();
            for (String sql : sqlCommands) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
        }
    }

    static Connection getConnection(PostgreSQLContainer<?> postgres) throws SQLException {
        return getPostgreSqlConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    protected static class Customer implements Serializable {

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

        public Customer(int id, String firstName, String lastName, String email) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.email = email;
        }

        public int getId() {
            return id;
        }

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public String getEmail() {
            return email;
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

    protected static class Order implements Serializable {

        @JsonProperty("id")
        public int orderNumber;

        @JsonProperty("order_date")
        public Date orderDate;

        @JsonProperty("purchaser")
        public int purchaser;

        @JsonProperty("quantity")
        public int quantity;

        @JsonProperty("product_id")
        public int productId;

        Order() {
        }

        public Order(int orderNumber, Date orderDate, int purchaser, int quantity, int productId) {
            this.orderNumber = orderNumber;
            this.orderDate = orderDate;
            this.purchaser = purchaser;
            this.quantity = quantity;
            this.productId = productId;
        }

        public void setOrderDate(Date orderDate) {
            long days = orderDate.getTime();
            this.orderDate = new Date(TimeUnit.DAYS.toMillis(days));
        }

        public int getOrderNumber() {
            return orderNumber;
        }

        public Date getOrderDate() {
            return orderDate;
        }

        public int getPurchaser() {
            return purchaser;
        }

        public int getQuantity() {
            return quantity;
        }

        public int getProductId() {
            return productId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderNumber, orderDate, purchaser, quantity, productId);
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
                    && Objects.equals(purchaser, other.purchaser)
                    && Objects.equals(quantity, other.quantity)
                    && Objects.equals(productId, other.productId);
        }

        @Override
        public String toString() {
            return "Order {orderNumber=" + orderNumber + ", orderDate=" + orderDate + ", purchaser=" + purchaser +
                    ", quantity=" + quantity + ", productId=" + productId + '}';
        }
    }

}
