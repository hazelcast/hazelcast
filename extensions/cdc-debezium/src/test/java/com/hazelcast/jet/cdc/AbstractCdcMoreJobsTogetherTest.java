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

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.cdc.TestUtils.Address;
import com.hazelcast.jet.cdc.TestUtils.AddressInfo;
import com.hazelcast.jet.cdc.TestUtils.Customer;
import com.hazelcast.jet.cdc.TestUtils.CustomerInfo;
import com.hazelcast.jet.cdc.TestUtils.Product;
import com.hazelcast.jet.cdc.TestUtils.ProductInfo;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.jet.cdc.TestNotificationChannel.waitForSnapshotEnd;
import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.cdc.Operation.INSERT;
import static com.hazelcast.jet.cdc.Operation.SYNC;
import static com.hazelcast.jet.cdc.Operation.UNSPECIFIED;
import static com.hazelcast.jet.cdc.Operation.UPDATE;
import static com.hazelcast.jet.cdc.TestUtils.executeSql;
import static com.hazelcast.jet.cdc.TestUtils.smallInstanceConfigWithCompactSerialization;
import static com.hazelcast.jet.cdc.TestUtils.standardConf;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class, IgnoreInJenkinsOnWindows.class})
public abstract class AbstractCdcMoreJobsTogetherTest<C extends GenericContainer<?>> extends JetTestSupport {

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void beforeClassCheckDocker() {
        assumeDockerEnabled();
    }

    /**
     * Extend in subclass if any special treatment is needed.
     */
    protected void prepareDatabase(C container) {
    }

    @Nonnull
    protected abstract C getContainer();

    @Nonnull
    protected abstract DebeziumCdcSources.Builder<ChangeRecord> conf(C container, String table, Map<String, String> options);

    @Test
    public void moreJobsReadingSameTable() {
        int jobsCount = 4;
        try (C container = getContainer()) {
            container.start();
            prepareDatabase(container);

            HazelcastInstance hz = createHazelcastInstances(smallInstanceConfigWithCompactSerialization(), 2)[0];

            List<String> uuids = new ArrayList<>();
            for (int i = 0; i < jobsCount; i++) {
                // given
                Map<String, String> map = new HashMap<>();
                map.put("clientId", Integer.toString(200 + i));
                String serverId = Integer.toString(190000 + i);
                map.put("serverId", serverId);
                String uuid = UUID.randomUUID().toString();
                map.put("uuid", uuid);
                uuids.add(uuid);

                Pipeline pipeline = getPipeline(conf(container, "inventory.customers", map).build(),
                        ChangeRecord::table, "customers", "results" + i,
                        (value, operation) -> {
                            Customer customer = value.toObject(Customer.class);
                            return new CustomerInfo(operation, customer);
                        });

                // when
                Job job = hz.getJet().newJob(pipeline, standardConf());
                assertThat(job).eventuallyHasStatus(RUNNING);

            }
            // then
            for (String id : uuids) {
                waitForSnapshotEnd(id);
            }

            //when
            performSetOfChangesInCustomers(container);

            //then
            for (int i = 0; i < jobsCount; i++) {
                IList<CustomerInfo> results = hz.getList("results" + i);
                assertContainsAddedCustomersRecords(results);
            }
        }

    }

    @Test
    public void jobsReadingDifferentTables() throws Exception {
        try (C container = getContainer()) {
            container.start();
            prepareDatabase(container);

            HazelcastInstance hz = createHazelcastInstances(smallInstanceConfigWithCompactSerialization(), 2)[0];

            // given
            Map<String, String> map1 = new HashMap<>();
            map1.put("clientId", Integer.toString(2000));
            map1.put("serverId", Integer.toString(190000));
            String uuidName = "uuid";
            map1.put(uuidName, UUID.randomUUID().toString());

            Pipeline pipeline1 = getPipeline(conf(container, "inventory.customers", map1).build(),
                    ChangeRecord::table, "customers", "resultsCustomers",
                    (value, operation) -> {
                        Customer customer = value.toObject(Customer.class);
                        return new CustomerInfo(operation, customer);
                    });

            Map<String, String> map2 = new HashMap<>();
            map2.put("clientId", Integer.toString(2001));
            map2.put("serverId", Integer.toString(190001));
            map2.put(uuidName, UUID.randomUUID().toString());

            Pipeline pipeline2 = getPipeline(conf(container, "inventory.products", map2).build(),
                    ChangeRecord::table, "products", "resultsProducts",
                    (value, operation) -> {
                        Product product = value.toObject(Product.class);
                        return new ProductInfo(operation, product);
                    });

            Map<String, String> map3 = new HashMap<>();
            map3.put("clientId", Integer.toString(2002));
            String serverId = Integer.toString(190002);
            map3.put("serverId", serverId);
            map3.put(uuidName, UUID.randomUUID().toString());

            Pipeline pipeline3 = getPipeline(conf(container, "inventory.addresses", map3).build(),
                    ChangeRecord::table, "addresses", "resultsAddresses",
                    (value, operation) -> {
                        Address address = value.toObject(Address.class);
                        return new AddressInfo(operation, address);
                    });

            // when
            Job job1 = hz.getJet().newJob(pipeline1, standardConf());
            Job job2 = hz.getJet().newJob(pipeline2, standardConf());
            Job job3 = hz.getJet().newJob(pipeline3, standardConf());
            assertThat(job1).eventuallyHasStatus(RUNNING);
            assertThat(job2).eventuallyHasStatus(RUNNING);
            assertThat(job3).eventuallyHasStatus(RUNNING);

            // then
            IList<CustomerInfo> results1 = hz.getList("resultsCustomers");
            IList<ProductInfo> results2 = hz.getList("resultsProducts");
            IList<AddressInfo> results3 = hz.getList("resultsAddresses");
            List.of(map1.get(uuidName), map2.get(uuidName), map3.get(uuidName))
                .forEach(TestNotificationChannel::waitForSnapshotEnd);

            //when
            performSetOfChangesInCustomers(container);
            performSetOfChangesInProducts(container);
            performSetOfChangesInAddresses(container);

            //then
            assertContainsAddedCustomersRecords(results1);
            assertContainsAddedProductsRecords(results2);
            assertContainsAddedAddressesRecords(results3);
        }

    }

    private <R> Pipeline getPipeline(StreamSource<ChangeRecord> source,
            FunctionEx<ChangeRecord, String> tableFn, String expectedTable,
            String outputList, BiFunctionEx<RecordPart, Operation, R> fn) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withIngestionTimestamps()
                .filter(changeRecord -> changeRecord.operation() != UNSPECIFIED)
                .map(changeRecord -> {
                    assertThat(tableFn.apply(changeRecord)).isEqualTo(expectedTable);
                    Operation operation = changeRecord.operation();
                    RecordPart value = changeRecord.value();
                    assert value != null;
                    return fn.apply(value, operation);
                })
                .writeTo(Sinks.list(outputList));
        pipeline.setPreserveOrder(true);
        return pipeline;
    }

    protected void performSetOfChangesInCustomers(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE inventory.customers SET first_name='Anne Marie' WHERE id=1004",
                    "INSERT INTO inventory.customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')",
                    "DELETE FROM inventory.customers WHERE id=1005"
            );
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcMoreJobsTogetherTest");
        }
    }

    protected void performSetOfChangesInProducts(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE inventory.products SET description='new item' WHERE id=104",
                    "INSERT INTO inventory.products VALUES (110, 'lightsabre', 'red lightsabre', '2.2')",
                    "DELETE FROM inventory.products WHERE id=110"
            );
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcMoreJobsTogetherTest");
        }
    }

    protected void performSetOfChangesInAddresses(C container) throws Exception {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE inventory.addresses SET city='Atlanta' WHERE id=12",
                    "INSERT INTO inventory.addresses VALUES (17, 1003, '742 Evergreen Terrace', "
                    + "'Springfield', 'Unknown', '12345', 'SHIPPING')",
                    "DELETE FROM inventory.addresses WHERE id=17"
            );
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcMoreJobsTogetherTest");
        }
    }

    private List<CustomerInfo> addedRecordsToCustomers() {
        return List.of(
                new CustomerInfo(UPDATE, new Customer(1004, "Anne Marie", "Kretchmar", "annek@noanswer.org")),
                new CustomerInfo(INSERT, new Customer(1005, "Jason", "Bourne", "jason@bourne.org")),
                new CustomerInfo(DELETE, new Customer(1005, "Jason", "Bourne", "jason@bourne.org"))
        );
    }

    private List<ProductInfo> addedRecordsToProducts() {
        return List.of(
                new ProductInfo(UPDATE, new Product(104, "hammer", "new item", 0.75)),
                new ProductInfo(INSERT, new Product(110, "lightsabre", "red lightsabre", 2.2)),
                new ProductInfo(DELETE, new Product(110, "lightsabre", "red lightsabre", 2.2))
        );
    }

    private List<AddressInfo> addedRecordsToAddresses() {
        return List.of(
                new AddressInfo(UPDATE, new Address(12, 1002, "281 Riverside Drive",
                        "Atlanta", "Georgia", "30901", "BILLING")),
                new AddressInfo(INSERT, new Address(17, 1003, "742 Evergreen Terrace",
                        "Springfield", "Unknown", "12345", "SHIPPING")),
                new AddressInfo(DELETE, new Address(17, 1003, "742 Evergreen Terrace",
                        "Springfield", "Unknown", "12345", "SHIPPING"))
        );
    }

    private void assertContainsAddedCustomersRecords(IList<CustomerInfo> results) {
        var expectedRecords = addedRecordsToCustomers();
        assertTrueEventually(() -> {
            var subset = results.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset).containsExactlyInAnyOrderElementsOf(expectedRecords);
        });
    }

    private void assertContainsAddedProductsRecords(IList<ProductInfo> results) {
        var expectedRecords = addedRecordsToProducts();
        assertTrueEventually(() -> {
            var subset = results.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset).containsExactlyInAnyOrderElementsOf(expectedRecords);
        });
    }

    private void assertContainsAddedAddressesRecords(IList<AddressInfo> results) {
        var expectedRecords = addedRecordsToAddresses();
        assertTrueEventually(() -> {
            var subset = results.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset).containsExactlyInAnyOrderElementsOf(expectedRecords);
        });
    }

}
