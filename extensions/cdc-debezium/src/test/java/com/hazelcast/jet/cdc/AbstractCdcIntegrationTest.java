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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.cdc.TestUtils.Customer;
import com.hazelcast.jet.cdc.TestUtils.CustomerInfo;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.cdc.Operation.INSERT;
import static com.hazelcast.jet.cdc.Operation.SYNC;
import static com.hazelcast.jet.cdc.Operation.UPDATE;
import static com.hazelcast.jet.cdc.TestUtils.executeSql;
import static com.hazelcast.jet.cdc.TestUtils.smallInstanceConfigWithCompactSerialization;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;

@Category({IgnoreInJenkinsOnWindows.class})
public abstract class AbstractCdcIntegrationTest<C extends GenericContainer<?>> extends JetTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCdcIntegrationTest.class);

    protected static GenericContainer<?> container;

    @Rule
    public TestName testName = new TestName();
    protected int testNumberModifier = 1000;
    protected String uuidForNotifications;

    @BeforeClass
    public static void beforeClassCheckDocker() {
        assumeDockerEnabled();
    }

    @Before
    @SuppressWarnings("unchecked")
    public void setUpDatabase() {
        if (container == null) {
            container = getContainer();
            container.start();
            prepareDatabase((C) container);
        }
        testNumberModifier = RandomUtils.insecure().randomInt(1100, 9999);
        uuidForNotifications = UUID.randomUUID().toString();
        LOG.info("Current test is '{}' and it has number modifier: {}, notification UUID: {}",
                testName.getMethodName(), testNumberModifier, uuidForNotifications);
    }

    @SuppressWarnings("unchecked")
    public C container() {
        return (C) container;
    }

    @AfterClass
    public static void afterClassCleanDb() {
        if (container != null) {
            container.stop();
            container = null;
            System.out.println("Container cleaned up");
        }
    }

    /**
     * Extend in subclass if any special treatment is needed.
     */
    public void prepareDatabase(C container) {
    }

    @Nonnull
    public abstract C getContainer();
    @Nonnull
    protected abstract DebeziumCdcSources.Builder<ChangeRecord> basicConf(C container);

    @Nonnull
    public Pipeline getPipeline(StreamSource<ChangeRecord> source) {
        return TestUtils.getPipeline(source, ChangeRecord::table);
    }

    protected String tablePrefix() {
        return "inventory";
    }

    protected void performSetOfChanges(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    ("INSERT INTO %1$s.customers (id, first_name, last_name, email) "
                            + "VALUES (1005, 'Jason%2$s', 'Bourne', 'jason@bourne.org')")
                            .formatted(tablePrefix(), testNumberModifier),
                    "DELETE FROM %1$s.customers WHERE id=1005".formatted(tablePrefix())
            );
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE %1$s.customers SET first_name='Anne Marie%2$s' WHERE id=1004"
                            .formatted(tablePrefix(), testNumberModifier)
            );
            LOG.info("Performed a set of queries");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcIntegrationTest");
        }
    }

    protected void revertSetOfChanges(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE %1$s.customers SET first_name='Anne' WHERE id=1004".formatted(tablePrefix())
            );
            LOG.info("Reverted a set of queries");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcIntegrationTest");
        }
    }

    protected void performSecondSetOfChanges(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(), """
            INSERT INTO %1$s.customers (id, first_name, last_name, email)
            VALUES (%2$s, 'Darth', 'Vader', 'vader@empire.com')
            """.formatted(tablePrefix(), 1007 + testNumberModifier));
            LOG.info("Performed a second set of queries");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcIntegrationTest");
        }
    }

    protected void revertSecondSetOfChanges(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(), """
            DELETE FROM %1$s.customers WHERE id=%2$s
            """.formatted(tablePrefix(), 1007 + testNumberModifier));
            LOG.info("Reverted a second set of queries");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcIntegrationTest");
        }
    }

    protected void performChangeToProduct(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "INSERT INTO %s.products (id, name, description, weight) values (%s, '%s', '%s', 3.14)"
                            .formatted(tablePrefix(), 100 + testNumberModifier, "product_" + testNumberModifier,
                                    "product_" + testNumberModifier));
            LOG.info("Performed a change to product");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcAlterTableTest");
        }
    }

    protected void revertChangeToProduct(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "DELETE FROM %s.products WHERE id=%s".formatted(tablePrefix(), 100 + testNumberModifier));
            LOG.info("Reverted a change to product");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcAlterTableTest");
        }
    }

    @Nonnull
    protected final StreamSource<ChangeRecord> getSource(C container) {
        return basicConf(container).build();
    }

    protected @Nonnull List<CustomerInfo> initialRecords() {
        return List.of(
                new CustomerInfo(SYNC, new Customer(1001, "Sally", "Thomas", "sally.thomas@acme.com")),
                new CustomerInfo(SYNC, new Customer(1002, "George", "Bailey", "gbailey@foobar.com")),
                new CustomerInfo(SYNC, new Customer(1003, "Edward", "Walker", "ed@walker.com")),
                new CustomerInfo(SYNC, new Customer(1004, "Anne", "Kretchmar", "annek@noanswer.org"))
        );
    }

    protected @Nonnull List<CustomerInfo> addedRecords() {
        return List.of(
                new CustomerInfo(UPDATE, new Customer(1004, "Anne Marie" + testNumberModifier, "Kretchmar", "annek@noanswer.org")),
                new CustomerInfo(INSERT, new Customer(1005, "Jason" + testNumberModifier, "Bourne", "jason@bourne.org")),
                new CustomerInfo(DELETE, new Customer(1005, "Jason" + testNumberModifier, "Bourne", "jason@bourne.org"))
        );
    }

    protected void waitForSnapshotEnd() {
        TestNotificationChannel.waitForSnapshotEnd(uuidForNotifications);
    }

    @Override
    protected HazelcastInstance[] createHazelcastInstances(int nodeCount) {
        return super.createHazelcastInstances(smallInstanceConfigWithCompactSerialization(), nodeCount);
    }
}
