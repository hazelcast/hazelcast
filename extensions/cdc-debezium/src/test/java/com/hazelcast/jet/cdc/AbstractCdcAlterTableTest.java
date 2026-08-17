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
import com.hazelcast.jet.cdc.TestUtils.Customer;
import com.hazelcast.jet.cdc.TestUtils.CustomerInfo;
import com.hazelcast.jet.cdc.TestUtils.CustomerWithDesignation;
import com.hazelcast.jet.cdc.TestUtils.CustomerWithDesignationInfo;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.cdc.Operation.INSERT;
import static com.hazelcast.jet.cdc.Operation.SYNC;
import static com.hazelcast.jet.cdc.Operation.UPDATE;
import static com.hazelcast.jet.cdc.TestUtils.executeSql;
import static com.hazelcast.jet.cdc.TestUtils.standardConf;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class})
public abstract class AbstractCdcAlterTableTest<C extends GenericContainer<?>> extends AbstractCdcIntegrationTest<C> {

    @Nonnull
    public Pipeline getPipelineWithNewColumn(StreamSource<ChangeRecord> source) {
        return TestUtils.getPipelineWithNewColumn(source, ChangeRecord::table);
    }

    @Test
    public void testAddColumn_notRead() {
        // given
        Pipeline pipeline = getPipeline(getSource(container()));

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job).eventuallyHasStatus(RUNNING);

        // then
        IList<CustomerInfo> results = hz.getList("results");
        waitForSnapshotEnd();

        // when
        performSetOfChanges(container());
        try {
            addColumn(container());
            performSetOfChangesWithNewColumn(container());

            //then
            assertContainsAddedRecordsAfterAddColumn(results);
        } finally {
            dropAddedColumn(container());
        }
    }

    @Test
    public void testAddColumn_read() {
        // given
        Pipeline pipeline = getPipelineWithNewColumn(getSource(container()));

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job).eventuallyHasStatus(RUNNING);

        // then
        IList<CustomerWithDesignationInfo> results = hz.getList("results");
        waitForSnapshotEnd();

        // when
        performSetOfChanges(container());
        addColumn(container());
        try {
            performSetOfChangesWithNewColumn(container());

            //then
            assertContainsAddedRecordsWithDesignationAfterRestart(results);
        } finally {
            dropAddedColumn(container());
        }
    }

    @Test
    public void testDropColumn() {
        // given
        Pipeline pipeline = getPipeline(getSource(container()));

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job).eventuallyHasStatus(RUNNING);

        // then
        IList<CustomerInfo> results = hz.getList("results");
        waitForSnapshotEnd();

        // when
        performSetOfChanges(container());
        dropColumn(container());
        try {
            performSetOfChangesWithDroppedColumn(container());

            //then
            assertContainsAddedRecordsAfterDropColumn(results);
        } finally {
            restoreColumn(container());
        }
    }

    @Test
    public void testRenameColumn() {
        // given
        Pipeline pipeline = getPipelineWithNewColumn(getSource(container()));

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline, standardConf());
        assertThat(job).eventuallyHasStatus(RUNNING);

        // then
        IList<CustomerWithDesignationInfo> results = hz.getList("results");
        waitForSnapshotEnd();

        // when
        performSetOfChanges(container());
        renameColumn(container());
        try {
            performSetOfChangesWithRenamedColumn(container());

            //then
            assertContainsAddedRecordsAfterRestartRenameColumn(results);
        } finally {
            undoRename(container());
        }
    }

    protected void addColumn(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "ALTER TABLE inventory.customers ADD COLUMN designation VARCHAR(255)");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcAlterTableTest");
        }
    }

    protected void dropAddedColumn(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "ALTER TABLE inventory.customers DROP COLUMN designation");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcAlterTableTest");
        }
    }

    protected void dropColumn(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "ALTER TABLE inventory.customers DROP COLUMN last_name");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcAlterTableTest");
        }
    }

    protected void restoreColumn(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "ALTER TABLE inventory.customers ADD COLUMN last_name VARCHAR(255)");
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE inventory.customers SET last_name='%s' WHERE id=%s".formatted("Thomas", 1001));
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE inventory.customers SET last_name='%s' WHERE id=%s".formatted("Bailey", 1002));
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE inventory.customers SET last_name='%s' WHERE id=%s".formatted("Walker", 1003));
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE inventory.customers SET last_name='%s', email='%s' WHERE id=%s"
                            .formatted("Kretchmar", "annek@noanswer.org", 1004));
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcAlterTableTest");
        }
    }

    protected void renameColumn(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "ALTER TABLE inventory.customers RENAME COLUMN last_name to designation");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcAlterTableTest");
        }
    }

    protected void undoRename(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "ALTER TABLE inventory.customers RENAME COLUMN designation to last_name");
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE inventory.customers SET last_name = 'Kretchmar' WHERE id = 1004");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcAlterTableTest");
        }
    }

    protected void performSetOfChangesWithNewColumn(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE inventory.customers SET designation='whatever' WHERE id=1004",
                    "INSERT INTO inventory.customers (id, first_name, last_name, email, designation) "
                            + "VALUES (1006, 'Luke', 'Skywalker', 'luke@jedi.com', 'good')",
                    "DELETE FROM inventory.customers WHERE id=1006");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcAlterTableTest");
        }
    }

    protected void performSetOfChangesWithDroppedColumn(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE inventory.customers SET email='annemarie@noanswer.org' WHERE id=1004",
                    "INSERT INTO inventory.customers (id, first_name, email) "
                            + "VALUES (1006, 'Luke', 'luke@jedi.com')",
                    "DELETE FROM inventory.customers WHERE id=1006");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcAlterTableTest");
        }
    }

    protected void performSetOfChangesWithRenamedColumn(C container) {
        if (container instanceof JdbcDatabaseContainer<?> jdbcContainer) {
            executeSql(jdbcContainer.getJdbcUrl(), jdbcContainer.getUsername(), jdbcContainer.getPassword(),
                    "UPDATE inventory.customers SET designation='whatever' WHERE id=1004",
                    "INSERT INTO inventory.customers (id, first_name, designation, email) "
                            + "VALUES (1006, 'Luke', 'good', 'luke@jedi.com')",
                    "DELETE FROM inventory.customers WHERE id=1006");
        } else {
            throw new NotImplementedException("Please override methods in AbstractCdcAlterTableTest");
        }
    }

    private void assertContainsAddedRecordsAfterAddColumn(IList<CustomerInfo> results) {
        var expectedRecords = new ArrayList<CustomerInfo>();
        expectedRecords.add(
                new CustomerInfo(UPDATE, new Customer(1004, "Anne Marie" + testNumberModifier, "Kretchmar", "annek@noanswer.org")));
        expectedRecords.add(
                new CustomerInfo(INSERT, new Customer(1006, "Luke", "Skywalker", "luke@jedi.com")));
        expectedRecords.add(
                new CustomerInfo(DELETE, new Customer(1006, "Luke", "Skywalker", "luke@jedi.com")));
        assertTrueEventually(() -> {
            var subset = results.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset).containsAll(expectedRecords);
        });
    }

    private void assertContainsAddedRecordsAfterDropColumn(IList<CustomerInfo> results) {
        var expectedRecords = new ArrayList<CustomerInfo>();
        expectedRecords.add(
                new CustomerInfo(UPDATE, new Customer(1004, "Anne Marie" + testNumberModifier, null, "annemarie@noanswer.org")));
        expectedRecords.add(
                new CustomerInfo(INSERT, new Customer(1006, "Luke", null, "luke@jedi.com")));
        expectedRecords.add(
                new CustomerInfo(DELETE, new Customer(1006, "Luke", null, "luke@jedi.com")));
        assertTrueEventually(() -> {
            var subset = results.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset).containsAll(expectedRecords);
        });
    }

    private List<CustomerWithDesignationInfo> addedRecordsWithDesignation() {
        return List.of(
                new CustomerWithDesignationInfo(UPDATE,
                        new CustomerWithDesignation(1004, "Anne Marie" + testNumberModifier, "Kretchmar", "annek@noanswer.org", null)),
                new CustomerWithDesignationInfo(INSERT,
                        new CustomerWithDesignation(1005, "Jason" + testNumberModifier, "Bourne", "jason@bourne.org", null)),
                new CustomerWithDesignationInfo(DELETE,
                        new CustomerWithDesignation(1005, "Jason" + testNumberModifier, "Bourne", "jason@bourne.org", null))
        );
    }

    private void assertContainsAddedRecordsWithDesignationAfterRestart(IList<CustomerWithDesignationInfo> results) {
        var expectedRecords = new ArrayList<>(addedRecordsWithDesignation());
        expectedRecords.add(
                new CustomerWithDesignationInfo(UPDATE,
                        new CustomerWithDesignation(1004, "Anne Marie" + testNumberModifier, "Kretchmar", "annek@noanswer.org", "whatever")));
        expectedRecords.add(
                new CustomerWithDesignationInfo(INSERT,
                        new CustomerWithDesignation(1006, "Luke", "Skywalker", "luke@jedi.com", "good")));
        expectedRecords.add(
                new CustomerWithDesignationInfo(DELETE,
                        new CustomerWithDesignation(1006, "Luke", "Skywalker", "luke@jedi.com", "good")));
        assertTrueEventually(() -> {
            var subset = results.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset).containsAll(expectedRecords);
        });
    }

    private void assertContainsAddedRecordsAfterRestartRenameColumn(IList<CustomerWithDesignationInfo> results) {
        var expectedRecords = new ArrayList<CustomerWithDesignationInfo>();
        expectedRecords.add(
                new CustomerWithDesignationInfo(UPDATE,
                        new CustomerWithDesignation(1004, "Anne Marie" + testNumberModifier, null, "annek@noanswer.org", "whatever")));
        expectedRecords.add(
                new CustomerWithDesignationInfo(INSERT,
                        new CustomerWithDesignation(1006, "Luke", null, "luke@jedi.com", "good")));
        expectedRecords.add(
                new CustomerWithDesignationInfo(DELETE,
                        new CustomerWithDesignation(1006, "Luke", null, "luke@jedi.com", "good")));
        assertTrueEventually(() -> {
            var subset = results.stream().filter(s -> s.op() != SYNC).toList();
            assertThat(subset).containsAll(expectedRecords);
        });
    }

}
