/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlErrorCode;
import org.junit.After;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class SqlErrorAbstractTest extends SqlTestSupport {
    protected static final String MAP_NAME = "map";
    private static final int DATA_SET_SIZE = 100;

    protected final TestHazelcastFactory factory = new TestHazelcastFactory();

    protected HazelcastInstance instance1;
    protected HazelcastInstance instance2;
    protected HazelcastInstance client;

    @After
    public void after() {
        instance1 = null;
        instance2 = null;
        client = null;

        factory.shutdownAll();
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    protected void checkTimeout(boolean useClient) {
        // Start two instances and fill them with data
        instance1 = newHazelcastInstance(false);
        instance2 = newHazelcastInstance(true);
        client = newClient();

        // Execute query on the instance1.
        SqlStatement query = new SqlStatement("select v from table(generate_stream(1))")
                .setTimeoutMillis(1L);
        HazelcastSqlException error = assertSqlException(useClient ? client : instance1, query);
        assertTrue(error.getMessage().contains("CANCEL_FORCEFUL"));
    }

    protected void checkDataTypeMismatch(boolean useClient) {
        // Start two instances and fill them with data
        instance1 = newHazelcastInstance(false);
        instance2 = newHazelcastInstance(true);
        client = newClient();

        IMap<Long, Object> map = instance1.getMap(MAP_NAME);

        createMapping(instance1, MAP_NAME, long.class, long.class);
        for (long i = 0; i < DATA_SET_SIZE; i++) {
            Object value = i == 0 ? Long.toString(i) : i;

            map.put(i, value);
        }

        // Execute query.
        HazelcastSqlException error = assertSqlException(useClient ? client : instance1, query());

        assertErrorCode(SqlErrorCode.DATA_EXCEPTION, error);
        assertTrue(error.getMessage().contains("Failed to extract map entry value because of type mismatch "
                + "[expectedClass=java.lang.Long, actualClass=java.lang.String]"));
    }

    protected void checkParsingError(boolean useClient) {
        instance1 = newHazelcastInstance(true);
        client = newClient();

        createMapping(instance1, MAP_NAME, long.class, long.class);
        IMap<Long, Long> map = instance1.getMap(MAP_NAME);
        map.put(1L, 1L);

        HazelcastInstance target = useClient ? client : instance1;

        HazelcastSqlException error = assertSqlException(target, new SqlStatement("SELECT bad_field FROM " + MAP_NAME));
        assertErrorCode(SqlErrorCode.PARSING, error);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    protected void checkUserCancel(boolean useClient) {
        instance1 = newHazelcastInstance(true);
        client = newClient();

        HazelcastInstance target = useClient ? client : instance1;

        try (SqlResult res = target.getSql().execute("select * from table(generate_stream(1))")) {
            sleepSeconds(1);
            res.close();

            try {
                for (SqlRow ignore : res) {
                    // No-op.
                }

                fail("Exception is not thrown");
            } catch (HazelcastSqlException e) {
                assertErrorCode(SqlErrorCode.CANCELLED_BY_USER, e);
            }
        }
    }

    @Nonnull
    protected static HazelcastSqlException assertSqlException(HazelcastInstance instance, SqlStatement query) {
        try {
            execute(instance, query, false);

            fail("Exception is not thrown");

            return null;
        } catch (HazelcastSqlException e) {
            System.out.println(">>> Caught expected SQL error: " + e);

            return e;
        }
    }

    @Nonnull
    protected static HazelcastSqlException assertSqlExceptionWithShutdown(HazelcastInstance instance, SqlStatement query) {
        try {
            execute(instance, query, true);

            fail("Exception is not thrown");

            return null;
        } catch (HazelcastSqlException e) {
            System.out.println(">>> Caught expected SQL error: " + e);

            return e;
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    protected static int execute(HazelcastInstance instance, SqlStatement query, boolean immediateShutdown) {
        try (SqlResult res = instance.getSql().execute(query)) {
            int count = 0;
            if (immediateShutdown) {
                instance.shutdown();
            }

            for (SqlRow ignore : res) {
                count++;
            }

            return count;
        }
    }

    protected static SqlStatement query() {
        return new SqlStatement("SELECT __key, this FROM " + MAP_NAME);
    }

    protected static void populate(HazelcastInstance instance) {
        populate(instance, DATA_SET_SIZE);
    }

    protected static void populate(HazelcastInstance instance, int size) {
        Map<Long, Long> map = new HashMap<>();

        for (long i = 0; i < size; i++) {
            map.put(i, i);
        }

        instance.getMap(MAP_NAME).putAll(map);
    }

    protected HazelcastInstance newClient() {
        return factory.newHazelcastClient(clientConfig());
    }

    protected ClientConfig clientConfig() {
        return new ClientConfig();
    }

    protected static void assertErrorCode(int expected, HazelcastSqlException error) {
        assertEquals(error.getCode() + ": " + error.getMessage(), expected, error.getCode());
    }

    protected static Throwable findRootCause(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    /**
     * Start the new Hazelcast instance.
     *
     * @param awaitAssignment whether to wait for a partition assignment to a new member
     * @return created instance
     */
    protected HazelcastInstance newHazelcastInstance(boolean awaitAssignment) {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        if (awaitAssignment) {
            assertTrueEventually(() -> {
                Set<UUID> memberIds = new HashSet<>();

                for (Member member : instance.getCluster().getMembers()) {
                    memberIds.add(member.getUuid());
                }

                PartitionService partitionService = instance.getPartitionService();

                Set<UUID> assignedMemberIds = new HashSet<>();

                for (Partition partition : partitionService.getPartitions()) {
                    Member owner = partition.getOwner();

                    assertNotNull(owner);

                    assignedMemberIds.add(owner.getUuid());
                }

                assertEquals(memberIds, assignedMemberIds);
            });
        }

        return instance;
    }
}
