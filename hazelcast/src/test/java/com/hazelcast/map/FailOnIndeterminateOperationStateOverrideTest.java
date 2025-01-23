/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.google.common.collect.ObjectArrays;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.partition.IndeterminateOperationStateExceptionTest.waitForBackupAndDrop;
import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class FailOnIndeterminateOperationStateOverrideTest extends HazelcastTestSupport {

    private static String absentKey(IMap<?, ?> m) {
        // hacky way to generate key that does not exist in map
        // and is owned by first instance
        assumeThat((Object) m).as("Testcase can be executed only with member proxy")
                .isInstanceOf(MapProxyImpl.class);
        return generateKeyOwnedBy(((MapProxyImpl<?, ?>) m).getNodeEngine().getHazelcastInstance());
    }

    @Parameterized.Parameters(
            name = "operation:{0},globalFailOnIndeterminateOperationState:{2},overriddenFailOnIndeterminateOperationState:{3},shutdownMember:{4}")
    public static Collection<Object[]> parameters() {

        List<Object[]> flags = cartesianProduct(List.of(true, false), List.of(true, false), List.of(false, true));

        // each operation must modify map so backup operation is executed
        // some operations do it depending on the arguments and map contents
        @SuppressWarnings("rawtypes,unchecked")
        List<Object[]> functions = Arrays.asList(new Object[][]{
                {"put", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertEquals(k, m.put(k, k + "1"))},
                {"remove", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertEquals(k, m.remove(k))},
                {"remove2", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertTrue(m.remove(k, k))},
                {"delete", (BiConsumerEx<IMap, Object>) IMap::delete},
                {"putAsync", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertEquals(k, m.putAsync(k, k + "1").toCompletableFuture().get())},
                {"setAsync", (BiConsumerEx<IMap, Object>) (m, k) ->
                        m.setAsync(k, k).toCompletableFuture().get()},
                {"removeAsync", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertEquals(k, m.removeAsync(k).toCompletableFuture().get())},
                {"tryRemove", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertTrue(m.tryRemove(k, 10, TimeUnit.SECONDS))},
                {"tryPut", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertTrue(m.tryPut(k, k, 10, TimeUnit.SECONDS))},
                {"putTransient", (BiConsumerEx<IMap, Object>) (m, k) ->
                        m.putTransient(k, k, 10, TimeUnit.SECONDS)},

                {"putIfAbsent", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertNull(m.putIfAbsent(absentKey(m), k))},
                {"replace2", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertEquals(k, m.replace(k, k + "1"))},
                {"replace3", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertTrue(m.replace(k, k, k + "1"))},
                {"set", (BiConsumerEx<IMap, Object>) (m, k) ->
                        m.set(k, k + "1")},

                {"evict", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertTrue(m.evict(k))},

                {"executeOnKey", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertEquals(k + "2", m.executeOnKey(k, e -> e.setValue(e.getValue() + "1") + "2"))},
                {"submitToKey", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertEquals(k + "2", m.submitToKey(k, e -> e.setValue(e.getValue() + "1") + "2").toCompletableFuture().get())},

                {"computeIfPresent", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertEquals(k + "1", m.computeIfPresent(k, (ek, ev) -> ev + "1"))},
                {"computeIfAbsent", (BiConsumerEx<IMap, Object>) (m, k) -> {
                    String absentKey = absentKey(m);
                    assertEquals(absentKey + "1", m.computeIfAbsent(absentKey, (ek) -> ek + "1"));
                } },
                {"compute", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertEquals(k + "1", m.compute(k, (ek, ev) -> ev + "1"))},
                {"merge", (BiConsumerEx<IMap, Object>) (m, k) ->
                        assertEquals(k + "1", m.merge(k, k, (ek, ev) -> ev + "1"))},

                // private methods
                {"putIfAbsentAsync", (BiConsumerEx<IMap, Object>) (m, k) -> {
                    assumeThat((Object) m).as("Testcase can be executed only with member proxy")
                            .isInstanceOf(MapProxyImpl.class);
                    assertNull(((MapProxyImpl) m).putIfAbsentAsync(absentKey(m), k).toCompletableFuture().get());
                } }
        });

        return cartesianProductTuple(functions, flags)
                .stream()
                .map(l -> ObjectArrays.concat(l.f0(), l.f1(), Object.class))
                .collect(Collectors.toList());
    }

    @Parameterized.Parameter(0)
    public String operationName;

    @Parameterized.Parameter(1)
    public BiConsumerEx<IMap<?, ?>, String> operation;

    @Parameterized.Parameter(2)
    public boolean globalFailOnIndeterminateOperationState;

    /**
     * Meaning of this parameter is different for member-side and client-side tests.
     * For members it means overriding using private IMap API, for clients it means
     * setting it in {@link ClientConfig}.
     */
    @Parameterized.Parameter(3)
    public boolean overriddenFailOnIndeterminateOperationState;

    @Parameterized.Parameter(4)
    public boolean shutdownMember;

    private boolean operationShouldFail() {
        return overriddenFailOnIndeterminateOperationState;
    }

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private HazelcastInstance instance1;

    private HazelcastInstance instance2;

    @Before
    public void setup() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(OPERATION_BACKUP_TIMEOUT_MILLIS.getName(), String.valueOf(1000));
        if (globalFailOnIndeterminateOperationState) {
            config.setProperty(ClusterProperty.FAIL_ON_INDETERMINATE_OPERATION_STATE.getName(), String.valueOf(true));
        }

        instance1 = factory.newHazelcastInstance(config);
        instance2 = factory.newHazelcastInstance(config);
        warmUpPartitions(instance1, instance2);
    }

    @After
    public void teardown() {
        factory.shutdownAll();
    }

    @Test
    public void operationShouldFail_whenBackupAckMissed() {
        // initialize map with content
        String key = generateKeyOwnedBy(instance1);
        IMap<Object, Object> map = instance1.getMap(randomMapName());
        map.put(key, key);
        ((MapProxyImpl<?, ?>) map).setFailOnIndeterminateOperationState(overriddenFailOnIndeterminateOperationState);

        // break backups
        var latch = waitForBackupAndDrop(instance1);
        shutdownBackupMemberIfNeeded(latch);

        if (operationShouldFail()) {
            assertThatThrownBy(() -> operation.accept(map, key))
                    .extracting(t -> t instanceof ExecutionException ? t.getCause() : t, InstanceOfAssertFactories.THROWABLE)
                    .isInstanceOf(IndeterminateOperationStateException.class);
        } else {
            operation.accept(map, key);
        }
    }

    @Test
    public void clientOperationShouldFail_whenBackupAckMissed() {
        // default config - backup ack to client enabled
        clientOperationShouldFail_whenBackupAckMissed(true);
    }

    @Test
    public void clientOperationShouldFail_whenBackupAckMissedAndBoomerangBackupDisabled() {
        clientOperationShouldFail_whenBackupAckMissed(false);
    }

    private void clientOperationShouldFail_whenBackupAckMissed(boolean backupAckToClientEnabled) {
        assumeThat(globalFailOnIndeterminateOperationState)
                .as("IntermediateOperationStateException should be thrown either on client or on member but not on both")
                .isNotEqualTo(backupAckToClientEnabled);

        // initialize map with content
        String key = generateKeyOwnedBy(instance1);
        var client = factory.newHazelcastClient(new ClientConfig()
                .setProperty(ClientProperty.FAIL_ON_INDETERMINATE_OPERATION_STATE.getName(), String.valueOf(overriddenFailOnIndeterminateOperationState))
                .setBackupAckToClientEnabled(backupAckToClientEnabled));
        IMap<Object, Object> map = client.getMap(randomMapName());
        map.put(key, key);

        // break backups
        var latch = waitForBackupAndDrop(instance1);
        shutdownBackupMemberIfNeeded(latch);

        if (globalFailOnIndeterminateOperationState || overriddenFailOnIndeterminateOperationState) {
            assertThatThrownBy(() -> operation.accept(map, key))
                    .extracting(t -> t instanceof ExecutionException ? t.getCause() : t, InstanceOfAssertFactories.THROWABLE)
                    .isInstanceOfAny(IndeterminateOperationStateException.class, AssumptionViolatedException.class);
        } else {
            operation.accept(map, key);
        }
    }

    private void shutdownBackupMemberIfNeeded(CountDownLatch latch) {
        if (shutdownMember) {
            spawn(() -> {
                assertOpenEventually(latch);
                instance2.getLifecycleService().terminate();
            });
        }
    }
}
