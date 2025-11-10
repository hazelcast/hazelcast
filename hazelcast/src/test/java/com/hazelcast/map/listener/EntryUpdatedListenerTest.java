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

package com.hazelcast.map.listener;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapInterceptorAdaptor;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serial;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryUpdatedListenerTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "offload: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Parameterized.Parameter
    public boolean offload;

    /**
     * @see <a href="https://hazelcast.atlassian.net/browse/HZ-2837">HZ-2837 - Field level mutation being taken by listener as
     *      old value but not being considered by interceptor - Strange Behaviour</a>
     */
    @Test
    public void testOldValues() throws InterruptedException, ExecutionException {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(MapServiceContext.PROP_FORCE_OFFLOAD_ALL_OPERATIONS, String.valueOf(offload));
        final HazelcastInstance instance = createHazelcastInstanceFactory().newHazelcastInstance(config);

        // Create a map
        final IMap<Object, AtomicInteger> map = instance.getMap(randomMapName());

        // Set up the listeners
        final CompletableFuture<Integer> entryListenerOldValue = setMapListener(
                listener -> map.addEntryListener(listener, true));
        final CompletableFuture<Integer> entryLocalListenerOldValue = setMapListener(map::addLocalEntryListener);

        final CompletableFuture<Integer> interceptorOldValue = new CompletableFuture<>();
        map.addInterceptor(new MapInterceptorAdaptor() {
            @Serial
            private static final long serialVersionUID = 1L;

            @Override
            public Object interceptPut(final Object oldValue, final Object newValue) {
                if (oldValue != null) {
                    interceptorOldValue.complete(((AtomicInteger) oldValue).get());
                }

                return super.interceptPut(oldValue, newValue);
            }
        });

        // Insert a dummy initial value
        final Object key = Void.TYPE;
        final int initial = 1;

        map.set(key, new AtomicInteger(initial));

        map.executeOnKey(key, entry -> {
            // Mutate the value in the map directly - expectation is that this will not be visible to anyone -
            // https://hazelcast.atlassian.net/browse/HZ-2837?focusedCommentId=82402
            entry.getValue().set(Integer.MAX_VALUE);

            // Also, update the map entry with a new value
            entry.setValue(new AtomicInteger(2));

            return null;
        });

        // Check all the listeners received the correct response
        assertEqualsStringFormat(
                "Initial value provided (%s) does not match old value observed by EntryUpdatedListener.entryUpdated (%s) differ",
                initial, entryListenerOldValue.get());

        assertEqualsStringFormat(
                "Initial value provided (%s) does not match old value observed by Local EntryUpdatedListener.entryUpdated (%s) differ",
                initial, entryLocalListenerOldValue.get());

        assertEqualsStringFormat(
                "Initial value provided (%s) does not match old value observed by MapInterceptor.interceptPut (%s)", initial,
                interceptorOldValue.get());
    }

    @Test
    public void testEntryValueVisibilityMatchesConfiguration_WithoutValue() {
        testEntryValueVisibilityMatchesConfiguration(false);
    }

    @Test
    public void testEntryValueVisibilityMatchesConfiguration_WithValue() {
        testEntryValueVisibilityMatchesConfiguration(true);
    }

    private void testEntryValueVisibilityMatchesConfiguration(boolean includeValue) {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(MapServiceContext.PROP_FORCE_OFFLOAD_ALL_OPERATIONS, String.valueOf(offload));

        // Configure both local & remote listeners for our map
        final String mapName = randomMapName();
        final HasValueEntryListener localListener = new HasValueEntryListener();
        final HasValueEntryListener remoteListener = new HasValueEntryListener();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addEntryListenerConfig(new EntryListenerConfig(localListener, true, includeValue));
        mapConfig.addEntryListenerConfig(new EntryListenerConfig(remoteListener, false, includeValue));

        // Start the instance, insert into the map, then update the value
        final HazelcastInstance instance = createHazelcastInstanceFactory().newHazelcastInstance(config);
        final IMap<String, String> map = instance.getMap(mapName);
        map.put("key", "previousValue");
        map.put("key", "newValue");

        // Wait for and assert condition of listeners
        validateListenerOutput(localListener, true, includeValue);
        validateListenerOutput(remoteListener, false, includeValue);
    }

    private static void validateListenerOutput(HasValueEntryListener listener, boolean local, boolean expectsValues) {
        try {
            listener.entryReceivedLatch.await(ASSERT_COMPLETES_STALL_TOLERANCE, TimeUnit.SECONDS);
            assertThat(listener.values).allMatch(expectsValues ? Objects::nonNull : Objects::isNull,
                    String.format("Expected %s listener to %s values!", local ? "local" : "remote",
                            expectsValues ? "include" : "not include"));
        } catch (InterruptedException ex) {
            throw new AssertionError(ex);
        }
    }

    private static class HasValueEntryListener implements EntryUpdatedListener<String, String> {
        private final CountDownLatch entryReceivedLatch = new CountDownLatch(1);
        private volatile Object[] values;

        @Override
        public void entryUpdated(EntryEvent<String, String> event) {
            values = new Object[]{event.getValue(), event.getOldValue()};
            entryReceivedLatch.countDown();
        }
    }

    /**
     * Constructs a {@link EntryUpdatedListener}, registering it via {@code listenerSetter}
     *
     * @return a {@link CompletableFuture} referencing the {@link EntryEvent#getOldValue()} from when the
     *         {@link EntryUpdatedListener} was fired
     */
    private static CompletableFuture<Integer> setMapListener(
            final Consumer<EntryUpdatedListener<Object, AtomicInteger>> listenerSetter) {
        final CompletableFuture<Integer> oldValue = new CompletableFuture<>();
        listenerSetter
                .accept(event -> oldValue.complete(event.getOldValue().get()));
        return oldValue;
    }
}
