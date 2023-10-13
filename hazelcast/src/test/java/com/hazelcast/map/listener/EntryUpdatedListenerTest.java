/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Arrays.asList;

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

        // Setup the listeners
        final CompletableFuture<Integer> entryListenerOldValue = setMapListener(
                listener -> map.addEntryListener(listener, true));
        final CompletableFuture<Integer> entryLocalListenerOldValue = setMapListener(map::addLocalEntryListener);

        final CompletableFuture<Integer> interceptorOldValue = new CompletableFuture<>();
        map.addInterceptor(new MapInterceptorAdaptor() {
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

    /**
     * Constructs a {@link EntryUpdatedListener}, registering it via {@code listenerSetter}
     *
     * @return a {@link CompleteableFuture} referencing the {@link EntryEvent.getOldValue()} from when the
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
