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

package com.hazelcast.internal.namespace.replicatedmap;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertNotNull;

public class ReplicatedMapPredicateUCDTest extends ReplicatedMapUCDTest {
    @Override
    public void test() throws Exception {
        final CompletableFuture<EntryEvent<Object, Object>> listenerEvent = new CompletableFuture<>();

        map.addEntryListener(new EntryAdapter<>() {
            @Override
            public void onEntryEvent(EntryEvent<Object, Object> event) {
                listenerEvent.complete(event);
            }
        }, getClassInstance());

        populate();

        // Block and wait for listener to fire
        assertNotNull(listenerEvent.get());
    }

    @Override
    protected String getUserDefinedClassName() {
        return "usercodedeployment.TruePredicate";
    }
}
