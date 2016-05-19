/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Test event types as published by {@link com.hazelcast.map.impl.event.DefaultEntryEventFilteringStrategy}
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class EntryListenerEventTypesTest extends AbstractEntryEventTypesTest {

    @Before
    public void setup() {
        instance = createHazelcastInstance();
        map = instance.getMap("EntryListenerEventTypesTestMap");
        eventCounter.set(0);
    }

    @After
    public void tearDown() {
        instance.shutdown();
    }

    MapListener mapListenerFor_entryUpdatedEvent_whenOldValueOutside_newValueMatchesPredicate() {
        return new CountEntryUpdatedListener(eventCounter);
    }

    MapListener mapListenerFor_entryUpdatedEvent_whenOldValueMatches_newValueOutsidePredicate() {
        return new CountEntryUpdatedListener(eventCounter);
    }

    Integer expectedCountFor_entryUpdatedEvent_whenOldValueMatches_newValueOutsidePredicate() {
        return 0;
    }

}
