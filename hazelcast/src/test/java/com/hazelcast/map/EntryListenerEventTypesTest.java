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

package com.hazelcast.map;

import com.hazelcast.map.listener.MapListener;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Test event types as published by {@link com.hazelcast.map.impl.event.DefaultEntryEventFilteringStrategy}
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

    @Override
    MapListener mapListenerFor_entryUpdatedEvent_whenOldValueOutside_newValueMatchesPredicate() {
        return new CountEntryUpdatedListener(eventCounter);
    }

    @Override
    MapListener mapListenerFor_entryUpdatedEvent_whenOldValueMatches_newValueOutsidePredicate() {
        return new CountEntryUpdatedListener(eventCounter);
    }

    @Override
    Integer expectedCountFor_entryUpdatedEvent_whenOldValueMatches_newValueOutsidePredicate() {
        return 0;
    }

}
