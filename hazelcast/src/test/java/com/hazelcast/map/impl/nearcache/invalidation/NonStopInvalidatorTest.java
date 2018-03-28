/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.NonStopInvalidator;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nearcache.impl.invalidation.InvalidationUtils.TRUE_FILTER;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NonStopInvalidatorTest extends HazelcastTestSupport {

    private Data key;
    private NonStopInvalidator invalidator;

    @Before
    public void setUp() {
        key = mock(Data.class);

        HazelcastInstance hz = createHazelcastInstance();
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);
        invalidator = new NonStopInvalidator(MapService.SERVICE_NAME, TRUE_FILTER, nodeEngineImpl);
    }

    @Test(expected = NullPointerException.class)
    public void testInvalidate_withInvalidMapName() {
        invalidator.invalidateKey(key, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testClear_withInvalidMapName() {
        invalidator.invalidateAllKeys(null, null);
    }
}
