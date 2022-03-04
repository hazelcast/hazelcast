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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.mockito.Mockito.mock;

public abstract class AbstractInvalidatorTest extends HazelcastTestSupport {

    private Invalidator invalidator;
    private Data key;
    private UUID sourceUuid = UuidUtil.newUnsecureUUID();

    @Before
    public void setUp() {
        Config config = getBaseConfig();
        HazelcastInstance hz = createHazelcastInstance(config);
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);
        invalidator = createInvalidator(nodeEngineImpl);
        key = mock(Data.class);
    }

    public abstract Invalidator createInvalidator(NodeEngineImpl nodeEngine);

    @Test(expected = NullPointerException.class)
    public void testInvalidate_withInvalidKey() {
        invalidator.invalidateKey(null, "mapName", sourceUuid);
    }

    @Test(expected = NullPointerException.class)
    public void testInvalidate_withInvalidMapName() {
        invalidator.invalidateKey(key, null, sourceUuid);
    }

    @Test(expected = NullPointerException.class)
    public void testInvalidate_withInvalidSourceUuid() {
        invalidator.invalidateKey(key, "mapName", null);
    }

    @Test(expected = NullPointerException.class)
    public void testInvalidateAllKeys_withInvalidMapName() {
        invalidator.invalidateAllKeys(null, sourceUuid);
    }

    @Test(expected = NullPointerException.class)
    public void testInvalidateAllKeys_withInvalidSourceUuid() {
        invalidator.invalidateAllKeys("mapName", null);
    }
}
