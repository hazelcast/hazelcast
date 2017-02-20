/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.nearcache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.nearcache.NearCacheLiteMemberTest.createConfig;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheLiteMemberInvalidationTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);

    @After
    public void destroy() {
        factory.terminateAll();
    }

    @Test
    public void no_near_cached_member_sends_invalidations_to_near_cached_lite_members() throws Exception {
        String mapName = "test";

        // populate map from ordinary member.
        IMap map = factory.newHazelcastInstance().getMap(mapName);
        map.put(1, 1);

        // cache value in lite members near cache.
        HazelcastInstance lite = factory.newHazelcastInstance(createConfig(mapName, true));
        final IMap liteMap = lite.getMap(mapName);
        liteMap.get(1);

        // update value from ordinary member.
        map.put(1, 2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, liteMap.get(1));
            }
        });
    }
}
