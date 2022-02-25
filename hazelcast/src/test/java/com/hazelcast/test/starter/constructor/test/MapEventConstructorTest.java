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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.MapEvent;
import com.hazelcast.cluster.Member;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.MapEventConstructor;
import com.hazelcast.internal.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapEventConstructorTest {

    @Test
    public void testConstructor() {
        String source = UuidUtil.newUnsecureUuidString();
        Member member = mock(Member.class);
        int eventType = EntryEventType.INVALIDATION.getType();

        MapEvent mapEvent = new MapEvent(source, member, eventType, 23);

        MapEventConstructor constructor = new MapEventConstructor(MapEvent.class);
        MapEvent clonedMapEvent = (MapEvent) constructor.createNew(mapEvent);

        assertEquals(mapEvent.getName(), clonedMapEvent.getName());
        assertEquals(mapEvent.getMember(), clonedMapEvent.getMember());
        assertEquals(mapEvent.getEventType(), clonedMapEvent.getEventType());
        assertEquals(mapEvent.getSource(), clonedMapEvent.getSource());
        assertEquals(mapEvent.getNumberOfEntriesAffected(), clonedMapEvent.getNumberOfEntriesAffected());
    }
}
