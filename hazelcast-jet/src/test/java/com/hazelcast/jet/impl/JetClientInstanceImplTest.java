/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
public class JetClientInstanceImplTest extends JetTestSupport {

    @Test
    public void given_singleMapOnMember_when_getDistributedObjectsCalled_then_ReturnedObjectInfo() {
        // Given
        HazelcastInstance member = createMember();
        HazelcastInstance client = createClient();
        String mapName = randomMapName();
        member.getMap(mapName);

        // When
        Collection<DistributedObject> objects = client.getDistributedObjects();


        // Then
        assertFalse(objects.isEmpty());
        DistributedObject info = objects.stream()
                .filter(i -> mapName.equals(i.getName()))
                .findFirst()
                .orElseThrow(AssertionError::new);
        assertEquals(MapService.SERVICE_NAME, info.getServiceName());
    }


}
