/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.map.IMap;

public class IndexCreateDynamicTest extends IndexCreateStaticTest {
    @Override
    protected HazelcastInstanceProxy createMap(IndexConfig... indexConfigs) {
        MapConfig mapConfig = new MapConfig(MAP_NAME);

        HazelcastInstance member = createHazelcastInstance(new Config().addMapConfig(mapConfig));

        IMap map = member.getMap(MAP_NAME);

        for (IndexConfig indexConfig : indexConfigs) {
            map.addIndex(indexConfig);
        }

        return (HazelcastInstanceProxy)member;
    }
}
