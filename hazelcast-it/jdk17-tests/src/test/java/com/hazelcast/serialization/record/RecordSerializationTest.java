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

package com.hazelcast.serialization.record;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.example.record.Person;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordSerializationTest extends HazelcastTestSupport {

    @Test
    @Ignore("We are introducing a module compiled with JDK17 - to test e.g. records - and this is currently failing")
    public void shouldSerializeAndDeserializeRecord() {
        Config config = smallInstanceConfig();
        config.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);

        HazelcastInstance instance = createHazelcastInstance(config);
        Person person = new Person(1, "Frantisek");
        IMap<Integer, Person> map = instance.getMap("my-map");

        map.put(person.id(), person);

        Person loaded = map.get(person.id());
        assertThat(loaded).isEqualTo(person);
    }
}
