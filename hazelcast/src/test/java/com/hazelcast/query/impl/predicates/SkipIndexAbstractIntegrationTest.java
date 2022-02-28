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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;


public abstract class SkipIndexAbstractIntegrationTest extends HazelcastTestSupport {

    @Test
    public void testIndexSuppressionDoesntChangeOutcome() {
        HazelcastInstance hz = getHazelcastInstance();
        IMap<Integer, Pojo> map = hz.getMap("foo");
        map.addIndex(IndexType.HASH, "f");

        for (int k = 0; k < 20; k++) {
            map.put(k, new Pojo(k));
        }

        assertEquals(1, map.values(Predicates.sql("%f=10")).size());
        assertEquals(9, map.values(Predicates.sql("%f>10")).size());
        assertEquals(10, map.values(Predicates.sql("%f>=10")).size());
        assertEquals(19, map.values(Predicates.sql("%f!=10")).size());
        assertEquals(10, map.values(Predicates.sql("%f<10")).size());
        assertEquals(11, map.values(Predicates.sql("%f<=10")).size());
        assertEquals(5, map.values(Predicates.sql("%f in (1,2,3,4,5)")).size());

        hz.shutdown();
    }

    @Test
    public void testSkipIndexIsIgnoredWhenNoIndex() {
        HazelcastInstance hz = getHazelcastInstance();
        IMap<Integer, Pojo> map = hz.getMap("foo");

        for (int k = 0; k < 20; k++) {
            map.put(k, new Pojo(k));
        }

        assertEquals(1, map.values(Predicates.sql("%f=10")).size());
        assertEquals(9, map.values(Predicates.sql("%f>10")).size());
        assertEquals(10, map.values(Predicates.sql("%f>=10")).size());
        assertEquals(19, map.values(Predicates.sql("%f!=10")).size());
        assertEquals(10, map.values(Predicates.sql("%f<10")).size());
        assertEquals(11, map.values(Predicates.sql("%f<=10")).size());
        assertEquals(5, map.values(Predicates.sql("%f in (1,2,3,4,5)")).size());

        hz.shutdown();
    }

    public abstract HazelcastInstance getHazelcastInstance();

    public static class Pojo implements Serializable {

        public int f;

        public Pojo(int f) {
            this.f = f;
        }

    }
}
