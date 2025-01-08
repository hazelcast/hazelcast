/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryProcessorInterceptorTest extends InterceptorTest {

    @Override
    protected void put(IMap map, Object key, Object value) {
        map.executeOnKey(key, new Putter(value));
    }

    @Override
    protected Object get(IMap map, Object key) {
        return map.executeOnKey(key, new Getter());
    }

    @Override
    protected Object remove(IMap map, Object key) {
        return map.executeOnKey(key, new Remover());
    }

    private static class Putter implements EntryProcessor {

        private Object value;

        Putter(Object value) {
            this.value = value;
        }

        @Override
        public Object process(Map.Entry entry) {
            entry.setValue(value);
            return null;
        }
    }

    private static class Getter implements EntryProcessor {

        Getter() {
        }

        @Override
        public Object process(Map.Entry entry) {
            return entry.getValue();
        }
    }

    private static class Remover implements EntryProcessor {

        Remover() {
        }

        @Override
        public Object process(Map.Entry entry) {
            Object oldValue = entry.getValue();
            entry.setValue(null);
            return oldValue;
        }
    }
}
