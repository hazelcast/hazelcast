/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package classloading.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class IntMapLoader implements com.hazelcast.core.MapLoader<Integer, Integer>, Serializable {
    @Override
    public Integer load(Integer integer) {
        return integer;
    }

    @Override
    public Map<Integer, Integer> loadAll(Collection<Integer> collection) {
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        for (Integer value : collection) {
            map.put(value, value);
        }
        return map;
    }

    @Override
    public Iterable<Integer> loadAllKeys() {
        Collection<Integer> keys = new ArrayList<Integer>();
        for (int i = 0; i < 10000; i++) {
            keys.add(i);
        }
        return keys;
    }
}
