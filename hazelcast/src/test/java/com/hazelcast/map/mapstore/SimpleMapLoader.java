package com.hazelcast.map.mapstore;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoader;

public class SimpleMapLoader implements MapLoader {

    final int size;
    final boolean slow;
    private HazelcastInstance hz;

    SimpleMapLoader(int size, boolean slow) {
        this.size = size;
        this.slow = slow;
    }

    @Override
    public Object load(Object key) {
        return null;
    }

    @Override
    public Map loadAll(Collection keys) {

        if (slow) {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Map result = new HashMap();
        for (Object key : keys) {
            result.put(key, key);
        }
        return result;
    }

    @Override
    public Set loadAllKeys() {

        Set keys = new HashSet();
        for (int i = 0; i < size; i++) {
            keys.add(i);
        }
        return keys;
    }

}