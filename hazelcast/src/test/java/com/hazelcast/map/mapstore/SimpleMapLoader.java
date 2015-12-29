package com.hazelcast.map.mapstore;

import com.hazelcast.core.MapLoader;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SimpleMapLoader implements MapLoader<Integer, Integer> {

    final int size;
    final boolean slow;

    SimpleMapLoader(int size, boolean slow) {
        this.size = size;
        this.slow = slow;
    }

    @Override
    public Integer load(Integer key) {
        return null;
    }

    @Override
    public Map<Integer, Integer> loadAll(Collection<Integer> keys) {

        if (slow) {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Map<Integer, Integer> result = new HashMap<Integer, Integer>();
        for (Integer key : keys) {
            result.put(key, key);
        }
        return result;
    }

    @Override
    public Iterable<Integer> loadAllKeys() {

        Set<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < size; i++) {
            keys.add(i);
        }
        return keys;
    }

}