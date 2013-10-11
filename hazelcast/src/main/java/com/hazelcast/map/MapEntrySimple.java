package com.hazelcast.map;

import java.util.AbstractMap;
import java.util.Map;

/**
 * User: ahmetmircik
 * Date: 10/11/13
 * Time: 7:33 AM
 */
public class MapEntrySimple<K,V> extends AbstractMap.SimpleEntry<K,V> {

    private boolean modified = false;

    public MapEntrySimple(K key, V value) {
        super(key, value);
    }

    @Override
    public V setValue(V value) {
        modified = true;
        return super.setValue(value);
    }

    public boolean isModified() {
        return modified;
    }
}
