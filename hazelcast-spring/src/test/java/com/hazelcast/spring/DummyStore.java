package com.hazelcast.spring;

import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class DummyStore implements MapStore {
    public void store(final Object key, final Object value) {
        // not implemented
    }

    public void storeAll(final Map map) {
        // not implemented
    }

    public void delete(final Object key) {
        // not implemented
    }

    public void deleteAll(final Collection keys) {
        // not implemented
    }

    public Object load(final Object key) {
        // not implemented
        return null;
    }

    public Map loadAll(final Collection keys) {
        // not implemented
        return null;
    }

	public Set loadAllKeys() {
		// not implemented
		return null;
	}
}
