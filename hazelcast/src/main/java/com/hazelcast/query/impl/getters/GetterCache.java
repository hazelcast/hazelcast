package com.hazelcast.query.impl.getters;

import javax.annotation.Nullable;

public interface GetterCache {
    @Nullable
    Getter getGetter(Class clazz, String attributeName);

    Getter putGetter(Class clazz, String attributeName, Getter getter);

    enum Type {
        EVICTABLE,
        NOT_EVICTABLE,
    }
}
