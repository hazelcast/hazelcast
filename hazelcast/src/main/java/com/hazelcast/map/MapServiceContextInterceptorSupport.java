package com.hazelcast.map;

public interface MapServiceContextInterceptorSupport {

    void interceptAfterGet(String mapName, Object value);

    Object interceptPut(String mapName, Object oldValue, Object newValue);

    void interceptAfterPut(String mapName, Object newValue);

    Object interceptRemove(String mapName, Object value);

    void interceptAfterRemove(String mapName, Object value);

    String addInterceptor(String mapName, MapInterceptor interceptor);

    void removeInterceptor(String mapName, String id);

    Object interceptGet(String mapName, Object value);
}
