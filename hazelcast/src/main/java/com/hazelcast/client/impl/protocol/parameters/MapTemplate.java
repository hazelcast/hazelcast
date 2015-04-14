package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.annotation.EncodeMethod;
import com.hazelcast.annotation.GenerateParameters;

@GenerateParameters(id = 1, name = "Map", ns = "Hazelcast.Client.Protocol.Map")
public interface MapTemplate {

    @EncodeMethod(id = 1)
    void put(String name, byte[] key, byte[] value, long threadId, long ttl);

    @EncodeMethod(id = 2)
    void get(String name, byte[] key, long threadId);

    @EncodeMethod(id = 3)
    void remove(String name, byte[] key, long threadId);

    @EncodeMethod(id = 4)
    void replace(String name, byte[] key, byte[] value, long threadId);

    @EncodeMethod(id = 5)
    void replaceIfSame(String name, byte[] key, byte[] testValue, byte[] value, long threadId);

    @EncodeMethod(id = 6)
    void addEntryListener(String name, byte[] key, byte[] predicate, boolean includeValue);

    @EncodeMethod(id = 7)
    void addSqlEntryListener(String name, byte[] key, String sqlPredicate, boolean includeValue);

    @EncodeMethod(id = 8)
    void addNearCacheEntryListener(String name, boolean includeValue);

    @EncodeMethod(id = 9)
    void putAsync(String name, byte[] key, byte[] value, long threadId, long ttl);

    @EncodeMethod(id = 10)
    void getAsync(String name, byte[] key, long threadId);

    @EncodeMethod(id = 11)
    void removeAsync(String name, byte[] key, long threadId);

    @EncodeMethod(id = 12)
    void containsKey(String name, byte[] key, long threadId);

    @EncodeMethod(id = 13)
    void containsValue(String name, byte[] value);

    @EncodeMethod(id = 14)
    void removeIfSame(String name, byte[] key, byte[] value, long threadId);

    @EncodeMethod(id = 15)
    void delete(String name, byte[] key, long threadId);

    @EncodeMethod(id = 16)
    void flush();
}
