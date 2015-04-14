package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.annotation.EncodeMethod;
import com.hazelcast.annotation.GenerateParameters;
import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.Set;

@GenerateParameters(id = 3, name = "Set", ns = "Hazelcast.Client.Protocol.Set")
public interface SetTemplate {

    @EncodeMethod(id = 1)
    void size(String name);

    @EncodeMethod(id = 2)
    void contains(String name, Data value);

    @EncodeMethod(id = 2)
    void containsAll(String name, Set<Data> valueSet);

    @EncodeMethod(id = 3)
    void add(String name, Data value);

    @EncodeMethod(id = 4)
    void remove(String name, Data value);

    @EncodeMethod(id = 5)
    void addAll(String name, List<Data> valueList);

    @EncodeMethod(id = 6)
    void compareAndRemoveAll(String name, Set<Data> valueSet);

    @EncodeMethod(id = 7)
    void compareAndRetainAll(String name, Set<Data> valueSet);

    @EncodeMethod(id = 8)
    void clear(String name);

    @EncodeMethod(id = 9)
    void getAll(String name);

    @EncodeMethod(id = 10)
    void addListener(String name, boolean includeValue);

    @EncodeMethod(id = 11)
    void removeListener(String name, String registrationId);

    @EncodeMethod(id = 12)
    void isEmpty(String name);

}
