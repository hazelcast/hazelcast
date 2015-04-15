package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.annotation.EncodeMethod;
import com.hazelcast.annotation.GenerateParameters;
import com.hazelcast.nio.serialization.Data;
import org.omg.CORBA.DATA_CONVERSION;

import java.util.List;
import java.util.Set;

@GenerateParameters(id = 4, name = "List", ns = "Hazelcast.Client.Protocol.List")
public interface ListTemplate {

    //COLLECTION PARAMS
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
    void compareAndRemove(String name, Set<Data> valueSet, boolean retain);

    @EncodeMethod(id = 7)
    void clear(String name);

    @EncodeMethod(id = 8)
    void getAll(String name);

    @EncodeMethod(id = 9)
    void addListener(String name, boolean includeValue);

    @EncodeMethod(id = 10)
    void removeListener(String name);

    @EncodeMethod(id = 11)
    void isEmpty(String name);

    //LIST PARAMS
    @EncodeMethod(id = 12)
    void addAllWithIndex(String name, int index, List<Data> valueList);

    @EncodeMethod(id = 13)
    void get(String name, int index);

    @EncodeMethod(id = 14)
    void set(String name, int index, Data value);

    @EncodeMethod(id = 15)
    void addWithIndex(String name, int index, Data value);

    @EncodeMethod(id = 16)
    void removeWithIndex(String name, int index, Data value);

    @EncodeMethod(id = 17)
    void lastIndexOf(String name, Data value);

    @EncodeMethod(id = 18)
    void indexOf(String name, Data value);

    @EncodeMethod(id = 19)
    void sub(String name, int from, int to);

}
