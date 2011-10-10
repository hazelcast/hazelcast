package com.hazelcast.impl;

import com.hazelcast.nio.Data;

public interface NearCacheRecord {

    Data getKeyData();
    
    Data getValueData();
    
    Object getValue() ;
    
    void setValueData(Data value);
    
    boolean hasValueData();
    
    void invalidate();
}
