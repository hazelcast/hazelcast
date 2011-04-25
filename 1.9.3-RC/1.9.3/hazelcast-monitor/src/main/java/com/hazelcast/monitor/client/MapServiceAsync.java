package com.hazelcast.monitor.client;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface MapServiceAsync {

    void get(int clusterId, String name, String key, AsyncCallback<MapEntry> async);
    
    void getEntries(int clusterId, String name, AsyncCallback<MapEntry[]> async);
}
