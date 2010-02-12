package com.hazelcast.monitor.client;

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.RemoteService;

public interface MapServiceAsync {
    void get(int clusterId, String name, String key, AsyncCallback<String> async);
}
