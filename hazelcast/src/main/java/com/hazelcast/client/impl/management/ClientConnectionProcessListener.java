package com.hazelcast.client.impl.management;

import com.hazelcast.cluster.Address;

import java.util.EventListener;

public interface ClientConnectionProcessListener
        extends EventListener {

    void attemptingToConnectToAddress(Address address);

    void connectionAttemptFailed(Object target);
}
