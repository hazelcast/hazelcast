package com.hazelcast.client.osgi;

import com.hazelcast.osgi.CheckDependenciesIT;

public class ClientCheckDependenciesIT extends CheckDependenciesIT {

    @Override
    protected boolean isMatching(String urlString) {
        return urlString.contains("hazelcast-client-") && urlString.contains("target");
    }
}
