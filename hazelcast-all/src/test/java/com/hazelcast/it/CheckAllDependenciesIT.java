package com.hazelcast.it;

import com.hazelcast.osgi.CheckDependenciesIT;

public class CheckAllDependenciesIT extends CheckDependenciesIT {

    @Override
    protected boolean isMatching(String urlString) {
        return urlString.contains("hazelcast-all-") && urlString.contains("target");
    }
}
