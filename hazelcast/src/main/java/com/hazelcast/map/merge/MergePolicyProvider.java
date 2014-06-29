package com.hazelcast.map.merge;

import com.hazelcast.config.MapConfig;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MergePolicyProvider {

    private Map<String, MapMergePolicy> mergePolicyMap;

    private NodeEngine nodeEngine;

    public MergePolicyProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        mergePolicyMap = new ConcurrentHashMap<String, MapMergePolicy>();
        addMergePolicies();
    }

    private void addMergePolicies() {
        mergePolicyMap.put(PutIfAbsentMapMergePolicy.class.getName(), new PutIfAbsentMapMergePolicy());
        mergePolicyMap.put(HigherHitsMapMergePolicy.class.getName(), new HigherHitsMapMergePolicy());
        mergePolicyMap.put(PassThroughMergePolicy.class.getName(), new PassThroughMergePolicy());
        mergePolicyMap.put(LatestUpdateMapMergePolicy.class.getName(), new LatestUpdateMapMergePolicy());
    }

    public MapMergePolicy getMergePolicy(String mergePolicyName) {
        MapMergePolicy mergePolicy = mergePolicyMap.get(mergePolicyName);
        if (mergePolicy == null && mergePolicyName != null) {
            try {

                // check if user has entered custom class name instead of policy name
                mergePolicy = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), mergePolicyName);
                mergePolicyMap.put(mergePolicyName, mergePolicy);
            } catch (Exception e) {
                nodeEngine.getLogger(getClass()).severe(e);
                throw ExceptionUtil.rethrow(e);
            }
        }
        if (mergePolicy == null) {
            return mergePolicyMap.get(MapConfig.DEFAULT_MAP_MERGE_POLICY);
        }
        return mergePolicy;
    }
}
