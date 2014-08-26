package com.hazelcast.map.merge;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;

/**
 * A provider for {@link com.hazelcast.map.merge.MergePolicyProvider} instances.
 */
public final class MergePolicyProvider {

    private final ConcurrentMap<String, MapMergePolicy> mergePolicyMap;

    private final NodeEngine nodeEngine;

    private final ConstructorFunction<String, MapMergePolicy> policyConstructorFunction
            = new ConstructorFunction<String, MapMergePolicy>() {
        @Override
        public MapMergePolicy createNew(String className) {
            try {
                return newInstance(nodeEngine.getConfigClassLoader(), className);
            } catch (Exception e) {
                nodeEngine.getLogger(getClass()).severe(e);
                throw ExceptionUtil.rethrow(e);
            }
        }
    };

    public MergePolicyProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        mergePolicyMap = new ConcurrentHashMap<String, MapMergePolicy>();
        addOutOfBoxPolicies();
    }

    private void addOutOfBoxPolicies() {
        mergePolicyMap.put(PutIfAbsentMapMergePolicy.class.getName(), new PutIfAbsentMapMergePolicy());
        mergePolicyMap.put(HigherHitsMapMergePolicy.class.getName(), new HigherHitsMapMergePolicy());
        mergePolicyMap.put(PassThroughMergePolicy.class.getName(), new PassThroughMergePolicy());
        mergePolicyMap.put(LatestUpdateMapMergePolicy.class.getName(), new LatestUpdateMapMergePolicy());
    }

    public MapMergePolicy getMergePolicy(String className) {
        if (className == null) {
            throw new NullPointerException("Class name is mandatory!");
        }
        return ConcurrencyUtil.getOrPutIfAbsent(mergePolicyMap, className, policyConstructorFunction);
    }
}
