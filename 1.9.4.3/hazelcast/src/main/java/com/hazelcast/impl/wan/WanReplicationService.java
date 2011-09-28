/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.impl.wan;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.impl.Node;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WanReplicationService {
    final Node node;
    final Map<String, WanReplication> mapWanReplications = new ConcurrentHashMap<String, WanReplication>(2);

    public WanReplicationService(Node node) {
        this.node = node;
    }

    public WanReplication getWanReplication(String name) {
        WanReplication wr = mapWanReplications.get(name);
        if (wr != null) return wr;
        synchronized (this) {
            wr = mapWanReplications.get(name);
            if (wr != null) return wr;
            WanReplicationConfig wanReplicationConfig = node.getConfig().getWanReplicationConfig(name);
            if (wanReplicationConfig == null) return null;
            List<WanTargetClusterConfig> targets = wanReplicationConfig.getTargetClusterConfigs();
            WanReplicationEndpoint[] targetClusters = new WanReplicationEndpoint[targets.size()];
            int count = 0;
            for (WanTargetClusterConfig targetClusterConfig : targets) {
                WanReplicationEndpoint target = new WanNoDelayReplication();
                String groupName = targetClusterConfig.getGroupName();
                String password = targetClusterConfig.getGroupPassword();
                String[] addresses = new String[targetClusterConfig.getEndpoints().size()];
                targetClusterConfig.getEndpoints().toArray(addresses);
                target.init(node, groupName, password, addresses);
                targetClusters[count++] = target;
            }
            wr = new WanReplication(name, targetClusters);
            mapWanReplications.put(name, wr);
            return wr;
        }
    }
}
