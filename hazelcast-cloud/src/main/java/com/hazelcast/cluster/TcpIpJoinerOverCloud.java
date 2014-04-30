/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 */
package com.hazelcast.cluster;

import com.hazelcast.cloud.CloudClient;
import com.hazelcast.config.CloudConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ExceptionUtil;
import java.util.Collection;
import java.util.List;

public class TcpIpJoinerOverCloud extends TcpIpJoiner {

    final CloudClient cloud;
    final ILogger logger;

    public TcpIpJoinerOverCloud(Node node) {
        super(node);
        logger = node.getLogger(getClass());
        CloudConfig cloudConfig = node.getConfig().getNetworkConfig().getJoin()
                .getCloudConfig();
        cloud = new CloudClient(cloudConfig);
    }

    @Override
    protected Collection<String> getMembers() {
        try {
            List<String> list = cloud.getPrivateIpAddresses();
            if (list.isEmpty()) {
                logger.warning("No cloud instances found!");
            } else {
                if (logger.isFinestEnabled()) {
                    StringBuilder sb = new StringBuilder(
                            "Found the following cloud instances:\n");
                    for (String ip : list) {
                        sb.append("    ").append(ip).append("\n");
                    }
                    logger.finest(sb.toString());
                }
            }
            return list;
        } catch (Exception e) {
            logger.warning(e);
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected int getConnTimeoutSeconds() {
        CloudConfig cloudConfig = node.getConfig().getNetworkConfig().getJoin()
                .getCloudConfig();
        return cloudConfig.getConnectionTimeoutSeconds();
    }

    @Override
    public String getType() {
        return "cloud";
    }
}
