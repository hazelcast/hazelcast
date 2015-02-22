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

package com.hazelcast.cluster.impl;

import java.util.Collection;

import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.kubernetes.KubernetesClient;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ExceptionUtil;

public class TcpIpJoinerOverKubernetes extends TcpIpJoiner {

    final KubernetesClient kubernetesClient;
    final ILogger logger;

    public TcpIpJoinerOverKubernetes(Node node) {
        super(node);
        logger = node.getLogger(getClass());
        KubernetesConfig kubernetesConfig = node.getConfig().getNetworkConfig().getJoin().getKubernetesConfig();
        kubernetesClient = new KubernetesClient(kubernetesConfig);
    }

    @Override
    protected Collection<String> getMembers() {
        logger.info("Retrieving members from Kubernetes");
        try {
            Collection<String> list = kubernetesClient.getPodIpAddresses();
            if (list.isEmpty()) {
                logger.warning("No instances found in Kubernetes!");
            } else {
                if (logger.isFinestEnabled()) {
                    StringBuilder sb = new StringBuilder("Found the following instances in Kubernetes:\n");
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
        KubernetesConfig kubernetesConfig = node.getConfig().getNetworkConfig().getJoin().getKubernetesConfig();
        return kubernetesConfig.getConnectionTimeoutSeconds();
    }

    @Override
    public String getType() {
        return "kubernetes";
    }
}

