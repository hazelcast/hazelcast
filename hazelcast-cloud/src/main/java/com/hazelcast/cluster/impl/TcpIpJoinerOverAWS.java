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

import com.hazelcast.aws.AWSClient;
import com.hazelcast.cluster.impl.TcpIpJoiner;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;

public class TcpIpJoinerOverAWS extends TcpIpJoiner {

    final AWSClient aws;
    final ILogger logger;

    public TcpIpJoinerOverAWS(Node node) {
        super(node);
        logger = node.getLogger(getClass());
        AwsConfig awsConfig = node.getConfig().getNetworkConfig().getJoin().getAwsConfig();
        aws = new AWSClient(awsConfig);
        if (awsConfig.getRegion() != null && awsConfig.getRegion().length() > 0) {
            aws.setEndpoint("ec2." + awsConfig.getRegion() + ".amazonaws.com");
        }
    }

    @Override
    protected Collection<String> getMembers() {
        try {
            Collection<String> list = aws.getPrivateIpAddresses();
            if (list.isEmpty()) {
                logger.warning("No EC2 instances found!");
            } else {
                if (logger.isFinestEnabled()) {
                    StringBuilder sb = new StringBuilder("Found the following EC2 instances:\n");
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
        AwsConfig awsConfig = node.getConfig().getNetworkConfig().getJoin().getAwsConfig();
        return awsConfig.getConnectionTimeoutSeconds();
    }

    @Override
    public String getType() {
        return "aws";
    }
}

