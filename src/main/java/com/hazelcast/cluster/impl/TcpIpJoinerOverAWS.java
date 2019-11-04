/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.aws.AwsConfig;
import com.hazelcast.aws.AwsDiscoveryStrategy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.TcpIpJoiner;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.Collection;

/**
 * Joiner used for the deprecated AwsConfig.
 *
 * @deprecated Use {@link AwsDiscoveryStrategy} instead of AwsConfig.
 */
@Deprecated
public class TcpIpJoinerOverAWS
        extends TcpIpJoiner {

    private final AWSClient aws;
    private final ILogger logger;

    public TcpIpJoinerOverAWS(Node node) {
        super(node);
        logger = node.getLogger(getClass());

        AwsConfig awsConfig = fromDeprecatedAwsConfig(node.getConfig().getNetworkConfig().getJoin().getAwsConfig());
        aws = new AWSClient(awsConfig);
    }

    static AwsConfig fromDeprecatedAwsConfig(com.hazelcast.config.AwsConfig awsConfig) {
        return AwsConfig.builder().setAccessKey(awsConfig.getProperty("access-key"))
                .setSecretKey(awsConfig.getProperty("secret-key"))
                .setRegion(awsConfig.getProperty("region"))
                .setSecurityGroupName(awsConfig.getProperty("security-group-name"))
                .setTagKey(awsConfig.getProperty("tag-key"))
                .setTagValue(awsConfig.getProperty("tag-value"))
                .setHostHeader(awsConfig.getProperty("host-header"))
                .setIamRole(awsConfig.getProperty("iam-role")).build();
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
    public String getType() {
        return "aws";
    }
}
