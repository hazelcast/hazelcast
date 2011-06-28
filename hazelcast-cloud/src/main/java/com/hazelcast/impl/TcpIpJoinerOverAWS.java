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

package com.hazelcast.impl;

import com.hazelcast.aws.impl.AWSClient;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;

import java.util.ArrayList;
import java.util.List;

public class TcpIpJoinerOverAWS extends TcpIpJoiner {

    final AWSClient aws;

    public TcpIpJoinerOverAWS(Node node) {
        super(node);
        AwsConfig awsConfig = node.getConfig().getNetworkConfig().getJoin().getAwsConfig();
        aws = new AWSClient(awsConfig.getAccessKey(), awsConfig.getSecretKey());
    }

    @Override
    protected List<String> getMembers(Config config) {
        try {
            return aws.getPrivateDnsNames();
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<String>();
        }
    }
}
