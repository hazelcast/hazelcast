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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;

import java.util.ArrayList;
import java.util.List;

public class TcpIpJoinerOverAWS extends TcpIpJoiner {
    final AmazonEC2Client ec2;

    public TcpIpJoinerOverAWS(Node node) {
        super(node);
        AwsConfig awsConfig = node.getConfig().getNetworkConfig().getJoin().getAwsConfig();
        AWSCredentials credentials = new BasicAWSCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey());
        ec2 = new AmazonEC2Client(credentials);
    }

    @Override
    protected List<String> getMembers(Config config) {
        List<String> possibleMembers = new ArrayList<String>();
        DescribeInstancesResult result = ec2.describeInstances(new DescribeInstancesRequest());
        for (Reservation reservation : result.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                System.out.println(instance);
                String ip = instance.getPrivateIpAddress();
                if (ip != null) {
                    possibleMembers.add(ip);
                }
            }
        }
        return possibleMembers;
    }
}
