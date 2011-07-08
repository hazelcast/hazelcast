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
import com.hazelcast.aws.impl.AWSClient;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.ArrayList;
import java.util.List;

public class TcpIpJoinerOverAWS extends TcpIpJoiner {

    final AWSClient aws;
    final ILogger logger = Logger.getLogger(this.getClass().getName());
    final AmazonEC2Client ec2;

    public TcpIpJoinerOverAWS(Node node) {
        super(node);
        AwsConfig awsConfig = node.getConfig().getNetworkConfig().getJoin().getAwsConfig();
        aws = new AWSClient(awsConfig.getAccessKey(), awsConfig.getSecretKey());
        AWSCredentials credentials = new BasicAWSCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey());
        ec2 = new AmazonEC2Client(credentials);
        if (awsConfig.getRegion() != null && awsConfig.getRegion().length() > 0) {
            ec2.setEndpoint("ec2." + awsConfig.getRegion() + ".amazonaws.com");
            aws.setEndpoint("ec2." + awsConfig.getRegion() + ".amazonaws.com");
        }
    }
//    @Override
//    protected List<String> getMembers(Config config) {
//        try {
//            return aws.getPrivateDnsNames();
//        } catch (Exception e) {
//            logger.log(Level.WARNING, e.getMessage(), e);
//            return new ArrayList<String>();
//        }
//    }

    @Override
    protected List<String> getMembers(Config config) {
        List<String> possibleMembers = new ArrayList<String>();
        DescribeInstancesResult result = ec2.describeInstances(new DescribeInstancesRequest());
        for (Reservation reservation : result.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                if ("running".equalsIgnoreCase(instance.getState().getName())) {
                    String ip = instance.getPrivateIpAddress();
                    possibleMembers.add(ip);
                    System.out.println("IP is " + ip);
                }
            }
        }
        return possibleMembers;
    }
}
