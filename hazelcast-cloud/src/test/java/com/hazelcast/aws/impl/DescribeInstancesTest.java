/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aws.impl;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DescribeInstancesTest {

    @Test(expected = IllegalArgumentException.class)
    public void test_whenAwsConfigIsNull() {
        new DescribeInstances(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_whenAccessKeyNull() {
        new DescribeInstances(new AwsConfig());
    }
    
    @Test
    public void test_whenProperConfig() {
        AwsConfig awsConfig = new AwsConfig();
        awsConfig.setAccessKey("accesskey");
        awsConfig.setSecretKey("secretkey");
        new DescribeInstances(awsConfig);
    }

    @Test
    public void test_whenGivenCredentialsProvider() {
        AwsConfig awsConfig = new AwsConfig();
        awsConfig.setAwsCredentialsProvider(new StaticCredentialsProvider(
                new BasicAWSCredentials("accesskey", "secretkey")));
        new DescribeInstances(awsConfig);

        assertEquals("accesskey", awsConfig.getAccessKey());
        assertEquals("secretkey", awsConfig.getSecretKey());

        awsConfig.setAwsCredentialsProvider(new StaticCredentialsProvider(
                new BasicSessionCredentials("accesskey", "secretkey", "token")));
        Map<String, String> attributes = new DescribeInstances(awsConfig).attributes;

        assertEquals("token", attributes.get("X-Amz-Security-Token"));
    }

    @Test
    public void test_endpointInNonUsEast1Region() {
        AwsConfig awsConfig = new AwsConfig();
        awsConfig.setAccessKey("accesskey");
        awsConfig.setSecretKey("secretkey");
        awsConfig.setRegion("us-west-1");

        assertEquals("ec2.us-west-1.amazonaws.com", new DescribeInstances(awsConfig).endpoint);
    }
}
