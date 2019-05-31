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

package com.hazelcast.aws;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AwsClientTest {

    private static AwsConfig.Builder predefinedAwsConfigBuilder() {
        return AwsConfig.builder().setHostHeader("ec2.amazonaws.com").setRegion("us-east-1").setConnectionTimeoutSeconds(5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAwsClient_whenNoAwsConfig() {
        new AWSClient(null);
    }

    @Test
    public void testAwsClient_getEndPoint() {
        AwsConfig awsConfig = predefinedAwsConfigBuilder().setIamRole("test").build();
        AWSClient awsClient = new AWSClient(awsConfig);
        assertEquals("ec2.us-east-1.amazonaws.com", awsClient.getEndpoint());
    }

    @Test
    public void testAwsClient_withDifferentHostHeader() {
        AwsConfig awsConfig = predefinedAwsConfigBuilder().setIamRole("test").setHostHeader("ec2.amazonaws.com.cn")
                                                          .setRegion("cn-north-1").build();
        AWSClient awsClient = new AWSClient(awsConfig);
        assertEquals("ec2.cn-north-1.amazonaws.com.cn", awsClient.getEndpoint());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testAwsClient_withInvalidHostHeader() {
        AwsConfig awsConfig = predefinedAwsConfigBuilder().setIamRole("test").setHostHeader("ec3.amazonaws.com.cn").build();
        new AWSClient(awsConfig);
    }
}
