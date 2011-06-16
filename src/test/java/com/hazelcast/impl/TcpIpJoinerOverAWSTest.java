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
import com.amazonaws.auth.PropertiesCredentials;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TcpIpJoinerOverAWSTest extends TestCase {

    @Test
    public void testAWSConfig() throws IOException {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAwsConfig().setEnabled(true);
        AWSCredentials credentials = new PropertiesCredentials(new File("/Users/Malikov/.aws/AwsCredentials.properties"));
        config.getNetworkConfig().getJoin().getAwsConfig().setAccessKey(credentials.getAWSAccessKeyId());
        config.getNetworkConfig().getJoin().getAwsConfig().setSecretKey(credentials.getAWSSecretKey());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
    }
}
