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

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class TcpIpJoinerOverAWSTest extends TestCase {

    @Test
    public void testAWSConfig() throws IOException {
        Config config = new Config();
        config.setPortAutoIncrement(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAwsConfig().setEnabled(true);
        Properties bundle = new Properties();
        bundle.load(new FileInputStream(new File("/Users/Malikov/.aws/AwsCredentials.properties")));
        config.getNetworkConfig().getJoin().getAwsConfig().setAccessKey(bundle.getProperty("accessKey"));
        config.getNetworkConfig().getJoin().getAwsConfig().setSecretKey(bundle.getProperty("secretKey"));
//        config.getNetworkConfig().getJoin().getAwsConfig().setRegion("us-west-1");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        h2.getLifecycleService().shutdown();
    }
}
