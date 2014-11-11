/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.cluster.impl.TcpIpJoinerOverAWS;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.instance.DefaultNodeContext;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpIpJoinerOverAWSTest extends HazelcastTestSupport {

    @Test
    public void testJoinerCreation() {
        Config config = new Config();
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);

        AwsConfig awsConfig = join.getAwsConfig();
        awsConfig.setEnabled(true)
                .setAccessKey(randomString())
                .setSecretKey(randomString());

        HazelcastInstanceImpl instance = Mockito.mock(HazelcastInstanceImpl.class);

        // keep ref to be able to close socketchannel
        final AtomicReference<ServerSocketChannel> channelRef = new AtomicReference<ServerSocketChannel>();
        NodeContext nodeContext = new DefaultNodeContext() {
            @Override
            public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
                channelRef.set(serverSocketChannel);
                return super.createConnectionManager(node, serverSocketChannel);
            }
        };

        Node node = new Node(instance, config, nodeContext);
        try {
            channelRef.get().close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Joiner joiner = node.getJoiner();
        Assert.assertNotNull(joiner);
        Assert.assertEquals(TcpIpJoinerOverAWS.class, joiner.getClass());
    }
}
