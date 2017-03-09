/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import com.hazelcast.internal.networking.SocketChannelWrapper;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DefaultSocketChannelWrapperFactoryTest extends HazelcastTestSupport {

    private DefaultSocketChannelWrapperFactory factory;

    @Before
    public void setup() {
        factory = new DefaultSocketChannelWrapperFactory();
    }

    @Test
    public void wrapSocketChannel() throws Exception {
        SocketChannel socketChannel = mock(SocketChannel.class);
        SocketChannelWrapper wrapper = factory.wrapSocketChannel(socketChannel, false);

        assertInstanceOf(DefaultSocketChannelWrapper.class, wrapper);
    }

    @Test
    public void isSSlEnabled() {
        boolean result = factory.isSSlEnabled();
        assertFalse(result);
    }
}
