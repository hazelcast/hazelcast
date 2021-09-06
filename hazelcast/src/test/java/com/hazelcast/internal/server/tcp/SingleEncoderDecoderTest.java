/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.networking.OutboundPipeline;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Objects;

import static com.hazelcast.instance.ProtocolType.CLIENT;
import static com.hazelcast.instance.ProtocolType.MEMBER;
import static com.hazelcast.internal.nio.Protocols.CLIENT_BINARY;
import static com.hazelcast.internal.nio.Protocols.CLUSTER;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SingleEncoderDecoderTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expect = ExpectedException.none();

    @Parameterized.Parameter
    public ProtocolType supportedProtocol;
    @Parameterized.Parameter(1)
    public String receivedProtocol;

    @Parameterized.Parameters(name = "supportedProtocol:{0}, receivedProtocol:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {MEMBER, CLUSTER},
                {MEMBER, CLIENT_BINARY},
                {MEMBER, "ASD"},
                {CLIENT, CLIENT_BINARY},
                {CLIENT, CLUSTER},
                {CLIENT, "ASD"},
        });
    }

    @Test
    public void testIncompatibleProtocolSentToClient() {
        // Set up encoder and decoder
        SingleProtocolEncoder protocolEncoder = new SingleProtocolEncoder((OutboundHandler) null);
        SingleProtocolDecoder protocolDecoder = new SingleProtocolDecoder(supportedProtocol, (InboundHandler) null, protocolEncoder);

        // Set up buffers
        ByteBuffer encoderDst = ByteBuffer.allocate(1000);
        ByteBuffer decoderSrc = ByteBuffer.allocate(1000);

        encoderDst.flip();
        decoderSrc.clear();

        decoderSrc.put(receivedProtocol.getBytes());

        protocolEncoder.dst(encoderDst);
        protocolDecoder.src(decoderSrc);

        // Mock channel in encoder
        Channel mockedChannel = mock(Channel.class);
        OutboundPipeline mockedPipeline = mock(OutboundPipeline.class);

        when(mockedChannel.outboundPipeline()).thenReturn(mockedPipeline);
        when(mockedPipeline.wakeup()).then(invocationOnMock -> {
            protocolEncoder.onWrite();
            return null;
        });
        protocolEncoder.setChannel(mockedChannel);

        // If protocols doesn't match expect ProtocolException else NPE is thrown because channel is mocked
        if (Objects.equals(supportedProtocol.getDescriptor(), receivedProtocol)) {
            expect.expect(NullPointerException.class);
        } else {
            expect.expect(ProtocolException.class);
        }
        protocolDecoder.onRead();
    }
}

