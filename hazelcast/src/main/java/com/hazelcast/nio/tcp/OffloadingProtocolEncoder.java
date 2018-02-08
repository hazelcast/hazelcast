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

package com.hazelcast.nio.tcp;

import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.WriteResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;

import java.net.SocketException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.WriteResult.BLOCKED;
import static com.hazelcast.internal.networking.WriteResult.CLEAN;
import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class OffloadingProtocolEncoder extends ChannelOutboundHandler {

    private final ILogger logger;
    private final IOService ioService;
    private boolean blocked;
    private volatile boolean protocolProcessed;

    public OffloadingProtocolEncoder(ILogger logger, IOService ioService) {
        this.logger = logger;
        this.ioService = ioService;
    }

    @Override
    public WriteResult onWrite() {
        System.out.println(channel + " ProtocolEncoder.onWrite:" + frame +" protocolProcessed:"+protocolProcessed+" blocked:"+blocked);

        if (protocolProcessed) {
            complete();
            return CLEAN;
        }

        if (blocked) {
            System.out.println(channel + " ProtocolEncoder.onWrite bypassing, with frame:" + frame);
            return BLOCKED;
        }

        blocked = true;

        // we doing bogus protocol offloading to see what we need to reschedule
        new Thread() {
            @Override
            public void run() {
                System.out.println(channel + " Starting the protocol offloading");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(channel + " ============================= Completing protocol offloading");
                protocolProcessed = true;

                channel.flushOutboundPipeline();
            }
        }.start();

        return BLOCKED;
    }

    private void complete() {
        System.out.println(channel + " Completing protocol offload with: " + frame);

        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);

        ChannelOutboundHandler handler = ioService.createOutboundHandler(connection);
        replaceBy(handler);

        // the first bytes we send contain the cluster protocol
        ByteBuffer outputBuffer = newOutputBuffer(ioService.getSocketSendBufferSize());
        outputBuffer.put(stringToBytes(CLUSTER));

        handler.dst = outputBuffer;
        if(handler.next!=null) {
            handler.next.dst = outputBuffer;
        }
    }

    //
//    private InitResult<ChannelOutboundHandler> initOutboundClientProtocol(Channel channel) {
//        ChannelOutboundHandler outboundHandler = new ClientChannelOutboundHandler();
//
//        ByteBuffer outputBuffer = newOutputBuffer(channel, ioService.getSocketClientSendBufferSize());
//
//        return new InitResult<ChannelOutboundHandler>(outputBuffer, outboundHandler);
//    }
//
//    private InitResult<ChannelOutboundHandler> initOutboundTextProtocol(Channel channel) {
//        ChannelOutboundHandler outboundHandler = (ChannelOutboundHandler) channel.attributeMap().get(TEXT_OUTBOUND_HANDLER);
//
//        ByteBuffer outputBuffer = newOutputBuffer(channel, ioService.getSocketClientSendBufferSize());
//
//        return new InitResult<ChannelOutboundHandler>(outputBuffer, outboundHandler);
//    }
//
    private ByteBuffer newOutputBuffer(int sizeKb) {
        int size = KILO_BYTE * sizeKb;

        ByteBuffer outputBuffer = newByteBuffer(size, ioService.useDirectSocketBuffer());

        try {
            channel.socket().setSendBufferSize(size);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP send buffer of " + channel + " to " + size + " B.", e);
        }

        return outputBuffer;
    }
}
