/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.rest.service;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.builtin.ErrorsCodec;
import com.hazelcast.client.impl.protocol.exception.ErrorHolder;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelOptions;
import com.hazelcast.internal.networking.InboundPipeline;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.networking.OutboundPipeline;
import com.hazelcast.internal.networking.nio.AbstractChannel;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.server.tcp.TcpServerConnectionManager;
import com.hazelcast.rest.security.CustomSecurityContext;
import com.hazelcast.rest.util.NodeEngineImplHolder;
import com.hazelcast.security.UsernamePasswordCredentials;
import org.springframework.stereotype.Service;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Service
public class LoginContextService {

    private final NodeEngineImplHolder nodeEngineImplHolder;
    private final CustomSecurityContext securityContext;

    public LoginContextService(NodeEngineImplHolder nodeEngineImplHolder, CustomSecurityContext securityContext) {
        this.nodeEngineImplHolder = nodeEngineImplHolder;
        this.securityContext = securityContext;
    }

    public LoginContext getLoginContext(UsernamePasswordCredentials credentials) {
        LocalChannel channel = new LocalChannel(nodeEngineImplHolder
                .getNodeEngine().getHazelcastInstance().getConfig().getNetworkConfig().getPort());

        TcpServerConnectionManager cm = (TcpServerConnectionManager) nodeEngineImplHolder.getNodeEngine().getNode().server
                .getConnectionManager(EndpointQualifier.CLIENT);
        LoginContext loginContext;
        try {
            loginContext = securityContext.getSecurityContext()
                    .createClientLoginContext(nodeEngineImplHolder
                                    .getNodeEngine().getHazelcastInstance().getConfig().getClusterName(),
                            credentials,
                            new LocalConnection(cm, channel));
        } catch (LoginException e) {
            System.out.println("error while getting login context: " + e.getMessage());
            throw new RuntimeException(e);
        }
        try {
            loginContext.login();
        } catch (LoginException e) {
            System.out.println("error login: " + e.getMessage());
            throw new RuntimeException(e);
        }
        System.out.println("loginContext: " + loginContext);
        return loginContext;
    }

    private static class LocalConnection extends TcpServerConnection {

        LocalConnection(TcpServerConnectionManager cm, Channel channel) {
            super(cm, new SimpleLifecycleListener(), 0, channel, true);
        }

        @Override
        public InetAddress getInetAddress() {
            return ((InetSocketAddress) getChannel().remoteSocketAddress()).getAddress();
        }

        public static class SimpleLifecycleListener implements ConnectionLifecycleListener<TcpServerConnection> {

            @Override
            public void onConnectionClose(TcpServerConnection connection, Throwable t, boolean silent) {
                if (t != null) {
                    t.printStackTrace();
                }
            }
        }
    }

    private static class LocalChannel extends AbstractChannel {

        private final BlockingQueue<ClientMessage> messageQueue = new ArrayBlockingQueue<>(10);
        // whatever address can be used there, it's not trying to bind to it
        private final SocketAddress localAddress = new InetSocketAddress("127.0.0.1", 8080);
        private final SocketAddress remoteAddress;
        private final int ten = 10;
        private final int timeout = 30;

        LocalChannel(int remotePort) {
            super(null, false);
            this.remoteAddress = new InetSocketAddress("127.0.0.1", remotePort);
        }

        protected ClientMessage waitForResponse(int responseMessageType) {
            ClientMessage cm = null;
            try {
                do {
                    cm = messageQueue.poll(timeout, TimeUnit.SECONDS);
                    if (cm.getMessageType() == ErrorsCodec.EXCEPTION_MESSAGE_TYPE) {
                        List<ErrorHolder> err = ErrorsCodec.decode(cm);
                        throw new ClientCallFailedException(err);
                    }
                } while (cm.getMessageType() != responseMessageType);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return cm;
        }

        @Override
        public SocketAddress remoteSocketAddress() {
            return remoteAddress;
        }

        @Override
        public SocketAddress localSocketAddress() {
            return localAddress;
        }

        @Override
        public ChannelOptions options() {
            // Auto-generated method stub
            return null;
        }

        @Override
        public InboundPipeline inboundPipeline() {
            // Auto-generated method stub
            return null;
        }

        @Override
        public OutboundPipeline outboundPipeline() {
            // Auto-generated method stub
            return null;
        }

        @Override
        public long lastReadTimeMillis() {
            return System.currentTimeMillis() - ten;
        }

        @Override
        public long lastWriteTimeMillis() {
            return System.currentTimeMillis() - ten;
        }

        @Override
        public void start() {
            // Auto-generated method stub

        }

        @Override
        public boolean write(OutboundFrame frame) {
            ClientMessage cm = (ClientMessage) frame;
            try {
                messageQueue.offer(cm, 1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }

        @Override
        public long bytesRead() {
            // Auto-generated method stub
            return 0;
        }

        @Override
        public long bytesWritten() {
            // Auto-generated method stub
            return 0;
        }
    }

    private static class ClientCallFailedException extends RuntimeException {

        private static final long serialVersionUID = 1L;
        private final List<ErrorHolder> err;

        ClientCallFailedException(List<ErrorHolder> err) {
            this.err = err;
        }

        public List<ErrorHolder> getErr() {
            return err;
        }
    }

}
