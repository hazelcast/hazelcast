package com.hazelcast.instance;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.AddressLocator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.TimeUnit;


public class DelegatingAddressPicker implements AddressPicker {
    //todo: this should be shared with DefaultAddressPicker
    private static final int SOCKET_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(1);
    private static final int SOCKET_BACKLOG_LENGTH = 100;

    private final AddressLocator addressLocator;
    private final NetworkConfig networkConfig;

    private volatile InetSocketAddress bindAddress;
    private volatile InetSocketAddress publicAddress;

    private volatile ServerSocketChannel serverSocketChannel;

    public DelegatingAddressPicker(AddressLocator addressLocator, NetworkConfig networkConfig) {
        this.addressLocator = addressLocator;
        this.networkConfig = networkConfig;
    }


    @Override
    public void pickAddress() throws Exception {
        bindAddress = addressLocator.getBindAddress();
        publicAddress = addressLocator.getPublicAddress();
        validatePublicAddress(publicAddress);
        serverSocketChannel = createServerSocketChannelInternal();

        if (publicAddress.getPort() == 0) {
            publicAddress = new InetSocketAddress(publicAddress.getAddress(), serverSocketChannel.socket().getLocalPort());
        }
    }


    private ServerSocketChannel createServerSocketChannelInternal() {
        int portCount = networkConfig.getPortCount();
        int port = bindAddress.getPort() == 0 ? networkConfig.getPort() : bindAddress.getPort();
        boolean portAutoIncrement = networkConfig.isPortAutoIncrement();

        int portTrialCount = port > 0 && portAutoIncrement ? portCount : 1;
        IOException error = null;
        for (int i = 0; i < portTrialCount; i++) {
            InetSocketAddress tmpBindAddress = new InetSocketAddress(bindAddress.getAddress(), port + i);
            boolean reuseAddress = networkConfig.isReuseAddress();
            ServerSocket serverSocket = null;
            ServerSocketChannel serverSocketChannel = null;
            try {
                serverSocketChannel = ServerSocketChannel.open();
                serverSocket = serverSocketChannel.socket();
                serverSocket.setReuseAddress(reuseAddress);
                serverSocket.setSoTimeout(SOCKET_TIMEOUT_MILLIS);
                serverSocket.bind(tmpBindAddress, SOCKET_BACKLOG_LENGTH);

                //todo: ugly side-effect
                bindAddress = tmpBindAddress;
                return serverSocketChannel;
            } catch (IOException e) {
                IOUtil.close(serverSocket);
                IOUtil.closeResource(serverSocketChannel);
                error = e;
            }
        }
        throw new IllegalStateException("Cannot bind to a given address" + bindAddress, error);
    }

    private void validatePublicAddress(InetSocketAddress inetSocketAddress) {
        if (inetSocketAddress.getAddress().isAnyLocalAddress()) {
            throw new IllegalStateException("Address locator has to return a specific public address to broadcast to"
                    + " other members.");
        }
    }

    @Override
    public Address getBindAddress() {
        return new Address(bindAddress);
    }

    @Override
    public Address getPublicAddress() {
        return new Address(publicAddress);
    }

    @Override
    public ServerSocketChannel getServerSocketChannel() {
        return serverSocketChannel;
    }
}
