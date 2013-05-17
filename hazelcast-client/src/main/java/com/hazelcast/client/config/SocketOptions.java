package com.hazelcast.client.config;

/**
 * @mdogan 5/17/13
 */
public class SocketOptions {

    // socket options

    private boolean socketTcpNoDelay = false;

    private boolean socketKeepAlive = true;

    private boolean socketReuseAddress = true;

    private int socketLingerSeconds = 3;

    private int socketTimeout = -1;

    private int socketBufferSize = 32; // in kb


    public boolean isSocketTcpNoDelay() {
        return socketTcpNoDelay;
    }

    public void setSocketTcpNoDelay(boolean socketTcpNoDelay) {
        this.socketTcpNoDelay = socketTcpNoDelay;
    }

    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    public void setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
    }

    public boolean isSocketReuseAddress() {
        return socketReuseAddress;
    }

    public void setSocketReuseAddress(boolean socketReuseAddress) {
        this.socketReuseAddress = socketReuseAddress;
    }

    public int getSocketLingerSeconds() {
        return socketLingerSeconds;
    }

    public void setSocketLingerSeconds(int socketLingerSeconds) {
        this.socketLingerSeconds = socketLingerSeconds;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    public void setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
    }
}
