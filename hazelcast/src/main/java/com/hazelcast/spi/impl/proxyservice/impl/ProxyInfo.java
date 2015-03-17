package com.hazelcast.spi.impl.proxyservice.impl;

public final class ProxyInfo {
    private final String serviceName;
    private final String objectName;

    public ProxyInfo(String serviceName, String objectName) {
        this.serviceName = serviceName;
        this.objectName = objectName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getObjectName() {
        return objectName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProxyInfo{");
        sb.append("serviceName='").append(serviceName).append('\'');
        sb.append(", objectName='").append(objectName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
