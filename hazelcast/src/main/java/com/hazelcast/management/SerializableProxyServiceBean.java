package com.hazelcast.management;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.ProxyService;
import java.io.IOException;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.ProxyServiceMBean}.
 */
public class SerializableProxyServiceBean implements DataSerializable {

    private int proxyCount;

    public SerializableProxyServiceBean() {
    }

    public SerializableProxyServiceBean(ProxyService proxyService) {
        this.proxyCount = proxyService.getProxyCount();
    }

    public int getProxyCount() {
        return proxyCount;
    }

    public void setProxyCount(int proxyCount) {
        this.proxyCount = proxyCount;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(proxyCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        proxyCount = in.readInt();
    }
}
