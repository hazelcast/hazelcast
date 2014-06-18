package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.spi.ProxyService;

import static com.hazelcast.util.JsonUtil.getInt;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.ProxyServiceMBean}.
 */
public class SerializableProxyServiceBean implements JsonSerializable {

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
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("proxyCount", proxyCount);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        proxyCount = getInt(json, "proxyCount", -1);
    }
}
