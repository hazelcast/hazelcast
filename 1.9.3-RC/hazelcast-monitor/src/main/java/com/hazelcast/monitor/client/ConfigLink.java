package com.hazelcast.monitor.client;

public class ConfigLink {
    private int clusterId;
    private String type;
    private String name;

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
