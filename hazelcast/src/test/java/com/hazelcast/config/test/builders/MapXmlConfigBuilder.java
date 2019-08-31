package com.hazelcast.config.test.builders;

public class MapXmlConfigBuilder {
    private static final int DEFAULT_BACKUP_COUNT = 1;

    private String name;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private int timeToLiveSeconds;
    private MapXmlStoreConfigBuilder mapXmlStoreConfigBuilder;

    public MapXmlConfigBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public MapXmlConfigBuilder withBackupCount(int backupCount) {
        this.backupCount = backupCount;
        return this;
    }

    public MapXmlConfigBuilder withTimeToLive(int timeToLiveSeconds) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        return this;
    }

    public MapXmlConfigBuilder withStore(MapXmlStoreConfigBuilder mapXmlStoreConfigBuilder) {
        this.mapXmlStoreConfigBuilder = mapXmlStoreConfigBuilder;
        return this;
    }

    public String build() {
        return "<map name=\"" + name + "\">\n"
                + "<backup-count>" + backupCount + "</backup-count>"
                + "<time-to-live-seconds>" + timeToLiveSeconds + "</time-to-live-seconds>"
                + (mapXmlStoreConfigBuilder != null ? mapXmlStoreConfigBuilder.build() : "")
                + "</map>\n";
    }
}
