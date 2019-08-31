package com.hazelcast.config.test.builders;

public class MapXmlStoreConfigBuilder {
    private boolean enabled;
    private String initialMode;
    private String className;
    private int writeDelaySeconds;
    private int writeBatchSize;

    public MapXmlStoreConfigBuilder enabled() {
        this.enabled = true;
        return this;
    }

    public MapXmlStoreConfigBuilder disabled() {
        this.enabled = false;
        return this;
    }

    public MapXmlStoreConfigBuilder withInitialMode(String initialMode) {
        this.initialMode = initialMode;
        return this;
    }

    public MapXmlStoreConfigBuilder withClassName(String className) {
        this.className = className;
        return this;
    }

    public MapXmlStoreConfigBuilder withWriteDelay(int writeDelaySeconds) {
        this.writeDelaySeconds = writeDelaySeconds;
        return this;
    }

    public MapXmlStoreConfigBuilder withWriteBatchSize(int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
        return this;
    }

    public String build() {
        return "<map-store enabled=\"" + enabled + "\" initial-mode=\"" + initialMode + "\">\n"
                + "    <class-name>" + className + "</class-name>\n"
                + "    <write-delay-seconds>" + writeDelaySeconds + "</write-delay-seconds>\n"
                + "    <write-batch-size>" + writeBatchSize + "</write-batch-size>\n"
                + "</map-store>";
    }
}
