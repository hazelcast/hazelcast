package com.hazelcast.config;

public class SchemaViolationConfigurationException extends InvalidConfigurationException {
    
    private final String keywordLocation;
    
    private final String instanceLocation;
    
    public SchemaViolationConfigurationException(String message, String keywordLocation, String instanceLocation) {
        super(message);
        this.keywordLocation = keywordLocation;
        this.instanceLocation = instanceLocation;
    }

    public String getKeywordLocation() {
        return keywordLocation;
    }

    public String getInstanceLocation() {
        return instanceLocation;
    }
    
    public String getError() {
        return getMessage();
    }
}
