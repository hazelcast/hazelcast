package com.hazelcast.instance;

/**
 * @author : ahmetmircik
 */
public class BuildInfo {

    private final String version;
    private final String build;
    private final int buildNumber;

    public BuildInfo(String version, String build, int buildNumber) {
        this.version = version;
        this.build = build;
        this.buildNumber = buildNumber;
    }

    public String getVersion() {
        return version;
    }

    public String getBuild() {
        return build;
    }

    public int getBuildNumber() {
        return buildNumber;
    }

}
