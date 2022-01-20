package com.hazelcast.internal.config;

public class LicenseKey {
    private final String licenseKey;

    public LicenseKey(String licenseKey) {
        this.licenseKey = licenseKey;
    }

    public String getLicenseKey() {
        return licenseKey;
    }
}
