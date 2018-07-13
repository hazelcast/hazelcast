package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;

public class LicenseInfo {
    private final int allowedNumberOfNodes;
    private final int type;
    private final String companyName;
    private final String email;
    private final long expiryDate;

    public LicenseInfo(int allowedNumberOfNodes, int type, String companyName, String email, long expiryDate) {
        this.allowedNumberOfNodes = allowedNumberOfNodes;
        this.type = type;
        this.companyName = companyName;
        this.email = email;
        this.expiryDate = expiryDate;
    }

    public int getAllowedNumberOfNodes() {
        return allowedNumberOfNodes;
    }

    public int getType() {
        return type;
    }

    public String getCompanyName() {
        return companyName;
    }

    public String getEmail() {
        return email;
    }

    public long getExpiryDate() {
        return expiryDate;
    }

    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("allowedNumberOfNodes", allowedNumberOfNodes);
        root.add("type", type);
        root.add("companyName", companyName);
        root.add("email", email);
        root.add("expiryDate", expiryDate);
        return root;
    }
}
