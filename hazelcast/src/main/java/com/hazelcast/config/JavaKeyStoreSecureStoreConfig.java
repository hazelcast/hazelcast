package com.hazelcast.config;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class JavaKeyStoreSecureStoreConfig extends SecureStoreConfig {
    private String type;
    private String location;
    private String password;
    private List<KeyEntry> entries = new ArrayList<>();

    public String getType() {
        return type;
    }

    public JavaKeyStoreSecureStoreConfig setType(String type) {
        checkNotNull(type, "Java Key Store type cannot be null!");
        this.type = type;
        return this;
    }

    public String getLocation() {
        return location;
    }

    public JavaKeyStoreSecureStoreConfig setLocation(String location) {
        checkNotNull(location, "Java Key Store location cannot be null!");
        this.location = location;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public JavaKeyStoreSecureStoreConfig setPassword(String password) {
        checkNotNull(password, "Java Key Store password cannot be null!");
        this.password = password;
        return this;
    }

    public List<KeyEntry> getEntries() {
        return entries;
    }

    public JavaKeyStoreSecureStoreConfig setEntries(List<KeyEntry> entries) {
        checkNotNull(entries, "Java Key Store entries cannot be null!");
        this.entries = entries;
        return this;
    }

    public static class KeyEntry {
        private final String alias;
        private final String password;

        public KeyEntry(String alias, String password) {
            this.alias = alias;
            this.password = password;
        }

        public String getAlias() {
            return alias;
        }

        public String getPassword() {
            return password;
        }
    }

}
