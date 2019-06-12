package com.hazelcast.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class JavaKeyStoreSecureStoreConfig
        extends SecureStoreConfig {
    private File path;
    private String type;
    private String password;
    private List<Entry> entries = new ArrayList<>();

    public String getType() {
        return type;
    }

    public JavaKeyStoreSecureStoreConfig setType(String type) {
        checkNotNull(type, "Java Key Store type cannot be null!");
        this.type = type;
        return this;
    }

    public File getPath() {
        return path;
    }

    public JavaKeyStoreSecureStoreConfig setPath(File path) {
        checkNotNull(path, "Java Key Store path cannot be null!");
        this.path = path;
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

    public List<Entry> getEntries() {
        return entries;
    }

    public JavaKeyStoreSecureStoreConfig setEntries(List<Entry> entries) {
        checkNotNull(entries, "Java Key Store entries cannot be null!");
        this.entries = entries;
        return this;
    }

    public static class Entry {
        private final String alias;
        private final String password;

        public Entry(String alias, String password) {
            this.alias = alias;
            this.password = password;
        }

        public String getAlias() {
            return alias;
        }

        public String getPassword() {
            return password;
        }

        @Override
        public String toString() {
            return "Entry{"
                    + "alias='" + alias + '\''
                    + ", password='***'"
                    + '}';
        }

        @Override
        public final boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Entry)) {
                return false;
            }
            Entry other = (Entry) o;
            if (!Objects.equals(alias, other.alias)) {
                return false;
            }
            return Objects.equals(password, other.password);
        }

        @Override
        public final int hashCode() {
            int result = alias == null ? 0 : alias.hashCode();
            result = 31 * result + (password == null ? 0 : password.hashCode());
            return result;
        }
    }

    @Override
    public String toString() {
        return "JavaKeyStoreSecureStoreConfig{"
                + "path='" + path + '\''
                + ", type='" + type + '\''
                + ", password='***'"
                + ", entries=" + entries
                + '}';
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JavaKeyStoreSecureStoreConfig)) {
            return false;
        }
        JavaKeyStoreSecureStoreConfig other = (JavaKeyStoreSecureStoreConfig) o;
        if (!Objects.equals(path, other.path)) {
            return false;
        }
        if (!Objects.equals(type, other.type)) {
            return false;
        }
        if (!Objects.equals(password, other.password)) {
            return false;
        }
        return Objects.equals(entries, other.entries);
    }

    @Override
    public final int hashCode() {
        int result = path == null ? 0 : path.hashCode();
        result = 31 * result + (type == null ? 0 : type.hashCode());
        result = 31 * result + (password == null ? 0 : password.hashCode());
        result = 31 * result + (entries == null ? 0 : entries.hashCode());
        return result;
    }

}
