/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Java KeyStore Secure Store configuration.
 */
public class JavaKeyStoreSecureStoreConfig
        extends SecureStoreConfig {
    /**
     * The default Java KeyStore type (JCEKS).
     */
    public static final String DEFAULT_KEYSTORE_TYPE = "JCEKS";

    private File path;
    private String type = DEFAULT_KEYSTORE_TYPE;
    private String password;
    private List<Entry> entries = new ArrayList<>();

    /**
     * Returns the type of the Java KeyStore.
     * @return the Java KeyStore type
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the Java KeyStore type (JCEKS, PKCS12 etc.)
     * @param type the KeyStore type
     * @return the updated {@link JavaKeyStoreSecureStoreConfig} instance
     * @throws IllegalArgumentException if type is {code null}
     */
    public JavaKeyStoreSecureStoreConfig setType(String type) {
        checkNotNull(type, "Java Key Store type cannot be null!");
        this.type = type;
        return this;
    }

    /**
     * Returns the Java KeyStore file path.
     * @return the file path
     */
    public File getPath() {
        return path;
    }

    /**
     * Sets the Java KeyStore file path.
     * @param path the file path
     * @return the updated {@link JavaKeyStoreSecureStoreConfig} instance
     * @throws IllegalArgumentException if path is {code null}
     */
    public JavaKeyStoreSecureStoreConfig setPath(File path) {
        checkNotNull(path, "Java Key Store path cannot be null!");
        this.path = path;
        return this;
    }

    /**
     * Returns the Java KeyStore password.
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the Java KeyStore password.
     * @param password the KeyStore password
     * @return the updated {@link JavaKeyStoreSecureStoreConfig} instance
     * @throws IllegalArgumentException if password is {code null}
     */
    public JavaKeyStoreSecureStoreConfig setPassword(String password) {
        checkNotNull(password, "Java Key Store password cannot be null!");
        this.password = password;
        return this;
    }

    /**
     * Returns the Java KeyStore entries.
     * @return a list of {@link Entry} objects
     */
    public List<Entry> getEntries() {
        return entries;
    }

    /**
     * Sets the Java KeyStore entries.
     * @param entries a list of {@link Entry} objects
     * @return the updated {@link JavaKeyStoreSecureStoreConfig} instance
     * @throws IllegalArgumentException if entries is {code null}
     */
    public JavaKeyStoreSecureStoreConfig setEntries(List<Entry> entries) {
        checkNotNull(entries, "Java Key Store entries cannot be null!");
        this.entries = entries;
        return this;
    }

    /**
     * A key entry to be looked up in the Java KeyStore.
     */
    public static class Entry {
        private final String name;
        private final String password;

        /**
         * Creates a new entry with given name and password
         * @param name the entry name
         * @param password the entry password
         */
        public Entry(String name, String password) {
            this.name = name;
            this.password = password;
        }

        /**
         * Returns the entry name.
         * @return the entry name
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the entry password.
         * @return the entry password
         */
        public String getPassword() {
            return password;
        }

        @Override
        public String toString() {
            return "Entry{"
                    + "name='" + name + '\''
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
            if (!Objects.equals(name, other.name)) {
                return false;
            }
            return Objects.equals(password, other.password);
        }

        @Override
        public final int hashCode() {
            int result = name == null ? 0 : name.hashCode();
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
