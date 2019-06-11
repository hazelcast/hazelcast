package com.hazelcast.config;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains configuration for the Hot Restart Persistence at Rest encryption
 */
public class EncryptionAtRestConfig
        extends AbstractSymmetricEncryptionConfig<EncryptionAtRestConfig> {

    private SecureStoreConfig secureStoreConfig;

    public SecureStoreConfig getSecureStoreConfig() {
        return secureStoreConfig;
    }

    public EncryptionAtRestConfig setSecureStoreConfig(SecureStoreConfig secureStoreConfig) {
        checkNotNull(secureStoreConfig, "Secure Store config cannot be null!");
        this.secureStoreConfig = secureStoreConfig;
        return this;
    }

    @Override
    public String toString() {
        return "EncryptionAtRestConfig{"
                + "enabled=" + isEnabled()
                + ", algorithm='" + getAlgorithm() + '\''
                + ", password='***'"
                + ", salt='***'"
                + ", iterationCount=***"
                + ", secureStoreConfig=" + secureStoreConfig
                + '}';
    }

    // TODO VT equals/hashCode

}

