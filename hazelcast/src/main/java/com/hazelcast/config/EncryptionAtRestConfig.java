package com.hazelcast.config;

import java.util.Objects;

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

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EncryptionAtRestConfig)) {
            return false;
        }
        EncryptionAtRestConfig other = (EncryptionAtRestConfig)o;
        if (enabled != other.enabled) {
            return false;
        }
        if (!Objects.equals(algorithm, other.algorithm)) {
            return false;
        }
        if (!Objects.equals(password, other.password)) {
            return false;
        }
        if (!Objects.equals(salt, other.salt)) {
            return false;
        }
        if (iterationCount != other.iterationCount) {
            return false;
        }
        return Objects.equals(secureStoreConfig, other.secureStoreConfig);
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (algorithm == null ? 0: algorithm.hashCode());
        result = 31 * result + (password == null ? 0: password.hashCode());
        result = 31 * result + (salt == null ? 0: salt.hashCode());
        result = 31 * result + iterationCount;
        result = 31 * result + (secureStoreConfig == null ? 0 : secureStoreConfig.hashCode());
        return result;
    }

}

