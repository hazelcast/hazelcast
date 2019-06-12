package com.hazelcast.config;

/**
 * Base class for symmetric encryption configuration classes.
 * @param <T> the type of the configuration class
 */
public class AbstractSymmetricEncryptionConfig<T extends AbstractSymmetricEncryptionConfig> {
    /**
     * Default symmetric encryption algorithm
     */
    public static final String DEFAULT_SYMMETRIC_ALGORITHM = "PBEWithMD5AndDES";
    /**
     * Default symmetric encryption password
     */
    public static final String DEFAULT_SYMMETRIC_PASSWORD = "thepassword";

    /**
     * Default symmetric encryption salt
     */
    public static final String DEFAULT_SYMMETRIC_SALT = "thesalt";

    private static final int ITERATION_COUNT = 19;

    protected boolean enabled;
    protected String algorithm = DEFAULT_SYMMETRIC_ALGORITHM;
    protected String password = DEFAULT_SYMMETRIC_PASSWORD;
    protected String salt = DEFAULT_SYMMETRIC_SALT;
    protected int iterationCount = ITERATION_COUNT;

    public boolean isEnabled() {
        return enabled;
    }

    public T setEnabled(boolean enabled) {
        this.enabled = enabled;
        return (T)this;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public T setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
        return (T)this;
    }

    public String getPassword() {
        return password;
    }

    public T setPassword(String password) {
        this.password = password;
        return (T)this;
    }

    public String getSalt() {
        return salt;
    }

    public T setSalt(String salt) {
        this.salt = salt;
        return (T)this;
    }

    public int getIterationCount() {
        return iterationCount;
    }

    public T setIterationCount(int iterationCount) {
        this.iterationCount = iterationCount;
        return (T)this;
    }

}
