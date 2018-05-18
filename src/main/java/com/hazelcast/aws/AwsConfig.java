package com.hazelcast.aws;

import com.hazelcast.config.TcpIpConfig;

import static com.hazelcast.util.Preconditions.checkHasText;

public class AwsConfig {

    private static final int CONNECTION_TIMEOUT = 5;

    private boolean enabled;
    private String accessKey;
    private String secretKey;
    private String region = "us-east-1";
    private String securityGroupName;
    private String tagKey;
    private String tagValue;
    private String hostHeader = "ec2.amazonaws.com";
    private String iamRole;
    private int connectionTimeoutSeconds = CONNECTION_TIMEOUT;
    private int connectionRetries;

    /**
     * Gets the access key to access AWS. Returns null if no access key is configured.
     *
     * @return the access key to access AWS
     * @see #setAccessKey(String)
     */
    public String getAccessKey() {
        return accessKey;
    }

    /**
     * Sets the access key to access AWS.
     *
     * @param accessKey the access key to access AWS
     * @return the updated AwsConfig.
     * @throws IllegalArgumentException if accessKey is null or empty.
     * @see #getAccessKey()
     * @see #setSecretKey(String)
     */
    public AwsConfig setAccessKey(String accessKey) {
        this.accessKey = checkHasText(accessKey, "accessKey must contain text");
        return this;
    }

    /**
     * Gets the secret key to access AWS. Returns null if no access key is configured.
     *
     * @return the secret key.
     * @see #setSecretKey(String)
     */
    public String getSecretKey() {
        return secretKey;
    }

    /**
     * Sets the secret key to access AWS.
     *
     * @param secretKey the secret key to access AWS
     * @return the updated AwsConfig.
     * @throws IllegalArgumentException if secretKey is null or empty.
     * @see #getSecretKey()
     * @see #setAccessKey(String)
     */
    public AwsConfig setSecretKey(String secretKey) {
        this.secretKey = checkHasText(secretKey, "secretKey must contain text");
        return this;
    }

    /**
     * Gets the region where the EC2 instances running the Hazelcast members will be running.
     *
     * @return the region where the EC2 instances running the Hazelcast members will be running
     * @see #setRegion(String)
     */
    public String getRegion() {
        return region;
    }

    /**
     * Sets the region where the EC2 instances running the Hazelcast members will be running.
     *
     * @param region the region where the EC2 instances running the Hazelcast members will be running
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if region is null or empty.
     */
    public AwsConfig setRegion(String region) {
        this.region = checkHasText(region, "region must contain text");
        return this;
    }

    /**
     * Gets the host header; the address where the EC2 API can be found.
     *
     * @return the host header; the address where the EC2 API can be found
     */
    public String getHostHeader() {
        return hostHeader;
    }

    /**
     * Sets the host header; the address where the EC2 API can be found.
     *
     * @param hostHeader the new host header; the address where the EC2 API can be found
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if hostHeader is null or an empty string.
     */
    public AwsConfig setHostHeader(String hostHeader) {
        this.hostHeader = checkHasText(hostHeader, "hostHeader must contain text");
        return this;
    }

    /**
     * Enables or disables the aws join mechanism.
     *
     * @param enabled true if enabled, false otherwise.
     * @return the updated AwsConfig.
     */
    public AwsConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Checks if the aws join mechanism is enabled.
     *
     * @return true if enabled, false otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the security group name. See the filtering section above for more information.
     *
     * @param securityGroupName the security group name.
     * @return the updated AwsConfig.
     * @see #getSecurityGroupName()
     */
    public AwsConfig setSecurityGroupName(String securityGroupName) {
        this.securityGroupName = securityGroupName;
        return this;
    }

    /**
     * Gets the security group name. If nothing has been configured, null is returned.
     *
     * @return the security group name; null if nothing has been configured
     */
    public String getSecurityGroupName() {
        return securityGroupName;
    }

    /**
     * Sets the tag key. See the filtering section above for more information.
     *
     * @param tagKey the tag key. See the filtering section above for more information.
     * @return the updated AwsConfig.
     * @see #setTagKey(String)
     */
    public AwsConfig setTagKey(String tagKey) {
        this.tagKey = tagKey;
        return this;
    }

    /**
     * Sets the tag value. See the filtering section above for more information.
     *
     * @param tagValue the tag value. See the filtering section above for more information.
     * @return the updated AwsConfig.
     * @see #setTagKey(String)
     * @see #getTagValue()
     */
    public AwsConfig setTagValue(String tagValue) {
        this.tagValue = tagValue;
        return this;
    }

    /**
     * Gets the tag key. If nothing is specified, null is returned.
     *
     * @return the tag key. null if nothing is returned.
     */
    public String getTagKey() {
        return tagKey;
    }

    /**
     * Gets the tag value. If nothing is specified, null is returned.
     *
     * @return the tag value. null if nothing is returned.
     */
    public String getTagValue() {
        return tagValue;
    }

    /**
     * Gets the connection timeout in seconds.
     *
     * @return the connectionTimeoutSeconds; connection timeout in seconds
     * @see #setConnectionTimeoutSeconds(int)
     */
    public int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    /**
     * Sets the connect timeout in seconds. See {@link TcpIpConfig#setConnectionTimeoutSeconds(int)} for more information.
     *
     * @param connectionTimeoutSeconds the connectionTimeoutSeconds (connection timeout in seconds) to set
     * @return the updated AwsConfig.
     * @see #getConnectionTimeoutSeconds()
     * @see TcpIpConfig#setConnectionTimeoutSeconds(int)
     */
    public AwsConfig setConnectionTimeoutSeconds(final int connectionTimeoutSeconds) {
        if (connectionTimeoutSeconds < 0) {
            throw new IllegalArgumentException("connection timeout can't be smaller than 0");
        }
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        return this;
    }

    public int getConnectionRetries() {
        return connectionRetries;
    }

    public AwsConfig setConnectionRetries(final int connectionRetries) {
        if (connectionTimeoutSeconds < 1) {
            throw new IllegalArgumentException("connection retries can't be smaller than 1");
        }
        this.connectionRetries = connectionRetries;
        return this;
    }

    /**
     * Gets the iamRole name
     *
     * @return the iamRole. null if nothing is returned.
     * @see #setIamRole(String) (int)
     */
    public String getIamRole() {
        return iamRole;
    }

    /**
     * Sets the tag value. See the filtering section above for more information.
     *
     * @param iamRole the IAM Role name.
     * @return the updated AwsConfig.
     * @see #getIamRole()
     */
    public AwsConfig setIamRole(String iamRole) {
        this.iamRole = iamRole;
        return this;
    }

    @Override
    public String toString() {
        return "AwsConfig{"
                + "enabled=" + enabled
                + ", region='" + region + '\''
                + ", securityGroupName='" + securityGroupName + '\''
                + ", tagKey='" + tagKey + '\''
                + ", tagValue='" + tagValue + '\''
                + ", hostHeader='" + hostHeader + '\''
                + ", iamRole='" + iamRole + '\''
                + ", connectionTimeoutSeconds=" + connectionTimeoutSeconds + '}';
    }
}
