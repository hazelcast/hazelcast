package com.hazelcast.client.config;

import com.hazelcast.config.AwsConfig;

/**
 * The AWSConfig contains the configuration for client
 * to connect to nodes in aws environment.
 */
public class ClientAwsConfig extends AwsConfig {
    private boolean insideAws = false;

    /**
     * If client is inside aws, it will use private ip addresses directly,
     * otherwise it will convert private ip addresses to public addresses
     * internally by calling AWS API.
     *
     * @return boolean true if client is inside aws environment.
     */
    public boolean isInsideAws() {
        return insideAws;
    }

    /**
     * Set to true if client is inside aws environment
     * Default value is false.
     *
     * @param insideAws isInsideAws
     */
    public ClientAwsConfig setInsideAws(boolean insideAws) {
        this.insideAws = insideAws;
        return this;
    }
}
