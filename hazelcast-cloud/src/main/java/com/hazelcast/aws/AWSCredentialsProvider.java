package com.hazelcast.aws;

/**
 * Provides AWS API credentials
 *
 * @author dturner@kixeye.com
 */
public interface AWSCredentialsProvider {

    /**
     * get the AWS access key
     * @return
     */
    public String getAccessKey();

    /**
     * get the AWS secret key
     * @return
     */
    public String getSecretKey();
}
