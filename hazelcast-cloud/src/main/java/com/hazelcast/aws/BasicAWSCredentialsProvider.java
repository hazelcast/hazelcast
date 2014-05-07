package com.hazelcast.aws;

/**
 * Basic AWS credentials provider which simply returns its own static credentials.
 *
 * @author dturner@kixeye.com
 */
public class BasicAWSCredentialsProvider implements AWSCredentialsProvider{

    private String accessKey;

    private String secretKey;

    public BasicAWSCredentialsProvider(String accessKey, String secretKey) {
        if(accessKey == null){
            throw new IllegalArgumentException("Access Key is required!");
        }
        if(secretKey == null){
            throw new IllegalArgumentException("Secret Key is required!");
        }
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    @Override
    public String getAccessKey() {
        return accessKey;
    }

    @Override
    public String getSecretKey() {
        return secretKey;
    }
}
