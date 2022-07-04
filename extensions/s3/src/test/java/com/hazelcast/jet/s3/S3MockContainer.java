/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.s3;

import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

/**
 * A mock implementation of the AWS S3 API started as a test container.
 */
public class S3MockContainer extends GenericContainer<S3MockContainer> {

    public static final String VERSION = "2.1.15";
    public static final Integer S3_PORT = 9090;

    private static final String IMAGE_NAME = "adobe/s3mock";

    public S3MockContainer() {
        this(VERSION);
    }

    public S3MockContainer(String version) {
        super(IMAGE_NAME + ":" + version);
    }

    @Override
    protected void configure() {
        addExposedPort(S3_PORT);
    }

    @Override
    public Set<Integer> getLivenessCheckPortNumbers() {
        return Collections.singleton(getMappedPort(S3_PORT));
    }

    String endpointURL() {
        return "http://" + getContainerIpAddress() + ":" + getMappedPort(S3_PORT);
    }

    S3Client client() {
        return client(endpointURL());
    }

    static S3Client client(String endpointURL) {
        return S3Client
                .builder()
                .credentialsProvider(StaticCredentialsProvider.create(new AwsCredentials() {
                    @Override
                    public String accessKeyId() {
                        return "foo";
                    }

                    @Override
                    public String secretAccessKey() {
                        return "bar";
                    }
                }))
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(endpointURL))
                .build();
    }
}
