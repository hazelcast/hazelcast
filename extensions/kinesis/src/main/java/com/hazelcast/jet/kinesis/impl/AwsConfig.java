/*
 * Copyright 2026 Hazelcast Inc.
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

package com.hazelcast.jet.kinesis.impl;

import com.hazelcast.function.SupplierEx;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;

import javax.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import java.net.URI;
import java.util.concurrent.ExecutorService;

public class AwsConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @Nullable
    private String endpoint;
    @Nullable
    private String region;
    @Nullable
    private String accessKey;
    @Nullable
    private String secretKey;
    @Nullable
    private SupplierEx<ExecutorService> executorServiceSupplier;

    public AwsConfig withEndpoint(@Nullable String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    @Nullable
    public String getEndpoint() {
        return endpoint;
    }

    public AwsConfig withRegion(@Nullable String region) {
        this.region = region;
        return this;
    }

    @Nullable
    public String getRegion() {
        return region;
    }

    public AwsConfig withCredentials(@Nullable String accessKey, @Nullable String secretKey) {
        if (accessKey == null ^ secretKey == null) {
            throw new IllegalArgumentException("AWS access and secret keys must be specified together");
        }
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        return this;
    }

    @Nullable
    public String getAccessKey() {
        return accessKey;
    }

    @Nullable
    public String getSecretKey() {
        return secretKey;
    }

    public AwsConfig withExecutorServiceSupplier(SupplierEx<ExecutorService> executorServiceSupplier) {
        this.executorServiceSupplier = executorServiceSupplier;
        return this;
    }

    @Nullable
    public SupplierEx<ExecutorService> getExecutorServiceSupplier() {
        return executorServiceSupplier;
    }

    public KinesisAsyncClient buildClient() {
        KinesisAsyncClientBuilder builder = KinesisAsyncClient.builder();
        if (region != null) {
            builder.region(Region.of(region));
        }

        if (endpoint != null) {
            builder.endpointOverride(URI.create(endpoint));
        }

        builder.credentialsProvider(accessKey == null ? DefaultCredentialsProvider.builder()
                .build() :
                StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
        );

        builder.overrideConfiguration(ClientOverrideConfiguration.builder()
                .build());
        if (executorServiceSupplier != null) {
            builder.asyncConfiguration(ClientAsyncConfiguration.builder()
                    .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executorServiceSupplier.get())
                    .build());
        }

        return builder.build();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(AwsConfig.class.getName()).append("(");
        boolean first = true;
        if (endpoint != null) {
            sb.append("endpoint: ").append(endpoint);
            sb.append(", region: ").append(region);
            first = false;
        }
        if (accessKey != null) {
            if (!first) {
                sb.append(", ");
            }
            sb.append("access key: ").append(accessKey);
            sb.append(", secret key: ").append(secretKey);

        }
        sb.append(")");
        return sb.toString();
    }
}
