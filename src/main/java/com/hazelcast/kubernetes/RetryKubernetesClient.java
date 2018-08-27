/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.kubernetes;

import java.util.concurrent.Callable;

/**
 * Connects to Kubernetes API with retries.
 */
class RetryKubernetesClient
        implements KubernetesClient {
    private static final int DEFAULT_RETRIES = 10;

    private final KubernetesClient kubernetesClient;
    private final int retries;

    RetryKubernetesClient(KubernetesClient kubernetesClient, int retries) {
        this.kubernetesClient = kubernetesClient;
        this.retries = retries;
    }

    RetryKubernetesClient(KubernetesClient kubernetesClient) {
        this(kubernetesClient, DEFAULT_RETRIES);
    }

    @Override
    public Endpoints endpoints(final String namespace) {
        return RetryUtils.retry(new Callable<Endpoints>() {
            @Override
            public Endpoints call()
                    throws Exception {
                return kubernetesClient.endpoints(namespace);
            }
        }, retries);
    }

    @Override
    public Endpoints endpointsByLabel(final String namespace, final String serviceLabel, final String serviceLabelValue) {
        return RetryUtils.retry(new Callable<Endpoints>() {
            @Override
            public Endpoints call()
                    throws Exception {
                return kubernetesClient.endpointsByLabel(namespace, serviceLabel, serviceLabelValue);
            }
        }, retries);
    }

    @Override
    public Endpoints endpointsByName(final String namespace, final String endpointName) {
        return RetryUtils.retry(new Callable<Endpoints>() {
            @Override
            public Endpoints call()
                    throws Exception {
                return kubernetesClient.endpointsByName(namespace, endpointName);
            }
        }, retries);
    }
}
