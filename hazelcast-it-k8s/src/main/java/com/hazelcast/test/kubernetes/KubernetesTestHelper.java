/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.test.kubernetes;

import com.hazelcast.test.kubernetes.helm.Helm;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.List;
import java.util.stream.Collectors;

public class KubernetesTestHelper implements AutoCloseable {
    private final KubernetesClient kubernetesClient;
    private final Helm helm;

    public KubernetesTestHelper() {
        this(null);
    }

    /**
     * squid:S2095: KubernetesClient will be closed within close method of this class
     */
    @SuppressWarnings("squid:S2095")
    public KubernetesTestHelper(String namespace) {
        this.helm = new Helm(namespace);
        this.kubernetesClient = new DefaultKubernetesClient().inNamespace(namespace);
    }

    public List<Pod> allPodsStartingWith(String prefix) {
        return kubernetesClient.pods().list().getItems().stream()
                .filter(p -> p.getMetadata().getName().startsWith(prefix))
                .collect(Collectors.toList());
    }

    public void createOrReplaceNamespace(String namespaceName) {
        Namespace namespace = new NamespaceBuilder()
                .withNewMetadata()
                .withName(namespaceName)
                .endMetadata()
                .build();
        kubernetesClient.namespaces().createOrReplace(namespace);
    }

    public void deleteNamespace(String namespaceName) {
        kubernetesClient.namespaces().withName(namespaceName).delete();
    }

    public Helm helm() {
        return helm;
    }

    public KubernetesClient kubernetesClient() {
        return kubernetesClient;
    }

    public String getServiceExternalIp(String serviceName) {
        return kubernetesClient.services().withName(serviceName).get().getStatus().getLoadBalancer().getIngress().get(0).getIp();
    }

    @Override
    public void close() {
        kubernetesClient.close();
    }
}
