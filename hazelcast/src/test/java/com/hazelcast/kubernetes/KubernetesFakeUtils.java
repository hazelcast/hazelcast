/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.EndpointAddressBuilder;
import io.fabric8.kubernetes.api.model.EndpointPortBuilder;
import io.fabric8.kubernetes.api.model.EndpointSubsetBuilder;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsBuilder;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.api.model.EndpointsListBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LoadBalancerIngressBuilder;
import io.fabric8.kubernetes.api.model.LoadBalancerStatusBuilder;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddressBuilder;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.NodeStatusBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.ServiceSpecBuilder;
import io.fabric8.kubernetes.api.model.ServiceStatusBuilder;
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointBuilder;
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointConditions;
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointPort;
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointSliceBuilder;
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointSliceList;
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointSliceListBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class KubernetesFakeUtils {

    static Pod pod(String name, String namespace, String nodeName, Integer... ports) {
        return pod(name, namespace, nodeName, null, ports);
    }

    static Pod notReadyPod(String name, String namespace, String nodeName, String ip, Integer... ports) {
        Pod pod = pod(name, namespace, nodeName, ip, ports);
        for (ContainerStatus containerStatus : pod.getStatus().getContainerStatuses()) {
            containerStatus.setReady(false);
        }
        return pod;
    }

    static Pod pod(String name, String namespace, String nodeName, String ip, Integer... ports) {
        return new PodBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .build())
                .withSpec(new PodSpecBuilder()
                        .withNodeName(nodeName)
                        .withContainers(new ContainerBuilder()
                                .withName("hazelcast")
                                .withImage("docker.io/hazelcast/hazelcast-enterprise:5.1")
                                .withPorts(ports(ports))
                                .build())
                        .build())
                .withStatus(new PodStatusBuilder()
                        .withContainerStatuses(new ContainerStatusBuilder().withReady().build())
                        .withPodIP(ip)
                        .build())
                .build();
    }

    static PodList podsList(Pod... pods) {
        return new PodListBuilder().withItems(pods).build();
    }

    static PodList podsList(List<KubernetesClient.EndpointAddress> addresses) {
        List<Pod> pods = new ArrayList<>();
        for (int i = 0; i < addresses.size(); i++) {
            KubernetesClient.EndpointAddress address = addresses.get(i);
            pods.add(pod("hazelcast-" + i, "default", "node-name-1", address.getIp(), address.getPort()));
        }
        return new PodListBuilder().withItems(pods).build();
    }

    static PodList podsListMultiplePorts(List<String> podsIp) {
        List<Pod> pods = new ArrayList<>();
        for (int i = 0; i < podsIp.size(); i++) {
            Pod pod = new PodBuilder()
                    .withMetadata(new ObjectMetaBuilder()
                            .withName("hazelcast-" + i)
                            .build())
                    .withSpec(new PodSpecBuilder()
                            .withContainers(new ContainerBuilder()
                                    .withName("hazelcast")
                                    .withPorts(ports(5701, 5702))
                                    .build())
                            .build())
                    .withStatus(new PodStatusBuilder()
                            .withContainerStatuses(new ContainerStatusBuilder().withReady().build())
                            .withPodIP(podsIp.get(i))
                            .build())
                    .build();
            pods.add(pod);
        }
        return new PodListBuilder().withItems(pods).build();
    }

    static Endpoints endpoints(String name, Map<String, String> ipToNode, List<Integer> ports) {
        EndpointSubsetBuilder subsetBuilder = new EndpointSubsetBuilder();
        List<Map.Entry<String, String>> entries = new ArrayList<>(ipToNode.entrySet());
        for (int i = 0; i < entries.size(); i++) {
            subsetBuilder.addToAddresses(new EndpointAddressBuilder()
                    .withNodeName(entries.get(i).getValue())
                    .withIp(entries.get(i).getKey())
                    .withTargetRef(new ObjectReferenceBuilder()
                            .withName("hazelcast-" + i)
                            .build())
                    .build());
        }
        for (Integer port : ports) {
            subsetBuilder.addToPorts(new EndpointPortBuilder()
                    .withAppProtocol("TCP")
                    .withPort(port)
                    .build());
        }

        return new EndpointsBuilder()
                .withMetadata(new ObjectMetaBuilder().withName(name).build())
                .withSubsets(subsetBuilder.build())
                .build();
    }

    static Endpoints endpoints(List<String> ips, List<Integer> ports) {
        return endpoints("hazelcast", ipsToNode(ips), ports);
    }

    static Endpoints endpoints(String... ips) {
        return endpoints(Arrays.asList(ips), Collections.singletonList(5701));
    }

    static Endpoints endpoints(String name, Map<String, String> ips) {
        return endpoints(name, ips, Collections.singletonList(5701));
    }

    static Endpoints endpoints(String ip, String targetRef, io.fabric8.kubernetes.api.model.EndpointPort... ports) {
        return new EndpointsBuilder()
                .addToSubsets(new EndpointSubsetBuilder()
                        .withAddresses(new EndpointAddressBuilder()
                                .withTargetRef(new ObjectReferenceBuilder()
                                        .withName(targetRef)
                                        .build())
                                .withIp(ip)
                                .build())
                        .withPorts(ports)
                        .build())
                .build();
    }

    static io.fabric8.kubernetes.api.model.EndpointPort endpointPort(String name, Integer port) {
        return new EndpointPortBuilder()
                .withName(name)
                .withPort(port)
                .withProtocol("TCP")
                .build();
    }

    static Endpoints endpoints(String readyAddress, String notReadyAddress, String targetRef, Integer port) {
        ObjectReference objectReference = new ObjectReferenceBuilder()
                .withName(targetRef)
                .build();
        return new EndpointsBuilder()
                .addToSubsets(new EndpointSubsetBuilder()
                        .withAddresses(new EndpointAddressBuilder()
                                .withTargetRef(objectReference)
                                .withIp(readyAddress)
                                .build())
                        .withNotReadyAddresses(new EndpointAddressBuilder()
                                .withTargetRef(objectReference)
                                .withIp(notReadyAddress)
                                .build())
                        .withPorts(new EndpointPortBuilder()
                                .withPort(port)
                                .build())
                        .build())
                .build();
    }

    static EndpointsList endpointsList(Endpoints... endpoints) {
        return new EndpointsListBuilder().withItems(endpoints).build();
    }

    static List<ContainerPort> ports(Integer... ports) {
        List<ContainerPort> result = new ArrayList<>();
        for (int i = 0; i < ports.length; i++) {
            result.add(new ContainerPortBuilder()
                    .withContainerPort(ports[i])
                    .withName("port-" + i)
                    .build());
        }
        return result;
    }

    static EndpointSliceList endpointSliceList(List<Integer> ports, String... ips) {
        EndpointSliceBuilder endpointSliceBuilder = new EndpointSliceBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName("es1")
                        .withUid("someUuid")
                        .build())
                .withEndpoints(new EndpointBuilder()
                        .withAddresses(ips)
                        .withConditions(new EndpointConditions(true, true, false))
                        .build());
        for (Integer port : ports) {
            endpointSliceBuilder.addToPorts(new EndpointPort("TCP", "portName", port, "TCP"));
        }
        return new EndpointSliceListBuilder()
                .withItems(endpointSliceBuilder.build())
                .build();
    }


    static Service serviceLb(ServicePort port, String lbIp) {
        return new ServiceBuilder()
                .withSpec(new ServiceSpecBuilder()
                        .withPorts(port)
                        .build())
                .withStatus(new ServiceStatusBuilder()
                        .withLoadBalancer(new LoadBalancerStatusBuilder()
                                .withIngress(new LoadBalancerIngressBuilder()
                                        .withIp(lbIp)
                                        .build())
                                .build())
                        .build())
                .build();
    }

    static Service serviceLbHost(ServicePort port, String hostname) {
        return new ServiceBuilder()
                .withSpec(new ServiceSpecBuilder()
                        .withPorts(port)
                        .build())
                .withStatus(new ServiceStatusBuilder()
                        .withLoadBalancer(new LoadBalancerStatusBuilder()
                                .withIngress(new LoadBalancerIngressBuilder()
                                        .withHostname(hostname)
                                        .build())
                                .build())
                        .build())
                .build();
    }

    static Service service(ServicePort... ports) {
        return new ServiceBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("service").build())
                .withSpec(new ServiceSpecBuilder()
                        .withPorts(ports)
                        .build())
                .build();
    }

    static ServicePort servicePort(Integer port, Integer targetPort, Integer nodePort) {
        return new ServicePortBuilder()
                .withPort(port)
                .withTargetPort(new IntOrString(targetPort))
                .withNodePort(nodePort)
                .build();
    }

    static Node node(String name, String internalIp, String externalIp) {
        return new NodeBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .build())
                .withStatus(new NodeStatusBuilder()
                        .withAddresses(new NodeAddressBuilder()
                                        .withAddress(internalIp)
                                        .withType("InternalIP")
                                        .build(),
                                new NodeAddressBuilder()
                                        .withAddress(externalIp)
                                        .withType("ExternalIP")
                                        .build())
                        .build())
                .build();
    }

    private static Map<String, String> ipsToNode(List<String> ips) {
        return ips.stream().collect(Collectors.toMap(Function.identity(), s -> "node-name-1"));
    }
}

