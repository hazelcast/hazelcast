package com.hazelcast.kubernetes;

import com.hazelcast.com.eclipsesource.json.Json;
import com.hazelcast.com.eclipsesource.json.JsonArray;
import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.com.eclipsesource.json.JsonValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

/**
 * Responsible for connecting to the Kubernetes API.
 *
 * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/">Kubernetes API</a>
 */
class KubernetesClient {
    private final String kubernetesMaster;
    private final String apiToken;

    KubernetesClient(String kubernetesMaster, String apiToken) {
        this.kubernetesMaster = kubernetesMaster;
        this.apiToken = apiToken;
    }

    /**
     * Retrieves POD addresses from all services in the given {@code namespace}.
     *
     * @param namespace namespace name
     * @return all POD addresses from the given {@code namespace}
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-143">Kubernetes Endpoint List API</a>
     */
    Endpoints endpoints(String namespace) {
        return callEndpointsService(namespace, null);
    }

    /**
     * Retrieves POD addresses from all services in the given {@code namespace} filtered by {@code serviceLabel} and {@code serviceLabelValue}.
     *
     * @param namespace         namespace name
     * @param serviceLabel      label used to filter responses
     * @param serviceLabelValue label value used to filter responses
     * @return all POD addresses from the given {@code namespace} filted by the label
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-143">Kubernetes Endpoint List API</a>
     */
    Endpoints endpointsByLabel(String namespace, String serviceLabel, String serviceLabelValue) {
        String param = String.format("labelSelector=%s=%s", serviceLabel, serviceLabelValue);
        return callEndpointsService(namespace, param);
    }

    /**
     * Retrieves POD addresses from the given {@code namespace} and the given {@code endpointName}.
     *
     * @param namespace    namespace name
     * @param endpointName endpoint name
     * @return all POD addresses from the given {@code namespace} and the given {@code endpointName}
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-143">Kubernetes Endpoint List API</a>
     */
    Endpoints endpointsByName(String namespace, String endpointName) {
        String urlString = String.format("%s/api/v1/namespaces/%s/endpoints/%s", kubernetesMaster, namespace, endpointName);
        JsonObject json = callGet(urlString);
        return parseEndpoint(json);
    }

    private Endpoints callEndpointsService(String namespace, String param) {
        String urlString = String.format("%s/api/v1/namespaces/%s/endpoints", kubernetesMaster, namespace);
        if (param != null && !param.isEmpty()) {
            urlString = String.format("%s?%s", urlString, param);
        }

        JsonObject json = callGet(urlString);
        return parseEndpointsList(json);

    }

    private static Endpoints parseEndpointsList(JsonObject json) {
        List<EntrypointAddress> addresses = new ArrayList<EntrypointAddress>();
        List<EntrypointAddress> notReadyAddresses = new ArrayList<EntrypointAddress>();
        for (JsonValue object : toJsonArray(json.get("items"))) {
            Endpoints endpoints = parseEndpoint(object);
            addresses.addAll(endpoints.getAddresses());
            notReadyAddresses.addAll(endpoints.getNotReadyAddresses());
        }

        return new Endpoints(addresses, notReadyAddresses);
    }

    private JsonObject callGet(String urlString) {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(urlString);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Authorization", String.format("Bearer %s", apiToken));

            if (connection.getResponseCode() != 200) {
                throw new KubernetesClientException(String.format("Failure executing: GET at: %s. Message: %s,", urlString,
                        extractErrorMessage(connection.getErrorStream())));
            }
            return Json.parse(new InputStreamReader(connection.getInputStream(), "UTF-8")).asObject();
        } catch (ProtocolException e) {
            throw new KubernetesClientException("Failure executing KubernetesClient", e);
        } catch (MalformedURLException e) {
            throw new KubernetesClientException("Failure executing KubernetesClient", e);
        } catch (IOException e) {
            throw new KubernetesClientException("Failure executing KubernetesClient", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static String extractErrorMessage(InputStream errorStream) {
        Scanner scanner = new Scanner(errorStream, "UTF-8");
        scanner.useDelimiter("\\Z");
        return scanner.next();
    }

    private static Endpoints parseEndpoint(JsonValue endpointsJson) {
        List<EntrypointAddress> addresses = new ArrayList<EntrypointAddress>();
        List<EntrypointAddress> notReadyAddresses = new ArrayList<EntrypointAddress>();
        for (JsonValue subset : toJsonArray(endpointsJson.asObject().get("subsets"))) {
            for (JsonValue address : toJsonArray(subset.asObject().get("addresses"))) {
                String ip = address.asObject().get("ip").asString();
                Map<String, Object> additionalProperties = extractAdditionalProperties(address.asObject());
                addresses.add(new EntrypointAddress(ip, additionalProperties));
            }
            for (JsonValue notReadyAddress : toJsonArray(subset.asObject().get("notReadyAddresses"))) {
                String ip = notReadyAddress.asObject().get("ip").asString();
                notReadyAddresses.add(new EntrypointAddress(ip, extractAdditionalProperties(notReadyAddress.asObject())));
            }
        }
        return new Endpoints(addresses, notReadyAddresses);
    }

    private static Map<String, Object> extractAdditionalProperties(JsonObject jsonObject) {
        Set<String> knownFieldNames = new HashSet<String>(Arrays.asList("ip", "nodeName", "targetRef", "hostname"));
        Map<String, Object> result = new HashMap<String, Object>();
        Iterator<JsonObject.Member> iter = jsonObject.iterator();
        while (iter.hasNext()) {
            JsonObject.Member member = iter.next();
            if (!knownFieldNames.contains(member.getName())) {
                result.put(member.getName(), toString(member.getValue()));
            }
        }
        return result;
    }

    private static String toString(JsonValue jsonValue) {
        if (jsonValue.isString()) {
            return jsonValue.asString();
        } else {
            return jsonValue.toString();
        }
    }

    private static JsonArray toJsonArray(JsonValue jsonValue) {
        if (jsonValue == null || jsonValue.isNull()) {
            return new JsonArray();
        } else {
            return jsonValue.asArray();
        }
    }

    static class Endpoints {
        private final List<EntrypointAddress> addresses;
        private final List<EntrypointAddress> notReadyAddresses;

        Endpoints(List<EntrypointAddress> addresses, List<EntrypointAddress> notReadyAddresses) {
            this.addresses = addresses;
            this.notReadyAddresses = notReadyAddresses;
        }

        public List<EntrypointAddress> getAddresses() {
            return addresses;
        }

        public List<EntrypointAddress> getNotReadyAddresses() {
            return notReadyAddresses;
        }
    }

    static class EntrypointAddress {
        private final String ip;
        private final Map<String, Object> additionalProperties;

        EntrypointAddress(String ip, Map<String, Object> additionalProperties) {
            this.ip = ip;
            this.additionalProperties = additionalProperties;
        }

        public String getIp() {
            return ip;
        }

        public Map<String, Object> getAdditionalProperties() {
            return additionalProperties;
        }
    }
}
