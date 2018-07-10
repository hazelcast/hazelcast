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
import java.util.List;
import java.util.Scanner;

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

        List<Address> addresses = new ArrayList<Address>();
        List<Address> notReadyAddresses = new ArrayList<Address>();
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
            return Json.parse(new InputStreamReader((connection.getInputStream()))).asObject();
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
        Scanner scanner = new Scanner(errorStream);
        scanner.useDelimiter("\\Z");
        return scanner.next();
    }

    private static Endpoints parseEndpoint(JsonValue endpointsJson) {
        List<Address> addresses = new ArrayList<Address>();
        List<Address> notReadyAddresses = new ArrayList<Address>();
        for (JsonValue subset : toJsonArray(endpointsJson.asObject().get("subsets"))) {
            for (JsonValue address : toJsonArray(subset.asObject().get("addresses"))) {
                String ip = address.asObject().get("ip").asString();
                addresses.add(new Address(ip));
            }
            for (JsonValue notReadyAddress : toJsonArray(subset.asObject().get("notReadyAddresses"))) {
                String ip = notReadyAddress.asObject().get("ip").asString();
                notReadyAddresses.add(new Address(ip));
            }
        }
        return new Endpoints(addresses, notReadyAddresses);
    }

    private static JsonArray toJsonArray(JsonValue jsonValue) {
        if (jsonValue == null || jsonValue.isNull()) {
            return new JsonArray();
        } else {
            return jsonValue.asArray();
        }
    }

    static class Endpoints {
        private final List<Address> addresses;
        private final List<Address> notReadyAddresses;

        Endpoints(List<Address> addresses, List<Address> notReadyAddresses) {
            this.addresses = addresses;
            this.notReadyAddresses = notReadyAddresses;
        }

        public List<Address> getAddresses() {
            return addresses;
        }

        public List<Address> getNotReadyAddresses() {
            return notReadyAddresses;
        }
    }

    static class Address {
        private final String ip;

        Address(String ip) {
            this.ip = ip;
        }

        public String getIp() {
            return ip;
        }
    }
}
