package com.hazelcast.kubernetes;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.com.eclipsesource.json.ParseException;
import com.hazelcast.kubernetes.KubernetesClient.Endpoints;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;

public class KubernetesClientTest {
    private static final String KUBERNETES_MASTER_IP = "localhost";
    private static final int KUBERNETES_MASTER_PORT = 8089;
    private static final String KUBERNETES_MASTER_URL = String
            .format("http://%s:%d", KUBERNETES_MASTER_IP, KUBERNETES_MASTER_PORT);

    private static final String TOKEN = "sample-token";
    private static final String NAMESPACE = "sample-namespace";

    private static final String SAMPLE_ADDRESS_1 = "192.168.0.25";
    private static final String SAMPLE_ADDRESS_2 = "172.17.0.5";
    private static final String SAMPLE_NOT_READY_ADDRESS = "172.17.0.6";
    private static final String SAMPLE_PORT_1 = "5701";
    private static final String SAMPLE_PORT_2 = "5702";
    private static final String SAMPLE_IP_PORT_1 = ipPort(SAMPLE_ADDRESS_1, SAMPLE_PORT_1);
    private static final String SAMPLE_IP_PORT_2 = ipPort(SAMPLE_ADDRESS_2, SAMPLE_PORT_2);
    private static final String SAMPLE_NOT_READY_IP = ipPort(SAMPLE_NOT_READY_ADDRESS, null);

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(KUBERNETES_MASTER_PORT);

    private final KubernetesClient kubernetesClient = new KubernetesClient(KUBERNETES_MASTER_URL, TOKEN);

    @Test
    public void endpointsByNamespace() {
        // given
        stubFor(get(urlEqualTo(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(endpointsListBody())));

        // when
        Endpoints result = kubernetesClient.endpoints(NAMESPACE);

        // then
        assertThat(extractIpPort(result.getAddresses()), containsInAnyOrder(SAMPLE_IP_PORT_1, SAMPLE_IP_PORT_2));
        assertThat(extractIpPort(result.getNotReadyAddresses()), containsInAnyOrder(SAMPLE_NOT_READY_IP));
    }

    @Test
    public void endpointsByNamespaceAndLabel() {
        // given
        String serviceLabel = "sample-service-label";
        String serviceLabelValue = "sample-service-label-value";
        stubFor(get(urlPathMatching(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE)))
                .withQueryParam("labelSelector", equalTo(String.format("%s=%s", serviceLabel, serviceLabelValue)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(endpointsListBody())));

        // when
        Endpoints result = kubernetesClient.endpointsByLabel(NAMESPACE, serviceLabel, serviceLabelValue);

        // then
        assertThat(extractIpPort(result.getAddresses()), containsInAnyOrder(SAMPLE_IP_PORT_1, SAMPLE_IP_PORT_2));
        assertThat(extractIpPort(result.getNotReadyAddresses()), containsInAnyOrder(SAMPLE_NOT_READY_IP));

    }

    @Test
    public void endpointsByNamespaceAndServiceName() {
        // given
        String serviceName = "service-name";
        stubFor(get(urlPathMatching(String.format("/api/v1/namespaces/%s/endpoints/%s", NAMESPACE, serviceName)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(endpointsBody())));

        // when
        Endpoints result = kubernetesClient.endpointsByName(NAMESPACE, serviceName);

        // then
        assertThat(extractIpPort(result.getAddresses()), containsInAnyOrder(SAMPLE_IP_PORT_1, SAMPLE_IP_PORT_2));
        assertTrue(result.getNotReadyAddresses().isEmpty());
    }

    private static List<String> extractIpPort(List<KubernetesClient.EntrypointAddress> addresses) {
        List<String> result = new ArrayList<String>();
        for (KubernetesClient.EntrypointAddress address : addresses) {
            String ip = address.getIp();
            String port = String.valueOf(address.getAdditionalProperties().get("hazelcast-service-port"));
            result.add(ipPort(ip, port));
        }
        return result;
    }

    private static String ipPort(String ip, String port) {
        return String.format("%s:%s", ip, port);
    }

    @Test(expected = KubernetesClientException.class)
    public void forbidden() {
        // given
        stubFor(get(urlEqualTo(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(501).withBody(forbiddenBody())));

        // when
        kubernetesClient.endpoints(NAMESPACE);
    }

    @Test(expected = ParseException.class)
    public void malformedResponse() {
        // given
        stubFor(get(urlEqualTo(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(malformedBody())));

        // when
        kubernetesClient.endpoints(NAMESPACE);
    }

    /**
     * Real string recorded from the Kubernetes API call "/api/v1/namespaces/{namespace}/endpoints".
     */
    private static String endpointsListBody() {
        return String.format("{\n" + "  \"kind\": \"EndpointsList\",\n" + "  \"apiVersion\": \"v1\",\n" + "  \"metadata\": {\n"
                        + "    \"selfLink\": \"/api/v1/namespaces/default/endpoints\",\n" + "    \"resourceVersion\": \"8792\"\n"
                        + "  },\n" + "  \"items\": [\n" + "    {\n" + "      \"metadata\": {\n" + "        \"name\": \"kubernetes\",\n"
                        + "        \"namespace\": \"default\",\n"
                        + "        \"selfLink\": \"/api/v1/namespaces/default/endpoints/kubernetes\",\n"
                        + "        \"uid\": \"01c5aaa4-8411-11e8-abd2-00155d395157\",\n" + "        \"resourceVersion\": \"38\",\n"
                        + "        \"creationTimestamp\": \"2018-07-10T07:15:21Z\"\n" + "      },\n" + "      \"subsets\": [\n"
                        + "        {\n" + "          \"addresses\": [\n" + "            {\n" + "              \"ip\": \"%s\",\n"
                        + "              \"hazelcast-service-port\" :\"%s\"\n" + "            }\n" + "          ],\n"
                        + "          \"ports\": [\n" + "            {\n" + "              \"name\": \"https\",\n"
                        + "              \"port\": 8443,\n" + "              \"protocol\": \"TCP\"\n" + "            }\n"
                        + "          ]\n" + "        }\n" + "      ]\n" + "    },\n" + "    {\n" + "      \"metadata\": {\n"
                        + "        \"name\": \"my-hazelcast\",\n" + "        \"namespace\": \"default\",\n"
                        + "        \"selfLink\": \"/api/v1/namespaces/default/endpoints/my-hazelcast\",\n"
                        + "        \"uid\": \"80f7f03d-8425-11e8-abd2-00155d395157\",\n" + "        \"resourceVersion\": \"8788\",\n"
                        + "        \"creationTimestamp\": \"2018-07-10T09:42:04Z\",\n" + "        \"labels\": {\n"
                        + "          \"app\": \"hazelcast\",\n" + "          \"chart\": \"hazelcast-1.0.0\",\n"
                        + "          \"heritage\": \"Tiller\",\n" + "          \"release\": \"my-hazelcast\"\n" + "        }\n"
                        + "      },\n" + "      \"subsets\": [\n" + "        {\n" + "          \"addresses\": [\n" + "            {\n"
                        + "              \"ip\": \"%s\",\n" + "              \"nodeName\": \"minikube\",\n"
                        + "              \"hazelcast-service-port\" : %s,\n" + "              \"targetRef\": {\n"
                        + "                \"kind\": \"Pod\",\n" + "                \"namespace\": \"default\",\n"
                        + "                \"name\": \"my-hazelcast-0\",\n"
                        + "                \"uid\": \"80f20bcb-8425-11e8-abd2-00155d395157\",\n"
                        + "                \"resourceVersion\": \"8771\"\n" + "              }\n" + "            }\n" + "          ],\n"
                        + "          \"notReadyAddresses\": [\n" + "            {\n" + "              \"ip\": \"%s\",\n"
                        + "              \"nodeName\": \"minikube\",\n" + "              \"targetRef\": {\n"
                        + "                \"kind\": \"Pod\",\n" + "                \"namespace\": \"default\",\n"
                        + "                \"name\": \"my-hazelcast-1\",\n"
                        + "                \"uid\": \"95bd3636-8425-11e8-abd2-00155d395157\",\n"
                        + "                \"resourceVersion\": \"8787\"\n" + "              }\n" + "            }\n" + "          ],\n"
                        + "          \"ports\": [\n" + "            {\n" + "              \"name\": \"hzport\",\n"
                        + "              \"port\": 5701,\n" + "              \"protocol\": \"TCP\"\n" + "            }\n"
                        + "          ]\n" + "        }\n" + "      ]\n" + "    }\n" + "  ]\n" + "}", SAMPLE_ADDRESS_1, SAMPLE_PORT_1,
                SAMPLE_ADDRESS_2, SAMPLE_PORT_2, SAMPLE_NOT_READY_ADDRESS);
    }

    /**
     * Real string recorded from the Kubernetes API call "/api/v1/namespaces/{namespace}/endpoints/{endpoint}".
     */
    private static String endpointsBody() {
        return String.format("{\n" + "  \"kind\": \"Endpoints\",\n" + "  \"apiVersion\": \"v1\",\n" + "  \"metadata\": {\n"
                        + "    \"name\": \"my-hazelcast\",\n" + "    \"namespace\": \"default\",\n"
                        + "    \"selfLink\": \"/api/v1/namespaces/default/endpoints/my-hazelcast\",\n"
                        + "    \"uid\": \"deefb354-8443-11e8-abd2-00155d395157\",\n" + "    \"resourceVersion\": \"18816\",\n"
                        + "    \"creationTimestamp\": \"2018-07-10T13:19:27Z\",\n" + "    \"labels\": {\n"
                        + "      \"app\": \"hazelcast\",\n" + "      \"chart\": \"hazelcast-1.0.0\",\n"
                        + "      \"heritage\": \"Tiller\",\n" + "      \"release\": \"my-hazelcast\"\n" + "    }\n" + "  },\n"
                        + "  \"subsets\": [\n" + "    {\n" + "      \"addresses\": [\n" + "        {\n" + "          \"ip\": \"%s\",\n"
                        + "          \"nodeName\": \"minikube\",\n" + "          \"hazelcast-service-port\" : %s,\n"
                        + "          \"targetRef\": {\n" + "            \"kind\": \"Pod\",\n"
                        + "            \"namespace\": \"default\",\n" + "            \"name\": \"my-hazelcast-0\",\n"
                        + "            \"uid\": \"def2f426-8443-11e8-abd2-00155d395157\",\n"
                        + "            \"resourceVersion\": \"18757\"\n" + "          }\n" + "        },\n" + "        {\n"
                        + "          \"ip\": \"%s\",\n" + "          \"nodeName\": \"minikube\",\n"
                        + "          \"hazelcast-service-port\" : %s,\n" + "          \"targetRef\": {\n"
                        + "            \"kind\": \"Pod\",\n" + "            \"namespace\": \"default\",\n"
                        + "            \"name\": \"my-hazelcast-1\",\n"
                        + "            \"uid\": \"f3b96106-8443-11e8-abd2-00155d395157\",\n"
                        + "            \"resourceVersion\": \"18815\"\n" + "          }\n" + "        }\n" + "      ],\n"
                        + "      \"ports\": [\n" + "        {\n" + "          \"name\": \"hzport\",\n" + "          \"port\": 5701,\n"
                        + "          \"protocol\": \"TCP\"\n" + "        }\n" + "      ]\n" + "    }\n" + "  ]\n" + "}", SAMPLE_ADDRESS_1,
                SAMPLE_PORT_1, SAMPLE_ADDRESS_2, SAMPLE_PORT_2);
    }

    private static String forbiddenBody() {
        return "Forbidden!Configured service account doesn't have access. Service account may have been revoked. "
                + "endpoints is forbidden: User \"system:serviceaccount:default:default\" cannot list endpoints "
                + "in the namespace \"default\"";
    }

    private static String malformedBody() {
        return "malformed response";
    }
}
