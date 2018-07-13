package com.hazelcast.kubernetes;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
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

    @Test(expected = KubernetesClientException.class)
    public void forbidden() {
        // given
        stubFor(get(urlEqualTo(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(501).withBody(forbiddenBody())));

        // when
        kubernetesClient.endpoints(NAMESPACE);
    }

    @Test(expected = KubernetesClientException.class)
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
        return String.format(
                "{" +
                        "  \"kind\": \"EndpointsList\"," +
                        "  \"apiVersion\": \"v1\"," +
                        "  \"metadata\": {" +
                        "    \"selfLink\": \"/api/v1/namespaces/default/endpoints\"," +
                        "    \"resourceVersion\": \"8792\"" +
                        "  }," +
                        "  \"items\": [" +
                        "    {" +
                        "      \"metadata\": {" +
                        "        \"name\": \"kubernetes\"," +
                        "        \"namespace\": \"default\"," +
                        "        \"selfLink\": \"/api/v1/namespaces/default/endpoints/kubernetes\"," +
                        "        \"uid\": \"01c5aaa4-8411-11e8-abd2-00155d395157\"," +
                        "        \"resourceVersion\": \"38\"," +
                        "        \"creationTimestamp\": \"2018-07-10T07:15:21Z\"" +
                        "      }," +
                        "      \"subsets\": [" +
                        "        {" +
                        "          \"addresses\": [" +
                        "            {" +
                        "              \"ip\": \"%s\"," +
                        "              \"hazelcast-service-port\" :\"%s\"" +
                        "            }" +
                        "          ]," +
                        "          \"ports\": [" +
                        "            {" +
                        "              \"name\": \"https\"," +
                        "              \"port\": 8443," +
                        "              \"protocol\": \"TCP\"" +
                        "            }" +
                        "          ]" +
                        "        }" +
                        "      ]" +
                        "    }," +
                        "    {" +
                        "      \"metadata\": {" +
                        "        \"name\": \"my-hazelcast\"," +
                        "        \"namespace\": \"default\"," +
                        "        \"selfLink\": \"/api/v1/namespaces/default/endpoints/my-hazelcast\"," +
                        "        \"uid\": \"80f7f03d-8425-11e8-abd2-00155d395157\"," +
                        "        \"resourceVersion\": \"8788\"," +
                        "        \"creationTimestamp\": \"2018-07-10T09:42:04Z\"," +
                        "        \"labels\": {" +
                        "          \"app\": \"hazelcast\"," +
                        "          \"chart\": \"hazelcast-1.0.0\"," +
                        "          \"heritage\": \"Tiller\"," +
                        "          \"release\": \"my-hazelcast\"" +
                        "        }" +
                        "      }," +
                        "      \"subsets\": [" +
                        "        {" +
                        "          \"addresses\": [" +
                        "            {" +
                        "              \"ip\": \"%s\"," +
                        "              \"nodeName\": \"minikube\"," +
                        "              \"hazelcast-service-port\" : %s," +
                        "              \"targetRef\": {" +
                        "                \"kind\": \"Pod\"," +
                        "                \"namespace\": \"default\"," +
                        "                \"name\": \"my-hazelcast-0\"," +
                        "                \"uid\": \"80f20bcb-8425-11e8-abd2-00155d395157\"," +
                        "                \"resourceVersion\": \"8771\"" +
                        "              }" +
                        "            }" +
                        "          ]," +
                        "          \"notReadyAddresses\": [" +
                        "            {" +
                        "              \"ip\": \"%s\"," +
                        "              \"nodeName\": \"minikube\"," +
                        "              \"targetRef\": {" +
                        "                \"kind\": \"Pod\"," +
                        "                \"namespace\": \"default\"," +
                        "                \"name\": \"my-hazelcast-1\"," +
                        "                \"uid\": \"95bd3636-8425-11e8-abd2-00155d395157\"," +
                        "                \"resourceVersion\": \"8787\"" +
                        "              }" +
                        "            }" +
                        "          ]," +
                        "          \"ports\": [" +
                        "            {" +
                        "              \"name\": \"hzport\"," +
                        "              \"port\": 5701," +
                        "              \"protocol\": \"TCP\"" +
                        "            }" +
                        "          ]" +
                        "        }" +
                        "      ]" +
                        "    }" +
                        "  ]" +
                        "}"
                , SAMPLE_ADDRESS_1, SAMPLE_PORT_1, SAMPLE_ADDRESS_2, SAMPLE_PORT_2, SAMPLE_NOT_READY_ADDRESS);
    }

    /**
     * Real string recorded from the Kubernetes API call "/api/v1/namespaces/{namespace}/endpoints/{endpoint}".
     */
    private static String endpointsBody() {
        return String.format(
                "{" +
                        "  \"kind\": \"Endpoints\"," +
                        "  \"apiVersion\": \"v1\"," +
                        "  \"metadata\": {" +
                        "    \"name\": \"my-hazelcast\"," +
                        "    \"namespace\": \"default\"," +
                        "    \"selfLink\": \"/api/v1/namespaces/default/endpoints/my-hazelcast\"," +
                        "    \"uid\": \"deefb354-8443-11e8-abd2-00155d395157\"," +
                        "    \"resourceVersion\": \"18816\"," +
                        "    \"creationTimestamp\": \"2018-07-10T13:19:27Z\"," +
                        "    \"labels\": {" +
                        "      \"app\": \"hazelcast\"," +
                        "      \"chart\": \"hazelcast-1.0.0\"," +
                        "      \"heritage\": \"Tiller\"," +
                        "      \"release\": \"my-hazelcast\"" +
                        "    }" +
                        "  }," +
                        "  \"subsets\": [" +
                        "    {" +
                        "      \"addresses\": [" +
                        "        {" +
                        "          \"ip\": \"%s\"," +
                        "          \"nodeName\": \"minikube\"," +
                        "          \"hazelcast-service-port\" : %s," +
                        "          \"targetRef\": {" +
                        "            \"kind\": \"Pod\"," +
                        "            \"namespace\": \"default\"," +
                        "            \"name\": \"my-hazelcast-0\"," +
                        "            \"uid\": \"def2f426-8443-11e8-abd2-00155d395157\"," +
                        "            \"resourceVersion\": \"18757\"" +
                        "          }" +
                        "        }," +
                        "        {" +
                        "          \"ip\": \"%s\"," +
                        "          \"nodeName\": \"minikube\"," +
                        "          \"hazelcast-service-port\" : %s," +
                        "          \"targetRef\": {" +
                        "            \"kind\": \"Pod\"," +
                        "            \"namespace\": \"default\"," +
                        "            \"name\": \"my-hazelcast-1\"," +
                        "            \"uid\": \"f3b96106-8443-11e8-abd2-00155d395157\"," +
                        "            \"resourceVersion\": \"18815\"" +
                        "          }" +
                        "        }" +
                        "      ]," +
                        "      \"ports\": [" +
                        "        {" +
                        "          \"name\": \"hzport\"," +
                        "          \"port\": 5701," +
                        "          \"protocol\": \"TCP\"" +
                        "        }" +
                        "      ]" +
                        "    }" +
                        "  ]" +
                        "}"
                , SAMPLE_ADDRESS_1, SAMPLE_PORT_1, SAMPLE_ADDRESS_2, SAMPLE_PORT_2);
    }

    private static String forbiddenBody() {
        return "Forbidden!Configured service account doesn't have access. Service account may have been revoked. "
                + "endpoints is forbidden: User \"system:serviceaccount:default:default\" cannot list endpoints "
                + "in the namespace \"default\"";
    }

    private static String malformedBody() {
        return "malformed response";
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
}
