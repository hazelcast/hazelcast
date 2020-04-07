/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.aws;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.hazelcast.aws.AwsEc2RequestSigner.SIGNATURE_METHOD_V4;
import static com.hazelcast.aws.AwsUrlUtils.canonicalQueryString;
import static com.hazelcast.aws.StringUtils.isNotEmpty;

/**
 * Responsible for connecting to AWS EC2 Describe Instances API.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeInstances.html">EC2 Describe Instances</a>
 */
class AwsDescribeInstancesApi {
    private static final ILogger LOGGER = Logger.getLogger(AwsDescribeInstancesApi.class);

    private static final int TIMESTAMP_FIELD_LENGTH = 8;

    private final AwsConfig awsConfig;
    private final AwsEc2RequestSigner requestSigner;
    private final Clock clock;

    AwsDescribeInstancesApi(AwsConfig awsConfig, AwsEc2RequestSigner requestSigner, Clock clock) {
        this.awsConfig = awsConfig;
        this.requestSigner = requestSigner;
        this.clock = clock;
    }

    /**
     * Invoke the service to describe the instances, unmarshal the response and return the discovered node map.
     * The map contains mappings from private to public IP and all contained nodes match the filtering rules defined by
     * the {@link AwsConfig}.
     *
     * @return map from private to public IP or empty map in case of failed response unmarshalling
     */
    Map<String, String> addresses(String region, String endpoint, AwsCredentials credentials) {
        Map<String, String> attributes = createAttributes(region, endpoint, credentials);
        return callServiceWithRetries(endpoint, attributes);
    }

    private Map<String, String> createAttributes(String region, String endpoint, AwsCredentials credentials) {
        Map<String, String> attributes = new HashMap<>();

        if (credentials.getToken() != null) {
            attributes.put("X-Amz-Security-Token", credentials.getToken());
        }
        attributes.put("Action", "DescribeInstances");
        attributes.put("Version", "2016-11-15");
        attributes.put("X-Amz-SignedHeaders", "host");
        attributes.put("X-Amz-Expires", "30");

        String timestamp = formatCurrentTimestamp();
        attributes.put("X-Amz-Date", timestamp);
        attributes.put("X-Amz-Credential", formatCredentials(region, credentials, timestamp));

        attributes.putAll(filterAttributes());
        attributes.put("X-Amz-Algorithm", SIGNATURE_METHOD_V4);
        attributes.put("X-Amz-Signature", requestSigner.sign(attributes, region, endpoint, credentials, timestamp));

        return attributes;
    }

    private String formatCurrentTimestamp() {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df.format(Instant.now(clock).toEpochMilli());
    }

    private static String formatCredentials(String region, AwsCredentials credentials, String timestamp) {
        return String.format("%s/%s/%s/ec2/aws4_request",
            credentials.getAccessKey(),
            timestamp.substring(0, TIMESTAMP_FIELD_LENGTH),
            region);
    }

    private Map<String, String> filterAttributes() {
        Filter filter = new Filter();
        if (isNotEmpty(awsConfig.getTagKey())) {
            if (isNotEmpty(awsConfig.getTagValue())) {
                filter.addFilter("tag:" + awsConfig.getTagKey(), awsConfig.getTagValue());
            } else {
                filter.addFilter("tag-key", awsConfig.getTagKey());
            }
        } else if (isNotEmpty(awsConfig.getTagValue())) {
            filter.addFilter("tag-value", awsConfig.getTagValue());
        }

        if (isNotEmpty(awsConfig.getSecurityGroupName())) {
            filter.addFilter("instance.group-name", awsConfig.getSecurityGroupName());
        }

        filter.addFilter("instance-state-name", "running");
        return filter.getFilterAttributes();
    }

    private Map<String, String> callServiceWithRetries(String endpoint, Map<String, String> attributes) {
        return RetryUtils.retry(() -> callService(endpoint, attributes),
            awsConfig.getConnectionRetries());
    }

    private Map<String, String> callService(String endpoint, Map<String, String> attributes) {
        String query = canonicalQueryString(attributes);
        String response = RestClient.create(urlFor(endpoint, query))
            .withConnectTimeoutSeconds(awsConfig.getConnectionTimeoutSeconds())
            .withReadTimeoutSeconds(awsConfig.getReadTimeoutSeconds())
            .get();
        return parse(response);
    }

    private static String urlFor(String endpoint, String query) {
        if (endpoint.startsWith("http")) {
            return endpoint + "/?" + query;
        }
        return "https://" + endpoint + "/?" + query;
    }

    private static Map<String, String> parse(String xmlResponse) {
        try {
            return tryParse(xmlResponse);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> tryParse(String xmlResponse) throws Exception {
        return XmlNode.create(xmlResponse)
            .getSubNodes("reservationset").stream()
            .flatMap(e -> e.getSubNodes("item").stream())
            .flatMap(e -> e.getSubNodes("instancesset").stream())
            .flatMap(e -> e.getSubNodes("item").stream())
            .filter(e -> e.getValue("privateipaddress") != null)
            .peek(AwsDescribeInstancesApi::logInstanceName)
            .collect(Collectors.toMap(
                e -> e.getValue("privateipaddress"),
                e -> e.getValue("ipaddress"))
            );
    }

    private static void logInstanceName(XmlNode item) {
        LOGGER.fine(String.format("Accepting EC2 instance [%s][%s]",
            parseInstanceName(item).orElse("<unknown>"),
            item.getValue("privateipaddress")));
    }

    private static Optional<String> parseInstanceName(XmlNode nodeHolder) {
        return nodeHolder.getSubNodes("tagset").stream()
            .flatMap(e -> e.getSubNodes("item").stream())
            .filter(AwsDescribeInstancesApi::isNameField)
            .flatMap(e -> e.getSubNodes("value").stream())
            .map(XmlNode::getNode)
            .map(Node::getFirstChild)
            .map(Node::getNodeValue)
            .findFirst();
    }

    private static boolean isNameField(XmlNode item) {
        return item.getSubNodes("key").stream()
            .map(XmlNode::getNode)
            .map(Node::getFirstChild)
            .map(Node::getNodeValue)
            .map("Name"::equals)
            .findFirst()
            .orElse(false);
    }
}
