package com.hazelcast.client.spi.impl;

import com.hazelcast.aws.AWSClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;

/**
 * Aws Address Loader. Calls aws api to load ip addresses related to given credentials.
 */
public class AwsAddressProvider implements AddressProvider {

    private static final ILogger logger = Logger.getLogger(AwsAddressProvider.class);
    private final AWSClient awsClient;
    private volatile Map<String, String> privateToPublic;

    AwsAddressProvider(ClientAwsConfig awsConfig) {
        awsClient = new AWSClient(awsConfig);
    }

    @Override
    public Collection<InetSocketAddress> loadAddresses() {
        updateLookupTable();
        final Map<String, String> lookupTable = getLookupTable();
        final Collection<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>(lookupTable.size());

        for (String privateAddress : lookupTable.keySet()) {
            addresses.addAll(AddressHelper.getSocketAddresses(privateAddress));
        }
        return addresses;

    }

    private Map<String, String> getLookupTable() {
        Map<String, String> table = privateToPublic;
        return table != null ? table : Collections.<String, String>emptyMap();
    }

    private void updateLookupTable() {
        try {
            privateToPublic = awsClient.getAddresses();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Aws addresses are failed to load : " + e.getMessage());
        }
    }
}
