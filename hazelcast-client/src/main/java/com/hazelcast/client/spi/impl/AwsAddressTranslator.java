package com.hazelcast.client.spi.impl;

import com.hazelcast.aws.AWSClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;

/**
 * AwsLookupTable loads ec2 ip addresses with given aws credentials.
 * Keeps a lookup table of private to public ip addresses.
 */
public class AwsAddressTranslator implements AddressTranslator {

    private static final ILogger LOGGER = Logger.getLogger(AwsAddressTranslator.class);
    private volatile Map<String, String> privateToPublic;
    private final AWSClient awsClient;
    private final boolean isInsideAws;

    public AwsAddressTranslator(ClientAwsConfig awsConfig) {
        awsClient = new AWSClient(awsConfig);
        isInsideAws = awsConfig.isInsideAws();
    }

    /**
     * @param address
     * @return public address of network whose private address is given, if address
     * not founds returns null.
     */
    public Address translate(Address address) {
        if (isInsideAws) {
            return address;
        }
        if (address == null) {
            return null;
        }
        String privateAddress = getLookupTable().get(address.getHost());
        if (privateAddress != null) {
            return constructPrivateAddress(privateAddress, address);
        }

        refresh();

        privateAddress = getLookupTable().get(address.getHost());
        if (privateAddress != null) {
            return constructPrivateAddress(privateAddress, address);
        }

        return null;
    }

    private Address constructPrivateAddress(String privateAddress, Address address) {
        try {
            return new Address(privateAddress, address.getPort());
        } catch (UnknownHostException e) {
            return null;
        }
    }

    private Map<String, String> getLookupTable() {
        Map<String, String> table = privateToPublic;
        return table != null ? table : Collections.<String, String>emptyMap();
    }

    public void refresh() {
        try {
            privateToPublic = awsClient.getAddresses();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Aws addresses are failed to load : " + e.getMessage());
        }
    }

}
