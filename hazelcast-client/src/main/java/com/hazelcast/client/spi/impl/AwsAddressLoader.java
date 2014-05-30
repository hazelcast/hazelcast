package com.hazelcast.client.spi.impl;

import com.hazelcast.aws.AWSClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.connection.ServiceAddressLoader;
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
 * Aws Address Loader. Calls aws api to load ip addresses related to given credentails.
 */
public class AwsAddressLoader implements ServiceAddressLoader {

    private static final ILogger logger = Logger.getLogger(AwsAddressLoader.class);
    private final AWSClient awsClient;
    private volatile Map<String, String> privateToPublic;
    private final boolean isInsideAws;

    AwsAddressLoader(ClientAwsConfig awsConfig) {
        awsClient = new AWSClient(awsConfig);
        isInsideAws = awsConfig.isInsideAws();
    }

    @Override
    public Collection<InetSocketAddress> loadAddresses() {
        final Map<String, String> lookupTable = getLookupTable();
        final Collection<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>(lookupTable.size());

//        if (isInsideAws) { //we can use private addresses if client is inside aws
            for (String privateAddress : lookupTable.keySet()) {
                addresses.addAll(AddressHelper.getSocketAddresses(privateAddress));
            }
            return addresses;
//        }

//        //Use public addresses if client is outside aws
//        for (String publicAddress : lookupTable.values()) {
//            addresses.addAll(AddressHelper.getSocketAddresses(publicAddress));
//        }
//        return addresses;

    }

    @Override
    public void clear() {
        getLookupTable().clear();
        privateToPublic = null;
    }

    private Map<String, String> getLookupTable() {
        Map<String, String> table = privateToPublic;
        return table != null ? table : Collections.<String, String>emptyMap();
    }

    @Override
    public void updateLookupTable() {
        try {
            privateToPublic = awsClient.getAddresses();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Aws addresses are failed to load : " + e.getMessage());
        }
    }
}
