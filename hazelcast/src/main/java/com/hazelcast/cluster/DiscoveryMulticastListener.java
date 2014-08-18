package com.hazelcast.cluster;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;

import java.util.Set;

import static com.hazelcast.util.AddressUtil.matchAnyInterface;
import static java.lang.String.format;

public class DiscoveryMulticastListener implements MulticastListener {

    final Node node;
    final Set<String> trustedInterfaces;
    final ILogger logger;

    public DiscoveryMulticastListener(Node node) {
        this.node = node;
        this.trustedInterfaces = node.getConfig().getNetworkConfig()
                .getJoin().getMulticastConfig().getTrustedInterfaces();
        this.logger = node.getLogger("DiscoveryMulticastListener");
    }

    @Override
    public void onMessage(Object msg) {
        // Do not log the type check as the Multicast Service handles multiple message types.
        if(!isDiscoveryMessage(msg)) return;

        if (!isValidDiscoveryMessage(msg)) {
            logDroppedMessage(msg);
            return;
        }

        DiscoveryMessage discoveryMessage = (DiscoveryMessage) msg;
        handleDiscoveryRequest(discoveryMessage);
    }

    private void logDroppedMessage(Object msg) {
        if (logger.isFinestEnabled()) {
            logger.info("Dropped: " + msg);
        }
    }

    private void logDiscoveryMessageDropped(String host) {
        if (logger.isFinestEnabled()) {
            logger.finest(format(
                    "DiscoveryMessage from %s is dropped because its sender is not a trusted interface", host));
        }
    }

    private void handleDiscoveryRequest(DiscoveryMessage request) {
        String host = request.getAddress().getHost();
        if (trustedInterfaces.isEmpty() || matchAnyInterface(host, trustedInterfaces)) {

            DiscoveryMessage response = new DiscoveryMessage(request.getPacketVersion(), request.getBuildNumber(),
                    node.getThisAddress(), request.getConfigCheck(), node.getClusterService().getSize());

            node.multicastService.send(response);
        } else {
            logDiscoveryMessageDropped(host);
        }

    }

    private boolean isDiscoveryMessage(Object msg) {
        return msg != null && msg instanceof DiscoveryMessage;
    }

    private boolean isValidDiscoveryMessage(Object msg) {
        if (!isDiscoveryMessage(msg)) {
            return false;
        }

        DiscoveryMessage discoveryMessage = (DiscoveryMessage) msg;

        if (isDiscoveryResponse(discoveryMessage)) {
            return false;
        }

        if (isMessageToSelf(discoveryMessage)) {
            return false;
        }

        try {
            return node.getClusterService().validateDiscoveryMessage(discoveryMessage);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isDiscoveryResponse(DiscoveryMessage discoveryMessage) {
        return discoveryMessage.getMemberCount() > 0;
    }

    private boolean isMessageToSelf(DiscoveryMessage discoveryMessage) {
        Address thisAddress = node.getThisAddress();
        return thisAddress == null || thisAddress.equals(discoveryMessage.getAddress());
    }
}