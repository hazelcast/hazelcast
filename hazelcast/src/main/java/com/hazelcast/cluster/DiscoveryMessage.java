package com.hazelcast.cluster;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class DiscoveryMessage implements DataSerializable {

    protected byte packetVersion;
    protected int buildNumber;
    protected Address address;
    protected ClientConfigCheck configCheck;
    protected int memberCount;

    public DiscoveryMessage() {
    }

    public DiscoveryMessage(byte packetVersion, int buildNumber, Address address, ClientConfigCheck configCheck,
                            int memberCount) {
        this.packetVersion = packetVersion;
        this.buildNumber = buildNumber;
        this.address = address;
        this.configCheck = configCheck;
        this.memberCount = memberCount;
    }

    public byte getPacketVersion() {
        return packetVersion;
    }

    public int getBuildNumber() {
        return buildNumber;
    }

    public Address getAddress() {
        return address;
    }

    public ClientConfigCheck getConfigCheck() {
        return configCheck;
    }

    public int getMemberCount() {
        return memberCount;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        packetVersion = in.readByte();
        buildNumber = in.readInt();
        address = new Address();
        address.readData(in);
        configCheck = new ClientConfigCheck();
        configCheck.readData(in);
        memberCount = in.readInt();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(packetVersion);
        out.writeInt(buildNumber);
        address.writeData(out);
        configCheck.writeData(out);
        out.writeInt(memberCount);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("DiscoverMessage");
        sb.append("{packetVersion=").append(packetVersion);
        sb.append(", buildNumber=").append(buildNumber);
        sb.append(", address=").append(address);
        sb.append('}');
        return sb.toString();
    }
}
