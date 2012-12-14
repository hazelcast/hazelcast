package com.hazelcast.impl.management;

import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: msk
 * Date: 11.12.2012
 * Time: 12:29
 */
public class MemberConfigRequest implements ConsoleRequest{

    public MemberConfigRequest(){
        super();
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MEMBER_CONFIG;
    }

    public Object readResponse(DataInput in) throws IOException {
        return in.readUTF();
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        String clusterXml;
        clusterXml = new ConfigXmlGenerator(true).generate(mcs.getHazelcastInstance().getConfig());
        dos.writeUTF(clusterXml);
    }

    public void writeData(DataOutput out) throws IOException {
    }

    public void readData(DataInput in) throws IOException {
    }
}
