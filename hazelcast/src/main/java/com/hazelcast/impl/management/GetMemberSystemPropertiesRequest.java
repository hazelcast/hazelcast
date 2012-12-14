package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: msk
 * Date: 12.12.2012
 * Time: 12:48
 */
public class GetMemberSystemPropertiesRequest implements ConsoleRequest {

    public GetMemberSystemPropertiesRequest(){
        super();
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MEMBER_SYSTEM_PROPERTIES;
    }

    public Object readResponse(DataInput in) throws IOException {
        Map<String,String> properties = new LinkedHashMap<String, String>();
        int size = in.readInt();
        String[] temp;
        for(int i = 0; i < size ; i++){
            temp = in.readUTF().split(":#");
            properties.put(temp[0],temp.length == 1 ? "" : temp[1] );
        }
        return properties;
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        Properties properties = System.getProperties();
        dos.writeInt(properties.size());

        for (Object property : properties.keySet()) {
            dos.writeUTF((String)property + ":#" + (String)properties.get(property) );
        }
    }

    public void writeData(DataOutput out) throws IOException {

    }

    public void readData(DataInput in) throws IOException {

    }
}
