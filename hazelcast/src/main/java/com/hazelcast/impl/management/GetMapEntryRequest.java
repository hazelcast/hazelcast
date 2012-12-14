package com.hazelcast.impl.management;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created with IntelliJ IDEA.
 * User: msk
 * Date: 12.12.2012
 * Time: 14:40
 */
public class GetMapEntryRequest implements ConsoleRequest {

    private String mapName;
    private String type;
    private String key;

    public GetMapEntryRequest(){
        super();
    }

    public GetMapEntryRequest(String type, String mapName, String key){
        this.type = type;
        this.mapName = mapName;
        this.key = key;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MAP_ENTRY;
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
        IMap map = mcs.getHazelcastInstance().getMap(mapName);

        MapEntry entry = null;
        if(type.equals("string")) {
            entry = map.getMapEntry(key);
        }else if(type.equals("long")) {
            entry = map.getMapEntry(Long.valueOf(key));
        }else if(type.equals("integer")) {
            entry = map.getMapEntry(Integer.valueOf(key));
        }

        TreeMap result = new TreeMap();

        if(entry == null ){
            result.put("No Value Found!", " ");
        }
        Object value = entry.getValue();

        result.put("browse_value", value != null ? value.toString() : "null");
        result.put("browse_class", value != null ? value.getClass().getName() : "null");

        result.put("memory_cost",  Long.toString(entry.getCost()));
        result.put("date_creation_time",  Long.toString(entry.getCreationTime()));
        result.put("date_expiration_time",  Long.toString(entry.getExpirationTime()));
        result.put("browse_hits", Integer.toString(entry.getHits()));
        result.put("date_access_time",  Long.toString(entry.getLastAccessTime()));
        result.put("date_update_time",  Long.toString(entry.getLastUpdateTime( )));
        result.put("browse_version", Long.toString(entry.getVersion()));
        result.put("boolean_valid", Boolean.toString(entry.isValid()));

        dos.writeInt(result.size());

        for (Object property : result.keySet()) {
            dos.writeUTF((String)property + ":#" + (String)result.get(property) );
        }

    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(type);
        out.writeUTF(mapName);
        out.writeUTF(key);
    }

    public void readData(DataInput in) throws IOException {
        type = in.readUTF();
        mapName = in.readUTF();
        key = in.readUTF();
    }
}
