package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.SerializationHelper;

public class ExecuteScriptRequest implements ConsoleRequest {
	
	private static final byte NULL = 0;
	private static final byte MAP = 1;
	private static final byte OTHER = -1;
	
    Address target;
    String script;
    String engine;
    
    public ExecuteScriptRequest() {
    }

    public ExecuteScriptRequest(String script, Address target) {
    	this(script, ConsoleRequestConstants.SCRIPT_JAVASCRIPT, target);
    }
    
    public ExecuteScriptRequest(String script, String engine, Address target) {
    	this.script = script;
        this.target = target;
        this.engine = engine;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_EXECUTE_SCRIPT;
    }

	public void writeResponse(ManagementConsoleService mcs, DataOutputStream dos) throws Exception {
		Object result = mcs.call(target, new ScriptExecutorCallable(engine, script));
		if(result != null) {
			if(result instanceof Map) {
				dos.writeByte(MAP);
				writeMap(dos, (Map) result);
			}
			else {
				dos.writeByte(OTHER);
				SerializationHelper.writeObject(dos, result);
			}
		}
		else {
			dos.writeByte(NULL);
		}
    }

    public Object readResponse(DataInputStream in) throws IOException {
    	byte flag = in.readByte();
    	switch (flag) {
		case MAP:
			return readMap(in);
		
		case OTHER:
			return SerializationHelper.readObject(in);
		}
    	return null;
    }
    
    private void writeMap(DataOutputStream dos, Map result) throws IOException {
    	int size = result != null ? result.size() : 0;
		dos.writeInt(size);
		if(size > 0) {
			Set<Entry<Object, Object>> entries = result.entrySet();
			for (Entry<Object, Object> entry : entries) {
				SerializationHelper.writeObject(dos, entry.getKey());
				SerializationHelper.writeObject(dos, entry.getValue());
			}
		}
    }

    private Map readMap(DataInputStream in) throws IOException {
    	Map props = new HashMap();
    	int size = in.readInt();
    	if(size > 0) {
    		for (int i = 0; i < size; i++) {
				Object key = SerializationHelper.readObject(in);
				Object value = SerializationHelper.readObject(in);
				props.put(key, value);
			}
    	}
        return props;
    }
    
    public void writeData(DataOutput out) throws IOException {
    	out.writeUTF(script);
    	out.writeUTF(engine);
        target.writeData(out);
    }

    public void readData(DataInput in) throws IOException {
    	script = in.readUTF();
    	engine = in.readUTF();
        target = new Address();
        target.readData(in);
    }
}
