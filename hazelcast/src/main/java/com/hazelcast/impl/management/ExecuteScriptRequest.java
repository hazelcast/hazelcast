package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.Set;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.SerializationHelper;

public class ExecuteScriptRequest implements ConsoleRequest {
	
	private static final byte NULL = 0;
	private static final byte MAP = 1;
	private static final byte COLLECTION = 2;
	private static final byte OTHER = -1;
	
    String script;
    String engine;
    Set<Address> targets;
    boolean allMembers = false;
    
    public ExecuteScriptRequest() {
    }
    
    public ExecuteScriptRequest(String script, String engine) {
    	this.script = script;
        this.engine = engine;
        this.targets = new HashSet<Address>(0);
    }
    
    public ExecuteScriptRequest(String script, String engine, boolean allMembers) {
    	this.script = script;
        this.engine = engine;
        this.targets = new HashSet<Address>(0);
        this.allMembers = allMembers;
    }
    
    public ExecuteScriptRequest(String script, String engine, Set<Address> targets) {
    	this.script = script;
        this.targets = targets;
        this.engine = engine;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_EXECUTE_SCRIPT;
    }

	public void writeResponse(ManagementConsoleService mcs, DataOutputStream dos) throws Exception {
		Object result = null;
		Callable callable = new ScriptExecutorCallable(engine, script);
		
		if(allMembers) {
			result = mcs.callOnAllMembers(callable);
		}
		else if(targets.isEmpty()) {
			result = mcs.call(callable);
		}
		else if(targets.size() == 1) {
			result = mcs.call(targets.iterator().next(), callable);
		}
		else {
			result = mcs.callOnMembers(targets, callable);
		}
		
		if(result != null) {
			if(result instanceof Map) {
				dos.writeByte(MAP);
				writeMap(dos, (Map) result);
			}
			else if(result instanceof Collection) {
				dos.writeByte(COLLECTION);
				writeCollection(dos, (Collection) result);
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
			
		case COLLECTION:
			return readCollection(in);
		
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
    	int size = in.readInt();
    	Map props = new HashMap(size);
    	if(size > 0) {
    		for (int i = 0; i < size; i++) {
				Object key = SerializationHelper.readObject(in);
				Object value = SerializationHelper.readObject(in);
				props.put(key, value);
			}
    	}
        return props;
    }
    
    private void writeCollection(DataOutputStream dos, Collection result) throws IOException {
    	int size = result != null ? result.size() : 0;
		dos.writeInt(size);
		if(size > 0) {
			Iterator iter = result.iterator();
			while(iter.hasNext()) {
				SerializationHelper.writeObject(dos, iter.next());
			}
		}
    }

    private Collection readCollection(DataInputStream in) throws IOException {
    	int size = in.readInt();
    	Collection coll = new ArrayList(size);
    	if(size > 0) {
    		for (int i = 0; i < size; i++) {
				Object value = SerializationHelper.readObject(in);
				coll.add(value);
			}
    	}
        return coll;
    }
    
    public void writeData(DataOutput out) throws IOException {
    	out.writeUTF(script);
    	out.writeUTF(engine);
    	out.writeBoolean(allMembers);
    	
    	out.writeInt(targets.size());
    	for (Address target : targets) {
			target.writeData(out);
		}
    }

    public void readData(DataInput in) throws IOException {
    	script = in.readUTF();
    	engine = in.readUTF();
    	allMembers = in.readBoolean();
    	
    	int size = in.readInt();
    	targets = new HashSet<Address>(size);
    	for (int i = 0; i < size; i++) {
    		Address target = new Address();
    		targets.add(target);
    		target.readData(in);
		}
    }
}
