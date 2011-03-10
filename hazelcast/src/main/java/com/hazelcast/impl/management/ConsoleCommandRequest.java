package com.hazelcast.impl.management;

import static com.hazelcast.nio.IOUtil.readLongString;
import static com.hazelcast.nio.IOUtil.writeLongString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ConsoleCommandRequest implements ConsoleRequest {
	
	private String command;

    public ConsoleCommandRequest() {
    }

	public ConsoleCommandRequest(String command) {
		super();
		this.command = command;
	}

	public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CONSOLE_COMMAND;
    }

    public void writeResponse(ManagementConsoleService mcs, DataOutput dos) throws Exception {
    	ConsoleCommandHandler handler = mcs.getCommandHandler();
    	try {
    		final String output = handler.handleCommand(command);
    		writeLongString(dos, output);
		} catch (Throwable e) {
			writeLongString(dos, "Error: " + e.getClass().getSimpleName() + "[" + e.getMessage() + "]");
		}
    }

    public Object readResponse(DataInput in) throws IOException {
        return readLongString(in);
    }

    public void writeData(DataOutput out) throws IOException {
    	out.writeUTF(command);
    }

    public void readData(DataInput in) throws IOException {
    	command = in.readUTF();
    }
}