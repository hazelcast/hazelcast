package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LoginRequest implements ConsoleRequest {
    String groupName;
    String password;

    public LoginRequest() {
    }

    public LoginRequest(String groupName, String password) {
        this.groupName = groupName;
        this.password = password;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_LOGIN;
    }

    public void writeResponse(ManagementConsoleService mcs, DataOutput dos) throws Exception {
        boolean success = mcs.login(groupName, password);
        dos.writeBoolean(success);
        if (!success) {
            Thread.sleep(3000);
            throw new IOException("Invalid login!");
        }
    }

    public Boolean readResponse(DataInput in) throws IOException {
        return in.readBoolean();
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(groupName);
        out.writeUTF(password);
    }

    public void readData(DataInput in) throws IOException {
        groupName = in.readUTF();
        password = in.readUTF();
    }
}