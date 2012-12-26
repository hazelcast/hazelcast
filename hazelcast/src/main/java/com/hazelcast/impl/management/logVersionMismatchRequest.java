/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.impl.management;

import com.hazelcast.logging.ILogger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;

// author: sancar - 17.12.2012
public class LogVersionMismatchRequest implements ConsoleRequest {

    private String manCenterVersion;

    public LogVersionMismatchRequest(String manCenterVersion){
        this.manCenterVersion = manCenterVersion;
    }

    public LogVersionMismatchRequest(){
        super();
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_LOG_VERSION_MISMATCH;
    }

    public Object readResponse(DataInput in) throws IOException {
        return "SUCCESS";
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        final ILogger logger = mcs.getHazelcastInstance().node.getLogger(LogVersionMismatchRequest.class.getName());
        mcs.setVersionMismatch(true);
        logger.log(Level.SEVERE, "The version of the management center is " + manCenterVersion);
    }

    public void writeData(DataOutput out) throws IOException {
       out.writeUTF(manCenterVersion);
    }

    public void readData(DataInput in) throws IOException {
       manCenterVersion = in.readUTF();
    }
}
