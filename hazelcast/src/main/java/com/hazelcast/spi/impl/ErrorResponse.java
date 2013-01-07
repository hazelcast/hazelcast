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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.logging.Level;

/**
 * @mdogan 10/19/12
 */
public class ErrorResponse extends AbstractOperation {

    private Address endPoint;
    private Throwable error;

    public ErrorResponse() {
    }

    public ErrorResponse(final Address endPoint, final Throwable error) {
        this.endPoint = endPoint;
        this.error = error;
    }

    public void run() {
        NodeEngine nodeEngine = getNodeEngine();
        nodeEngine.getLogger(NodeEngine.class.getName()).log(Level.SEVERE,
                "Error while executing operation on " + endPoint, error);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        endPoint.writeData(out);
        out.writeObject(error);
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        endPoint = new Address();
        endPoint.readData(in);
        error = in.readObject();
    }
}
