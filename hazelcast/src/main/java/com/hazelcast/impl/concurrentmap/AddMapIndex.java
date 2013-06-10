/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.impl.CMap;
import com.hazelcast.query.Expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;

public class AddMapIndex extends AbstractRemotelyProcessable {

    String mapName;
    private Expression expression;
    private boolean ordered;
    private int attributeIndex = -1;

    private transient Throwable error;

    public AddMapIndex() {
    }

    public AddMapIndex(String mapName, Expression expression, boolean ordered) {
        this.mapName = mapName;
        this.expression = expression;
        this.ordered = ordered;
    }

    public AddMapIndex(String mapName, Expression expression, boolean ordered, int attributeIndex) {
        this.mapName = mapName;
        this.attributeIndex = attributeIndex;
        this.setExpression(expression);
        this.setOrdered(ordered);
    }

    public void process() {
        CMap cmap = node.concurrentMapManager.getOrCreateMap(mapName);
        try {
            cmap.addIndex(getExpression(), isOrdered(), attributeIndex);
        } catch (Exception e) {
            error = e;
            node.getLogger(getClass().getName()).log(Level.WARNING, e.getMessage());
        }
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeBoolean(isOrdered());
        out.writeInt(attributeIndex);
        writeObject(out, getExpression());
    }

    public void readData(DataInput in) throws IOException {
        mapName = in.readUTF();
        setOrdered(in.readBoolean());
        attributeIndex = in.readInt();
        setExpression((Expression) readObject(in));
    }

    /**
     * @param ordered the ordered to set
     */
    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }

    /**
     * @return the ordered
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * @param expression the expression to set
     */
    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    /**
     * @return the expression
     */
    public Expression getExpression() {
        return expression;
    }

    public int getAttributeIndex() {
        return attributeIndex;
    }

    public void setAttributeIndex(int attributeIndex) {
        this.attributeIndex = attributeIndex;
    }

    public Throwable getError() {
        return error;
    }
}
