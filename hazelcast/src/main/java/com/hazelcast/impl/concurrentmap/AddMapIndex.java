/**
 * 
 */
package com.hazelcast.impl.concurrentmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.impl.CMap;
import com.hazelcast.query.Expression;

public class AddMapIndex extends AbstractRemotelyProcessable {
	
    String mapName;
    private Expression expression;
    private boolean ordered;

    public AddMapIndex() {
    }

    public AddMapIndex(String mapName, Expression expression, boolean ordered) {
        this.mapName = mapName;
        this.setExpression(expression);
        this.setOrdered(ordered);
    }

    public void process() {
        CMap cmap = getNode().concurrentMapManager.getOrCreateMap(mapName);
        cmap.addIndex(getExpression(), isOrdered());
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeBoolean(isOrdered());
        writeObject(out, getExpression());
    }

    public void readData(DataInput in) throws IOException {
        mapName = in.readUTF();
        setOrdered(in.readBoolean());
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
}