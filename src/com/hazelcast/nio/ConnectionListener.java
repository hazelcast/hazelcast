package com.hazelcast.nio;

public interface ConnectionListener {
	void connectionAdded(Connection connection);
	void connectionRemoved (Connection connection);
}
