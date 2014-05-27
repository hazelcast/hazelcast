package com.hazelcast.cluster;

import com.hazelcast.instance.Node;

/**
 * Responsible for creating (or returning) Joiner instances. Implementations are expected to
 * have a single, no-arg constructor only.
 *
 * @author dturner@kixeye.com
 */
public interface JoinerFactory {

    /**
     * Creates a Joiner.
     * @param node
     * @return
     */
    Joiner createJoiner(Node node);
}
