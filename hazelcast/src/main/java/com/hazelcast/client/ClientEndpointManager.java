package com.hazelcast.client;

import com.hazelcast.nio.Connection;

import java.util.Collection;
import java.util.Set;

/**
 * A manager for {@link com.hazelcast.client.ClientEndpoint}s.
 *
 * All the methods are thread-safe.
 */
public interface ClientEndpointManager {

    /**
     * Returns the current endpoints.
     *
     * @return the endpoints.
     */
    Collection<ClientEndpoint> getEndpoints();

    /**
     * Gets the endpoint for a given connection.
     *
     * @param connection the connection to the endpoint.
     * @return the found endpoint or null of no endpoint was found.
     * @throws java.lang.NullPointerException if connection is null.
     */
    ClientEndpoint getEndpoint(Connection connection);

    /**
     * Gets all the endpoints for a given client.
     *
     * @param clientUuid the uuid of the client
     * @return a set of all the endpoints for the client. If no endpoints are found, an empty set is returned.
     * @throws java.lang.NullPointerException if clientUuid is null.
     */
    Set<ClientEndpoint> getEndpoints(String clientUuid);

    /**
     * Returns the current number of endpoints.
     *
     * @return the current number of endpoints.
     */
    int size();

    /**
     * Removes all endpoints. Nothing is done on the endpoints themselves; they are just removed from this ClientEndpointManager.
     *
     * Can safely be called when there are no endpoints.
     */
    void clear();

    /**
     * Registers an endpoint with this ClientEndpointManager.
     *
     * If the endpoint already is registered, the call is ignored.
     *
     * @param endpoint the endpoint to register.
     * @throws java.lang.NullPointerException if endpoint is null.
     */
    void registerEndpoint(ClientEndpoint endpoint);

    /**
     * Removes an endpoint from this ClientEndpointManager. This method doesn't close the endpoint.
     *
     * todo: what happens when the endpoint already is removed
     * todo: what happens when the endpoint was never registered
     *
     * @param endpoint the endpoint to remove.
     * @throws java.lang.NullPointerException if endpoint is null.
     * @see #removeEndpoint(ClientEndpoint, boolean)
     */
    void removeEndpoint(ClientEndpoint endpoint);

    /**
     * Removes an endpoint and optionally closes it immediately.
     *
     * todo: what happens when the endpoint already is removed
     * todo: what happens when the endpoint was never registered
     *
     * @param endpoint the endpoint to remove.
     * @param closeImmediately if the endpoint is immediately closed.
     * @throws java.lang.NullPointerException if endpoint is null.
     * @see #removeEndpoint(ClientEndpoint)
     */
    void removeEndpoint(ClientEndpoint endpoint, boolean closeImmediately);
}
