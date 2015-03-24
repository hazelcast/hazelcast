package com.hazelcast.spi.persistence;

import com.hazelcast.spi.SharedService;

/**
 * @author mdogan 06/01/15
 */
public interface DBRepository extends SharedService {

    String SERVICE_NAME = "hz:spi:dbRepository";

    Database get(String name);

    Database getOrCreate(String name) throws DBException;

    void destroy();

}
