package com.hazelcast.queue.proxy;

import com.hazelcast.core.IQueue;
import com.hazelcast.spi.ServiceProxy;

/**
 * Created with IntelliJ IDEA.
 * User: ali
 * Date: 11/14/12
 * Time: 12:51 AM
 * To change this template use File | Settings | File Templates.
 */
public interface QueueProxy<E> extends IQueue<E>, ServiceProxy {



}
