package com.hazelcast.map;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: ahmet
 * Date: 05.09.2013
 * Time: 14:36
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractSizeEstimator implements SizeEstimator{

    private final AtomicLong _size ;

    protected AbstractSizeEstimator(){
        _size =  new AtomicLong( 0 );
    }

    @Override
    public long getSize() {
        return _size.longValue();
    }

    public void add( long size )
    {
        _size.addAndGet(size);
    }


    public void reset()
    {
        _size.set( 0 );
    }

}
