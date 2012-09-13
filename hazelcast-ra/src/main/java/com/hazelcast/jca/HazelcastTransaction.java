package com.hazelcast.jca;


/**
 * Wrapper interface to bundle {@link javax.resource.cci.LocalTransaction} and 
 * {@link javax.resource.spi.LocalTransaction} into one interface
 */
public interface HazelcastTransaction extends javax.resource.cci.LocalTransaction, javax.resource.spi.LocalTransaction {

}
