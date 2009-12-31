/**
 * 
 */
package com.hazelcast.impl;

import com.hazelcast.core.IList;
import com.hazelcast.core.ISet;

interface CollectionProxy extends IRemoveAwareProxy, ISet, IList {

}