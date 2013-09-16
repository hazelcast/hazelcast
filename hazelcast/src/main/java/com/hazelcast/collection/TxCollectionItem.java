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

package com.hazelcast.collection;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * @ali 9/9/13
 */
public class TxCollectionItem extends CollectionItem {

    String transactionId;

    boolean removeOperation;


    public TxCollectionItem() {
    }

    public TxCollectionItem(CollectionItem item) {
        super(item.container, item.itemId, item.value);
    }

    public TxCollectionItem(CollectionContainer container, long itemId, Object value, String transactionId, boolean removeOperation) {
        super(container, itemId, value);
        this.transactionId = transactionId;
        this.removeOperation = removeOperation;
    }



    public String getTransactionId() {
        return transactionId;
    }

    public boolean isRemoveOperation() {
        return removeOperation;
    }

    public TxCollectionItem setTransactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public TxCollectionItem setRemoveOperation(boolean removeOperation) {
        this.removeOperation = removeOperation;
        return this;
    }

    public int getId() {
        return CollectionDataSerializerHook.TX_COLLECTION_ITEM;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(transactionId);
        out.writeBoolean(removeOperation);
    }

    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        transactionId = in.readUTF();
        removeOperation = in.readBoolean();
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TxCollectionItem)) return false;
        if (!super.equals(o)) return false;

        TxCollectionItem that = (TxCollectionItem) o;

        if (removeOperation != that.removeOperation) return false;
        if (!transactionId.equals(that.transactionId)) return false;

        return true;
    }

    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + transactionId.hashCode();
        result = 31 * result + (removeOperation ? 1 : 0);
        return result;
    }
}
