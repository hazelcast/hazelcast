package com.hazelcast.dataset;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class TradeInfo implements DataSerializable {

    private long tradeId;

    // the person making the thread
    private long clientId;

    // venue is a particular trading platform, e.g. the NY stock exchange
    // venue code can be used to route on
    private int venueCode;

    // the code of the tradeable asset. E.g. gold
    private int instrumentCode;

    // the total price of the trade.
    private long price;

    // the number of items to trade
    private long quantity;
    private char side;

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(tradeId);
        out.writeLong(clientId);
        out.writeInt(venueCode);
        out.writeInt(instrumentCode);
        out.writeLong(price);
        out.writeLong(quantity);
        out.writeChar(side);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.tradeId = in.readLong();
        this.clientId = in.readLong();
        this.venueCode = in.readInt();
        this.instrumentCode = in.readInt();
        this.price = in.readLong();
        this.quantity = in.readLong();
        this.side = in.readChar();
    }
}
