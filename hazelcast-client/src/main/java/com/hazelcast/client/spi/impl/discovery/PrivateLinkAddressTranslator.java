/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl.discovery;

import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.nio.Address;
import com.hazelcast.logging.Logger;
import java.util.List;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class PrivateLinkAddressTranslator implements AddressTranslator {

    private final String[] privateLinkOrderedZonalNames;    

    public PrivateLinkAddressTranslator(List<String> privateLinkOrderedZonalNames) {
        this.privateLinkOrderedZonalNames = privateLinkOrderedZonalNames.toArray(new String[privateLinkOrderedZonalNames.size()]);
    }

    @Override
    public Address translate(Address address) {

        String IP = address.getHost();
        int len = IP.length(), idx=len, cnt=0; 
        char[] c = IP.toCharArray();
        String[] buffer = new String[2];
        for (int j=len-2;j>=0; j--){
            if (c[j]=='.'){
                buffer[cnt]=IP.substring(j+1, idx);
                idx = j; cnt++; j--;
            }
            if (cnt >1){break;}
        }
        String zonalName = privateLinkOrderedZonalNames[Integer.parseInt(buffer[1])];
        int port = Integer.parseInt(buffer[1]) * 256 + Integer.parseInt(buffer[0]);
        
        Address pvtlnAddr;
        try {
            pvtlnAddr = new Address(InetAddress.getByName(zonalName), port);
        } catch (UnknownHostException ignored) {
            Logger.getLogger(PrivateLinkAddressTranslator.class).severe("Address not available", ignored);
            return null;
        }        

        return pvtlnAddr;
    }

    @Override
    public void refresh() {
    }
}
