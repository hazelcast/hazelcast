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

package com.hazelcast.nio.protocol;

public enum Command {

    AUTH(), OK(), ERROR(), INSTANCES(), MEMBERS(), MEMBERLISTEN(), CLUSTERTIME(), PARTITIONS(), TRXCOMMIT(), TRXROLLBACK(), TRXBEGIN(),
    DESTROY(),UNKNOWN(), EVENT(),

    MGET(), MGETALL(), MPUT(), MTRYPUT(), MSET(), MPUTTRANSIENT(), MPUTANDUNLOCK(), MREMOVE(), MREMOVEITEM(),
    MCONTAINSKEY(), MCONTAINSVALUE(), ADDLISTENER(), MEVENT(), REMOVELISTENER(), KEYSET(), MENTRYSET(), MGETENTRY(),
    MLOCK(), MISLOCKED(), MUNLOCK(),MTRYLOCK(), MLOCKMAP(), MUNLOCKMAP(), MFORCEUNLOCK(), MPUTALL(), MPUTIFABSENT(),
    MREMOVEIFSAME(), MREPLACEIFNOTNULL(), MREPLACEIFSAME(),MTRYLOCKANDGET(), MTRYREMOVE(), MFLUSH(), MEVICT(),
    MLISTEN(), MREMOVELISTENER(), MSIZE(), MADDINDEX(), MISKEYLOCKED(),

    QOFFER(), QPUT(), QPOLL(), QTAKE(), QSIZE(), QPEEK(), QREMOVE(), QREMCAPACITY(), QENTRIES(), QLISTEN(),
    QREMOVELISTENER(), QEVENT(),

    CDLAWAIT(), CDLGETCOUNT(), CDLGETOWNER(), CDLSETCOUNT(), CDLCOUNTDOWN(),

    SEMATTACHDETACHPERMITS(), SEMCANCELACQUIRE(), SEMDESTROY(), SEM_DRAIN_PERMITS(), SEMGETATTACHEDPERMITS(),
    SEMGETAVAILPERMITS(), SEMREDUCEPERMITS(), SEMRELEASE(), SEMTRYACQUIRE(),

    LOCK(), TRYLOCK(), UNLOCK(), FORCEUNLOCK(), ISLOCKED(),

    SADD(), LADD(),
    MMPUT(), MMREMOVE(), MMVALUECOUNT(), MMSIZE(), MMCONTAINSENTRY(), MMCONTAINSKEY(), MMCONTAINSVALUE(), MMKEYS(),
    MMGET(), MMLOCK(), MMUNLOCK(), MMTRYLOCK(), MMADDLISTENER(), MMREMOVELISTENER(),
    ADDANDGET(), GETANDSET(), COMPAREANDSET(), GETANDADD(),
    NEWID(),
    TPUBLISH(), TLISTEN(), TREMOVELISTENER(),  MESSAGE();

    private final byte value;

    static byte idGen= 0;
    public final static int LENGTH = 200;


    private Command() {
        this.value = nextId();
    }

    public byte getValue() {
        return value;
    }

    private synchronized byte nextId() {
        return idGen++;
    }

}
