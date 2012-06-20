/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio.protocol;

public enum Command {

    
    AUTH, OK, ERROR, INSTANCES, MEMBERS, CLUSTERTIME, PARTITIONS,
    MGET, MGETALL, MPUT, MTRYPUT, MSET, MPUTTRANSIENT, MPUTANDUNLOCK, MREMOVE, MREMOVEITEM,
    MCONTAINSKEY, MCONTAINSVALUE, ADDLISTENER, EVENT, REMOVELISTENER, KEYSET, ENTRYSET, MGETENTRY, MLOCK, MISKEYLOCKED, MUNLOCK,
    MLOCKMAP, MUNLOCKMAP, MFORCEUNLOCK, MPUTALL, MPUTIFABSENT, MREMOVEIFSAME, MREPLACEIFNOTNULL, MREPLACEIFSAME,
    MTRYLOCKANDGET, MFLUSH, MEVICT, MADDLISTENER, MREMOVELISTENER, TRXCOMMIT, TRXROLLBACK, TRXBEGIN,
    QOFFER, QPOLL, QSIZE, QPEEK, QREMOVE, QREMCAPACITY, QENTRIES, QADDLISTENER, QREMOVELISTENER,
    CDLAWAIT, CDLGETCOUNT, CDLGETOWNER, CDLSETCOUNT, CDLCOUNTDOWN,
    SEMATTACHDETACHPERMITS, SEMCANCELACQUIRE, SEMDESTROY, SEM_DRAIN_PERMITS, SEMGETATTACHEDPERMITS,
    SEMGETAVAILPERMITS, SEMREDUCEPERMITS, SEMRELEASE, SEMTRYACQUIRE,
    LOCK_LOCK, LOCK_TRYLOCK, LOCK_UNLOCK, LOCK_FORCE_UNLOCK, LOCK_IS_LOCKED,
    SADD, LADD, MMPUT, MMREMOVE, MMVALUECOUNT, ADDANDGET, GETANDSET, COMPAREANDSET, GETANDADD, NEWID;
    public final String value;

    private Command() {
        this.value = this.toString();
    }

}
