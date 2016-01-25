/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
#ifndef HAZELCAST_CLIENT_PROTOCOL_CODEC_${model.className?upper_case}_H_
#define HAZELCAST_CLIENT_PROTOCOL_CODEC_${model.className?upper_case}_H_

#if  defined(WIN32) || defined(_WIN32) || defined(WIN64) || defined(_WIN64)
#pragma warning(push)
#pragma warning(disable: 4251) //for dll export
#endif

#include <memory>
<#if typeExists("vector")>
#include <vector>
</#if>
<#if typeExists("string")>
#include <string>
</#if>
<#if typeExists("map")>
#include <map>
</#if>

<#assign listenerInterface = "">
<#assign isAddListener=false>
<#assign isRemoveListener=false>
<#if model.className?contains("Listener") && model.className?contains("Add")>
<#assign listenerInterface = "IAddListenerCodec">
<#assign isAddListener=true>
</#if>
<#if model.className?contains("Listener") && model.className?contains("Remove") && model.className != "ClientRemoveAllListenersCodec">
<#assign listenerInterface = "IRemoveListenerCodec">
<#assign isRemoveListener=true>
</#if>
<#assign isAddRemoveListener=(isAddListener || isRemoveListener)>

#include "hazelcast/client/protocol/codec/${model.parentName}MessageType.h"
#include "hazelcast/util/HazelcastDll.h"
#include "hazelcast/client/impl/BaseEventHandler.h"
#include "hazelcast/client/protocol/ClientMessage.h"
<#if isAddListener>
#include "hazelcast/client/protocol/codec/IAddListenerCodec.h"
</#if>
<#if isRemoveListener>
#include "hazelcast/client/protocol/codec/IRemoveListenerCodec.h"
</#if>

<#if shouldIncludeHeader("Data", true)>
#include "hazelcast/client/serialization/pimpl/Data.h"
</#if>
<#if shouldIncludeHeader("Address", isAddRemoveListener)>
#include "hazelcast/client/Address.h"
</#if>
<#if shouldIncludeHeader("Member", isAddRemoveListener)>
#include "hazelcast/client/Member.h"
</#if>
<#if shouldIncludeHeader("MemberAttributeChange", isAddRemoveListener)>
#include "hazelcast/client/impl/MemberAttributeChange.h"
</#if>
<#if shouldIncludeHeader("EntryView", isAddRemoveListener)>
#include "hazelcast/client/map/DataEntryView.h"
</#if>
<#if shouldIncludeHeader("DistributedObjectInfo", isAddRemoveListener)>
#include "hazelcast/client/impl/DistributedObjectInfo.h"
</#if>

namespace hazelcast {
    namespace client {
<#if shouldForwardDeclare("Data", isAddRemoveListener)>
        namespace serialization {
            namespace pimpl {
                class Data;
            }
        }
</#if>
<#if shouldForwardDeclare("Address", isAddRemoveListener)>
        class Address;
</#if>
<#if shouldForwardDeclare("Member", isAddRemoveListener)>
        class Member;
</#if>
<#if shouldForwardDeclare("MemberAttributeChange", isAddRemoveListener)>
        namespace impl {
            class MemberAttributeChange;
        }
</#if>
<#if shouldForwardDeclare("EntryView", isAddRemoveListener)>
        namespace map {
            class EntryView;
        }
</#if>
<#if shouldForwardDeclare("DistributedObjectInfo", isAddRemoveListener)>
        namespace impl {
                class DistributedObjectInfo;
        }
</#if>

        namespace protocol {
            namespace codec {
                class HAZELCAST_API ${model.className} <#if isAddListener || isRemoveListener>: public ${listenerInterface}</#if>{
                public:
<#if isAddListener || isRemoveListener>                    virtual ~${model.className}();</#if>
                    //************************ REQUEST STARTS ******************************************************************//
                    class HAZELCAST_API RequestParameters {
                        public:
                            static const enum ${model.parentName}MessageType TYPE;
                            static const bool RETRYABLE;

                        static std::auto_ptr<ClientMessage> encode(<#list model.requestParams as param>
                                <#if util.isPrimitive(param.type)>${util.getCppType(param.type)} ${param.name}<#else>const ${util.getCppType(param.type)} <#if param.nullable >*<#else>&</#if>${param.name}</#if><#if param_has_next>, </#if></#list>);

                        static int32_t calculateDataSize(<#list model.requestParams as param>
                                <#if util.isPrimitive(param.type)>${util.getCppType(param.type)} ${param.name}<#else>const ${util.getCppType(param.type)} <#if param.nullable >*<#else>&</#if>${param.name}</#if><#if param_has_next>, </#if></#list>);

                        private:
                            // Preventing public access to constructors
                            RequestParameters();
                    };
                    //************************ REQUEST ENDS ********************************************************************//

                    //************************ RESPONSE STARTS *****************************************************************//
                    class HAZELCAST_API ResponseParameters {
                        public:
                            static const int TYPE;

                            <#list model.responseParams as param>
                            <#if !param.nullable >${util.getCppType(param.type)} ${param.name};
                            <#else>std::auto_ptr<${util.getCppType(param.type)} > ${param.name};
                            </#if>
                            </#list>

                            static ResponseParameters decode(ClientMessage &clientMessage);

                            // define copy constructor (needed for auto_ptr variables)
                            ResponseParameters(const ResponseParameters &rhs);
                        private:
                            ResponseParameters(ClientMessage &clientMessage);
                    };
                    //************************ RESPONSE ENDS *******************************************************************//
                    <#if model.events?has_content>

                    //************************ EVENTS START*********************************************************************//
                    class HAZELCAST_API AbstractEventHandler : public impl::BaseEventHandler {
                        public:
                            virtual ~AbstractEventHandler();

                            void handle(std::auto_ptr<protocol::ClientMessage> message);

                            <#list model.events as event>
                            virtual void handle${event.name?cap_first}(<#list event.eventParams as param><#if !param.nullable>const ${util.getCppType(param.type)} &<#else>std::auto_ptr<${util.getCppType(param.type)} > </#if>${param.name}<#if param_has_next>, </#if></#list>) = 0;

                            </#list>
                    };

                    //************************ EVENTS END **********************************************************************//
                    </#if>
                    <#if isAddListener || isRemoveListener>

                    ${model.className} (<#list model.requestParams as param>const ${util.getCppType(param.type)} &${param.name}<#if param_has_next>, </#if></#list>);

                    //************************ ${listenerInterface} interface starts *******************************************//
                    std::auto_ptr<ClientMessage> encodeRequest() const;

                    <#if isAddListener>
                    std::string decodeResponse(ClientMessage &responseMessage) const;

                    </#if>
                    <#if isRemoveListener>
                    bool decodeResponse(ClientMessage &clientMessage) const;

                    const std::string &getRegistrationId() const;

                    void setRegistrationId(const std::string &id);

                    </#if>
                    //************************ ${listenerInterface} interface ends *********************************************//
                    </#if>
                    private:
                        // Preventing public access to constructors
                        ${model.className} ();
                        <#if isAddListener || isRemoveListener>

                        <#list model.requestParams as param>
                        ${util.getCppType(param.type)} ${param.name}_;
                        </#list>
                        </#if>
                };
            }
        }
    }
}

#if  defined(WIN32) || defined(_WIN32) || defined(WIN64) || defined(_WIN64)
#pragma warning(pop)
#endif

#endif /* HAZELCAST_CLIENT_PROTOCOL_CODEC_${model.className?upper_case}_H_ */

<#--FUNCTIONS BELOW-->
<#function shouldIncludeHeader type isAddRemoveListener>
    <#list model.responseParams as param>
        <#if param.type?contains(util.getCppType(type)) && false == param.nullable>
             <#return true>
        </#if>
    </#list>
    <#if isAddRemoveListener>
        <#list model.requestParams as param>
            <#if param.type?contains(util.getCppType(type))>
                 <#return true>
            </#if>
        </#list>
    </#if>

    <#return false>
</#function>

<#function shouldForwardDeclare type isAddRemoveListener>
    <#if shouldIncludeHeader(type, isAddRemoveListener) >
        <#return false>
    </#if>

    <#return typeExists(type)>
</#function>

<#function typeExists type>
    <#list model.requestParams as param>
        <#if util.getCppType(param.type)?contains(type)>
             <#return true>
        </#if>
    </#list>
    <#list model.responseParams as param>
        <#if util.getCppType(param.type)?contains(type)>
             <#return true>
        </#if>
    </#list>
<#if model.events?has_content>
    <#list model.events as event>
        <#list event.eventParams as param>
        <#if util.getCppType(param.type)?contains(type)>
             <#return true>
        </#if>
        </#list>
    </#list>
</#if>

<#return false>
</#function>
