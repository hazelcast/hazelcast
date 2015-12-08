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

<#if isAddRemoveListener>#include "hazelcast/util/Util.h"
#include "hazelcast/util/ILogger.h"</#if>

#include "hazelcast/client/protocol/codec/${model.className}.h"
#include "hazelcast/client/exception/UnexpectedMessageTypeException.h"
<#if shouldIncludeHeader("Data", isAddRemoveListener)>
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
<#if model.events?has_content>
#include "hazelcast/client/protocol/EventMessageConst.h"
</#if>

namespace hazelcast {
    namespace client {
        namespace protocol {
            namespace codec {
                const ${model.parentName}MessageType ${model.className}::RequestParameters::TYPE = HZ_${model.parentName?upper_case}_${model.name?upper_case};
                const bool ${model.className}::RequestParameters::RETRYABLE = <#if model.retryable == 1>true<#else>false</#if>;
                const int32_t ${model.className}::ResponseParameters::TYPE = ${model.response};
                <#if isAddListener || isRemoveListener>

                ${model.className}::~${model.className}() {
                }

                </#if>
                std::auto_ptr<ClientMessage> ${model.className}::RequestParameters::encode(<#list model.requestParams as param>
                        <#if util.isPrimitive(param.type)>${util.getCppType(param.type)} ${param.name}<#else>const ${util.getCppType(param.type)} <#if param.nullable >*<#else>&</#if>${param.name}</#if><#if param_has_next>, </#if></#list>) {
                    int32_t requiredDataSize = calculateDataSize(<#list model.requestParams as param>${param.name}<#if param_has_next>, </#if></#list>);
                    std::auto_ptr<ClientMessage> clientMessage = ClientMessage::createForEncode(requiredDataSize);
                    clientMessage->setMessageType((uint16_t)${model.className}::RequestParameters::TYPE);
                    clientMessage->setRetryable(RETRYABLE);
                    <#list model.requestParams as p>
                        <@setterText varName=p.name type=p.type isNullable=p.nullable/>
                    </#list>
                    clientMessage->updateFrameLength();
                    return clientMessage;
                }

                int32_t ${model.className}::RequestParameters::calculateDataSize(<#list model.requestParams as param>
                        <#if util.isPrimitive(param.type)>${util.getCppType(param.type)} ${param.name}<#else>const ${util.getCppType(param.type)} <#if param.nullable >*<#else>&</#if>${param.name}</#if><#if param_has_next>, </#if></#list>) {
                    int32_t dataSize = ClientMessage::HEADER_SIZE;
                    <#list model.requestParams as p>
                        <@sizeText varName=p.name type=p.type isNullable=p.nullable/>
                    </#list>
                    return dataSize;
                }

                ${model.className}::ResponseParameters::ResponseParameters(ClientMessage &clientMessage) {
                    if (TYPE != clientMessage.getMessageType()) {
                        throw exception::UnexpectedMessageTypeException("${model.className}::ResponseParameters::decode", clientMessage.getMessageType(), TYPE);
                    }
    <#if model.responseParams?has_content><#list model.responseParams as p><@getterText varName=p.name type=p.type isNullable=p.nullable/></#list></#if>
                }

                ${model.className}::ResponseParameters ${model.className}::ResponseParameters::decode(ClientMessage &clientMessage) {
                    return ${model.className}::ResponseParameters(clientMessage);
                }

                ${model.className}::ResponseParameters::ResponseParameters(const ${model.className}::ResponseParameters &rhs) {
                    <#list model.responseParams as param>
                    <#if param.nullable >
                        ${param.name} = std::auto_ptr<${util.getCppType(param.type)}>(new ${util.getCppType(param.type)}(*rhs.${param.name}));
                    <#else>
                        ${param.name} = rhs.${param.name};
                    </#if>
                    </#list>
                }
                <#if model.events?has_content>

                //************************ EVENTS START*************************************************************************//
                ${model.className}::AbstractEventHandler::~AbstractEventHandler() {
                }

                void ${model.className}::AbstractEventHandler::handle(std::auto_ptr<protocol::ClientMessage> clientMessage) {
                    int messageType = clientMessage->getMessageType();
                    switch (messageType) {
                    <#list model.events as event>
                        case protocol::EVENT_${event.name?upper_case}:
                        {
                        <#list event.eventParams as p>
                            <@eventGetterText varName=p.name type=p.type isNullable=p.nullable isEvent=true/>
                        </#list>
                            handle${event.name?cap_first}(<#list event.eventParams as param>${param.name}<#if param_has_next>, </#if></#list>);
                            break;
                        }
                        </#list>
                        default:
                            char buf[300];
                            util::snprintf(buf, 300, "[${model.className}::AbstractEventHandler::handle] Unknown message type (%d) received on event handler.", clientMessage->getMessageType());
                            util::ILogger::getLogger().warning(buf);
                    }
                }
                </#if>
                //************************ EVENTS END **************************************************************************//
                <#if isAddListener || isRemoveListener>

                ${model.className}::${model.className} (<#list model.requestParams as param>const ${util.getCppType(param.type)} &${param.name}<#if param_has_next>, </#if></#list>)
<#if model.requestParams?has_content>                        : <#list model.requestParams as param>${param.name}_(${param.name})<#if param_has_next>, </#if></#list> </#if>{
                }

                //************************ ${listenerInterface} interface start ************************************************//
                std::auto_ptr<ClientMessage> ${model.className}::encodeRequest() const {
                    return RequestParameters::encode(<#list model.requestParams as param>${param.name}_<#if param_has_next>, </#if></#list>);
                }

                <#if isAddListener>
                std::string ${model.className}::decodeResponse(ClientMessage &responseMessage) const {
                    return ResponseParameters::decode(responseMessage).response;
                }
                </#if>
                <#if isRemoveListener>
                const std::string &${model.className}::getRegistrationId() const {
                    return registrationId_;
                }

                void ${model.className}::setRegistrationId(const std::string &id) {
                    registrationId_ = id;
                }

                bool ${model.className}::decodeResponse(ClientMessage &responseMessage) const {
                    return ResponseParameters::decode(responseMessage).response;
                }
                </#if>
                //************************ ${listenerInterface} interface ends *************************************************//
                </#if>

            }
        }
    }
}

<#--MACROS BELOW-->
<#--SIZE NULL CHECK MACRO -->
<#macro sizeText varName type isNullable=false>
<#local cat= util.getTypeCategory(type)>
<#switch cat>
            <#case "OTHER">
            <#case "CUSTOM">
                    dataSize += ClientMessage::calculateDataSize(${varName});
                <#break >
            <#case "COLLECTION">
            <#local itemVariableType= util.getGenericType(type)>
                    dataSize += ClientMessage::calculateDataSize<${util.getCppType(itemVariableType)} >(${varName});
                <#break >
            <#case "ARRAY">
                <#local itemVariableType= util.getGenericType(type)>
                    dataSize += ClientMessage::calculateDataSize<${util.getCppType(itemVariableType)} >(${varName});
                <#break >
            <#case "MAP">
                <#local keyType = util.getFirstGenericParameterType(type)>
                <#local valueType = util.getSecondGenericParameterType(type)>
                    dataSize += ClientMessage::calculateDataSize<${util.getCppType(keyType)}, ${util.getCppType(valueType)} > (${varName});
</#switch>
</#macro>

<#--SETTER NULL CHECK MACRO -->
<#macro setterText varName type isNullable=false>
<#local cat= util.getTypeCategory(type)>
<#switch cat>
                <#case "OTHER">
                <#case "CUSTOM">
                    clientMessage->set(${varName});
                    <#break >
                <#case "COLLECTION">
                <#local itemVariableType= util.getGenericType(type)>
                    clientMessage->setArray<${util.getCppType(itemVariableType)} >(${varName});
                    <#break >
                <#case "ARRAY">
                <#local itemVariableType= util.getArrayType(type)>
                    clientMessage->setArray<${util.getCppType(itemVariableType)} > (${varName});
                    <#break >
                <#case "MAP">
                    <#local keyType = util.getFirstGenericParameterType(type)>
                    <#local valueType = util.getSecondGenericParameterType(type)>
                    clientMessage->setEntryArray<${util.getCppType(keyType)}, ${util.getCppType(valueType)} >(${varName});
                    <#break >
</#switch>
</#macro>

<#--GETTER NULL CHECK MACRO -->
<#macro getterText varName type isNullable=false isEvent=false>
<#if isNullable>
    <#assign nullableStr = "Nullable">
<#else>
    <#assign nullableStr = "">
</#if>

<#local cat= util.getTypeCategory(type)>
<#switch cat>
                <#case "OTHER">
                <#case "CUSTOM">
                    ${varName} = clientMessage.get${nullableStr}<${util.getCppType(type)} >();
                    <#break >
                <#case "COLLECTION">
                <#local itemVariableType= util.getGenericType(type)>
                    ${varName} = clientMessage.get${nullableStr}Array<${util.getCppType(itemVariableType)} >();
                    <#break >
                <#case "ARRAY">
                <#local itemVariableType= util.getArrayType(type)>
                    ${varName} = clientMessage.get${nullableStr}Array<${util.getCppType(itemVariableType)} >();
                    <#break >
                <#case "MAP">
                    <#local keyType = util.getFirstGenericParameterType(type)>
                    <#local valueType = util.getSecondGenericParameterType(type)>
                    ${varName} = clientMessage.get${nullableStr}EntryArray<${util.getCppType(keyType)}, ${util.getCppType(valueType)} >();
                    <#break >
</#switch>
</#macro>

<#--Event getter MACRO -->
<#macro eventGetterText varName type isNullable=false isEvent=false>
<#if isNullable>
    <#assign nullableStr = "Nullable">
<#else>
    <#assign nullableStr = "">
</#if>
<#local cat= util.getTypeCategory(type)>
<#switch cat>
                        <#case "OTHER">
                        <#case "CUSTOM">
                            <#if !isNullable >${util.getCppType(type)} ${varName} = clientMessage->get${nullableStr}<${util.getCppType(type)} >();
                            <#else>std::auto_ptr<${util.getCppType(type)} > ${varName} = clientMessage->get${nullableStr}<${util.getCppType(type)} >();
                            </#if>
                            <#break >
                        <#case "COLLECTION">
                        <#local itemVariableType= util.getGenericType(type)>
                            <#if !isNullable >${util.getCppType(type)} ${varName} = clientMessage->get${nullableStr}Array<${util.getCppType(itemVariableType)} >();
                            <#else>std::auto_ptr<${util.getCppType(type)} > ${varName} = clientMessage->get${nullableStr}Array<${util.getCppType(itemVariableType)} >();
                            </#if>
                            <#break >
                        <#case "ARRAY">
                        <#local itemVariableType= util.getArrayType(type)>
                            <#if !isNullable >${util.getCppType(type)} ${varName} = clientMessage->get${nullableStr}Array<${util.getCppType(itemVariableType)} >();
                            <#else>std::auto_ptr<${util.getCppType(type)} > ${varName} = clientMessage->get${nullableStr}Array<${util.getCppType(itemVariableType)} >();
                            </#if>
                            <#break >
                        <#case "MAP">
                            <#local keyType = util.getFirstGenericParameterType(type)>
                            <#local valueType = util.getSecondGenericParameterType(type)>
                            <#if !isNullable >${util.getCppType(type)} ${varName} = clientMessage->get${nullableStr}EntryArray<${util.getCppType(keyType)}, ${util.getCppType(valueType)} >();
                            <#else>std::auto_ptr<${util.getCppType(type)} > ${varName} = clientMessage->get${nullableStr}EntryArray<${util.getCppType(keyType)}, ${util.getCppType(valueType)} >();
                            </#if>
                            <#break >
</#switch>

</#macro>

<#function isHeaderAlreadyIncluded type isAddRemoveListener>
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

<#--FUNCTIONS BELOW-->
<#function shouldIncludeHeader type isAddRemoveListener >
    <#if isHeaderAlreadyIncluded(type, isAddRemoveListener)>
        <#return false>
    </#if>

    <#list model.requestParams as param>
        <#if param.type?contains(util.getCppType(type))>
             <#return true>
        </#if>
    </#list>
    <#list model.responseParams as param>
        <#if param.type?contains(util.getCppType(type))>
             <#return true>
        </#if>
    </#list>
<#if model.events?has_content>
    <#list model.events as event>
        <#list event.eventParams as param>
            <#if param.type?contains(util.getCppType(type))>
                 <#return true>
            </#if>
        </#list>
    </#list>
</#if>

<#return false>
</#function>
