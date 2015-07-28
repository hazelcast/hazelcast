#PROTOCOL DOC :

<#list model?keys as key>
<#assign map=model?values[key_index]?values/>
<#if map?has_content>
#${key}
<#list map as cm>
## **${cm.name}** operation id:${cm.id}

###Request Format

<#if cm.requestParams?has_content>
NAME | TYPE | Nullable
---- | ---- | --------
    <#list cm.requestParams as param>
${param.name} | ${param.type} | ${param.nullable?c}
    </#list>
<#else>
**EMPTY PARAMETERS**
</#if>

###Response Format

<#if cm.responseParams?has_content>
NAME | TYPE | Nullable
---- | ---- | --------
    <#list cm.responseParams as param>
${param.name} | ${param.type} | ${param.nullable?c}
    </#list>
<#else>
**EMPTY PARAMETERS**
</#if>

<#if cm.events?has_content>
###Event Response<#if cm.events?size gt 1 >s</#if>

    <#list cm.events as event>
####${event.name} Event Response Format

<#if cm.responseParams?has_content>
NAME | TYPE | Nullable
---- | ---- | --------
        <#list event.eventParams as param>
${param.name} | ${param.type} | ${param.nullable?c}
        </#list>
<#else>
**EMPTY PARAMETERS**
</#if>
    </#list>
</#if>

</#list>
</#if>
</#list>