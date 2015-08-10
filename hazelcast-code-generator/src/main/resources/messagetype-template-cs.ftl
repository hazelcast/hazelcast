namespace ${model.packageName}
{
    internal enum ${model.className}
    {

<#list model.params as param>
        ${model.name?cap_first}${param.name?cap_first} = ${param.id}<#if param_has_next>,</#if>
</#list>

    }

}


