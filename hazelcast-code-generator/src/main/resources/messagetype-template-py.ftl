
<#list model.params as param>
${model.name?upper_case}_${param.name?upper_case} = ${param.id}
</#list>
