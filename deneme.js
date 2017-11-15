function deneme(par){
. test @{par}
}

var obj = require("./hazelcast/src/main/resources/protocol.json");
//console.log(obj)
//for(var i = 0; i < obj.length; i++){
//    console.log(obj[i].fullName)
//}
. @{deneme(3)}

//$.getJSON("hazelcast/src/main/resources/protocol.json", function(json) {
//    console.log(json);
//});