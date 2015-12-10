#!/bin/sh
if [[ ( $1 == "--help") ||  ($1 == "-h") ]]; then 
   	echo "parameters : "
   	echo "	-o, --operation	    : Defines state operation of the script. Operation can be 'get' or 'change'."
        echo "	-s, --state 	    : Defines new state of the cluster. State can be 'active', 'frozen', 'passive'"
   	echo "	-p, --port  	    : Defines which port hazelcast is running. Default value is '5701'."
   	echo "	-g, --groupname     : Defines groupname of the cluster. Default value is 'dev'."
   	echo "	-P, --password      : Defines password of the cluster. Default value is 'dev-pass'."
   	exit 0
fi

while [[ $# > 1 ]]
do
key="$1"
case $key in
  	-o|--operation)
    OPERATION="$2"
    shift # past argument
    ;;
    -s|--state)
    STATE="$2"
    shift # past argument
    ;;
    -p|--port)
    PORT="$2"
    shift # past argument
    ;;
    -g|--groupname)
    GROUPNAME="$2"
    shift # past argument
    ;;
    -P|--password)
    PASSWORD="$2"
    shift # past argument
    ;;
    *)
esac
shift # past argument or value
done

if [[ -z "$OPERATION" ]]; then
 	echo "No operation is defined, running script with default operation : 'get'."
 	OPERATION="get"
fi

if [[ -z "$PORT" ]]; then
    echo "No port is defined, running script with default port : '5701'."
    PORT="5701"
fi

if [[ -z "$GROUPNAME" ]]; then
    echo "No groupname is defined, running script with default groupname : 'dev'."
    GROUPNAME="dev"
fi

if [[ -z "$PASSWORD" ]]; then
    echo "No password is defined, running script with default password : 'dev-pass'."
    PASSWORD="dev-pass"
fi

command -v curl >/dev/null 2>&1 || { echo >&2 "Cluster state script requires curl but it's not installed. Aborting."; exit -1; }

if [ "$OPERATION" != "get" ] && [ "$OPERATION" != "change" ] && [ "$OPERATION" != "shutdown" ]; then
    echo "Not a valid cluster operation, valid operations  are 'get' || 'change' || 'shutdown'"
    exit -1
fi

if [ $OPERATION = "get" ]; then
	echo "Getting cluster state from member on this machine on port ${PORT}"
	request="http://127.0.0.1:${PORT}/hazelcast/rest/management/cluster/state"
 	response=$(curl --data "${GROUPNAME}&${PASSWORD}" --silent "${request}");

 	if [[ $response == *"fail"* ]];then
        echo "An error occured while listing !";
    	exit -1
    fi
	if [[ $response == *"forbidden"* ]];then
        echo "Please make sure you provide valid user/pass.";
        exit -1
    fi
	if [[ $response == *"success"* ]];then
	    CURRSTATE=$(echo "${response}" | sed -e 's/^.*"state"[ ]*:[ ]*"//' -e 's/".*//');
	    echo "Cluster is in ${CURRSTATE} state."
    	exit 0
    fi
    echo "No hazelcast cluster is running."
    exit -1
fi

if [ $OPERATION = "change" ]; then

    if [[ -z "STATE" ]]; then
        echo "No new state is defined, Please define new state with --state 'active', 'frozen', 'passive' "
        exit -1
    fi

    if [ "$STATE" != "frozen" ] && [ "$STATE" != "active" ] && [ "$STATE" != "passive" ]; then
        echo "Not a valid cluster state, valid states  are 'active' || 'frozen' || 'passive'"
        exit -1
    fi

    echo "Changing cluster state to ${STATE} from member on this machine on port ${PORT}"
    request="http://127.0.0.1:${PORT}/hazelcast/rest/management/cluster/changeState"
    response=$(curl --data "${GROUPNAME}&${PASSWORD}&${STATE}" --silent "${request}");

    if [[ $response == *"fail"* ]];then
       NEWSTATE=$(echo "${response}" | sed -e 's/^.*"state"[ ]*:[ ]*"//' -e 's/".*//');
       if [ $NEWSTATE != "null"]; then
           echo "Cluster is already in ${STATE} state}"
       else
            echo "An error occured while changing cluster state!";
       fi
       exit -1
    fi

    if [[ $response == *"forbidden"* ]];then
        echo "Please make sure you provide valid user/pass.";
        exit -1
    fi

    if [[ $response == *"success"* ]];then
        NEWSTATE=$(echo "${response}" | sed -e 's/^.*"state"[ ]*:[ ]*"//' -e 's/".*//');
        STATUS=$(echo "${response}" | sed -e 's/^.*"state"[ ]*:[ ]*"//' -e 's/".*//');
        echo "State of the cluster changed to ${NEWSTATE} state"
        exit 0
    fi

    echo "No hazelcast cluster is running."
    exit -1
fi

if [ $OPERATION = "shutdown" ]; then

    echo "You are shutting down the cluster. Please make sure you provide valid user/pass."
    echo "Shutting down cluster from member on this machine on port ${PORT}"

    request="http://127.0.0.1:${PORT}/hazelcast/rest/management/cluster/shutdown"
    response=$(curl --data "${GROUPNAME}&${PASSWORD}" --silent "${request}");

    if [[ $response == *"fail"* ]];then
        echo "An error occured while shutting down cluster!";
        exit -1
    fi

    if [[ $response == *"forbidden"* ]];then
        echo "Wrong user/pass!";
        exit -1
    fi

    if [[ $response == *"success"* ]];then
        echo "Cluster shutdown completed!"
        exit 0
    fi

    echo "No hazelcast cluster is running."
    exit -1
fi
