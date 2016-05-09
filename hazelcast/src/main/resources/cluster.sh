#!/bin/sh

if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
   	echo "parameters : "
   	echo "	-o, --operation	    : Executes cluster-wide operation. Operation can be 'get-state','change-state','shutdown','force-start'."
    echo "	-s, --state 	    : Updates state of the cluster to new state. New state can be 'active', 'frozen', 'passive'"
    echo "	-a, --address  	    : Defines which ip address hazelcast is running. Default value is '127.0.0.1'."
   	echo "	-p, --port  	    : Defines which port hazelcast is running. Default value is '5701'."
   	echo "	-g, --groupname     : Defines groupname of the cluster. Default value is 'dev'."
   	echo "	-P, --password      : Defines password of the cluster. Default value is 'dev-pass'."
   	exit 0
fi

while [ $# -gt 1 ]
do
key="$1"
case "$key" in
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
     -a|--address)
    ADDRESS="$2"
    shift # past argument
    ;;
    *)
esac
shift # past argument or value
done

if [ -z "$OPERATION" ]; then
 	echo "No operation is defined, running script with default operation : 'get-state'."
 	OPERATION="get-state"
fi

if [ -z "$PORT" ]; then
    echo "No port is defined, running script with default port : '5701'."
    PORT="5701"
fi

if [ -z "$GROUPNAME" ]; then
    echo "No groupname is defined, running script with default groupname : 'dev'."
    GROUPNAME="dev"
fi

if [ -z "$PASSWORD" ]; then
    echo "No password is defined, running script with default password : 'dev-pass'."
    PASSWORD="dev-pass"
fi

if [ -z "$ADDRESS" ]; then
    echo "No specific ip address is defined, running script with default ip : '127.0.0.1."
    ADDRESS="127.0.0.1"
fi

command -v curl >/dev/null 2>&1 || { echo >&2 "Cluster state script requires curl but it's not installed. Aborting."; exit -1; }

if [ "$OPERATION" != "get-state" ] && [ "$OPERATION" != "change-state" ] && [ "$OPERATION" != "shutdown" ] &&  [ "$OPERATION" != "force-start" ]; then
    echo "Not a valid cluster operation, valid operations  are 'get-state' || 'change-state' || 'shutdown' || 'force-start'"
    exit 0
fi

if [ "$OPERATION" = "get-state" ]; then
    echo "Getting cluster state on ip ${ADDRESS} on port ${PORT}"
	request="http://${ADDRESS}:${PORT}/hazelcast/rest/management/cluster/state"
 	response=$(curl --data "${GROUPNAME}&${PASSWORD}" --silent "${request}");
    STATUS=$(echo "${response}" | sed -e 's/^.*"status"[ ]*:[ ]*"//' -e 's/".*//');
 	if [ "$STATUS" = "fail" ];then
        echo "An error occured while listing !";
    	exit 0
    fi
	if [ "$STATUS" = "forbidden" ];then
        echo "Please make sure you provide valid user/pass.";
        exit 0
    fi
	if [ "$STATUS" = "success" ];then
	    CURRSTATE=$(echo "${response}" | sed -e 's/^.*"state"[ ]*:[ ]*"//' -e 's/".*//');
	    echo "Cluster is in ${CURRSTATE} state."
    	exit 0
    fi
    echo "No hazelcast cluster is running on ip ${ADDRESS} on port ${PORT}."
    exit 0
fi

if [ "$OPERATION" = "change-state" ]; then

    if [ -z "$STATE" ]; then
        echo "No new state is defined, Please define new state with --state 'active', 'frozen', 'passive' "
        exit 0
    fi

    if [ "$STATE" != "frozen" ] && [ "$STATE" != "active" ] && [ "$STATE" != "passive" ]; then
        echo "Not a valid cluster state, valid states  are 'active' || 'frozen' || 'passive'"
        exit 0
    fi

    echo "Changing cluster state to ${STATE} on ip ${ADDRESS} on port ${PORT}"
    request="http://${ADDRESS}:${PORT}/hazelcast/rest/management/cluster/changeState"
    response=$(curl --data "${GROUPNAME}&${PASSWORD}&${STATE}" --silent "${request}");
    STATUS=$(echo "${response}" | sed -e 's/^.*"status"[ ]*:[ ]*"//' -e 's/".*//');

    if [ "$STATUS" = "fail" ];then
       NEWSTATE=$(echo "${response}" | sed -e 's/^.*"state"[ ]*:[ ]*"//' -e 's/".*//');
       if [ "$NEWSTATE" != "null" ]; then
           echo "Cluster is already in ${STATE} state}"
       else
            echo "An error occured while changing cluster state!";
       fi
       exit 0
    fi

    if [ "$STATUS" = "forbidden" ];then
        echo "Please make sure you provide valid user/pass.";
        exit 0
    fi

    if [ "$STATUS" = "success" ];then
        NEWSTATE=$(echo "${response}" | sed -e 's/^.*"state"[ ]*:[ ]*"//' -e 's/".*//');
        echo "State of the cluster changed to ${NEWSTATE} state"
        exit 0
    fi

    echo "No hazelcast cluster is running on ip ${ADDRESS} on port ${PORT}."
    exit 0
fi

if [ "$OPERATION" = "force-start" ]; then
    echo "Force-start makes cluster operational if cluster start is blocked by problematic members. Please make sure you provide valid user/pass."
    echo "Starting cluster from member on ip ${ADDRESS} on port ${PORT}"

    request="http://${ADDRESS}:${PORT}/hazelcast/rest/management/cluster/forceStart"
    response=$(curl --data "${GROUPNAME}&${PASSWORD}" --silent "${request}");
    STATUS=$(echo "${response}" | sed -e 's/^.*"status"[ ]*:[ ]*"//' -e 's/".*//');

    if [ "$STATUS" = "fail" ];then
       echo "An error occured while force starting the cluster!";
       exit 0
    fi

    if [ "$STATUS" = "forbidden" ];then
        echo "Please make sure you provide valid user/pass.";
        exit 0
    fi

    if [ "$STATUS" = "success" ];then
        echo "Cluster force-start completed!"
        exit 0
    fi

    echo "No hazelcast cluster is running on ip ${ADDRESS} on port ${PORT}."
    exit 0
fi

if [ "$OPERATION" = "shutdown" ]; then

    echo "You are shutting down the cluster. Please make sure you provide valid user/pass."
    echo "Shutting down from member on ip ${ADDRESS} on port ${PORT}"

    request="http://${ADDRESS}:${PORT}/hazelcast/rest/management/cluster/clusterShutdown"
    response=$(curl --data "${GROUPNAME}&${PASSWORD}" --silent "${request}");
    STATUS=$(echo "${response}" | sed -e 's/^.*"status"[ ]*:[ ]*"//' -e 's/".*//');

    if [ "$STATUS" = "fail" ];then
        echo "An error occured while shutting down cluster!";
        exit 0
    fi

    if [ "$STATUS" = "forbidden" ];then
        echo "Wrong user/pass!";
        exit 0
    fi

    if [ "$STATUS" = "success" ];then
        echo "Cluster shutdown completed!"
        exit 0
    fi

    echo "No hazelcast cluster is running on ip ${ADDRESS} on port ${PORT}."
    exit 0
fi
