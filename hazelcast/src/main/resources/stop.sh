#!/bin/sh
PRG="$0"
PRGDIR=`dirname "$PRG"`
HAZELCAST_HOME=`cd "$PRGDIR/.." >/dev/null; pwd`
PID_FILE=$HAZELCAST_HOME/bin/hazelcast_instance.pid

if [[ ( $1 == "--help") ||  ($1 == "-h") ]]; then 
   	echo "parameters : "
   	echo "	-s, --scope 	: Defines scope of the script. Scope can be 'member' or 'cluster'."
   	echo "	-p, --port  	: Defines which port hazelcast is running. Default value is '5701'."
   	echo "	-g, --groupname : Defines groupname of the cluster. Default value is 'dev'."
   	echo "	-P, --password  : Defines password of the cluster. Default value is 'dev-pass'."
   	exit 0
fi 

while [[ $# > 1 ]]
do
key="$1"
case $key in
  	-s|--scope)
    SCOPE="$2"
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

if [[ -z "$SCOPE" ]]; then
 	echo "No scope is defined, running script with default scope : 'member'"
 	SCOPE="member"
fi
	
if [ $SCOPE = "cluster" ]; then

	command -v curl >/dev/null 2>&1 || { echo >&2 "Cluster shutdown requires curl but it's not installed. Aborting."; exit -1; }
	if [[ -z "$PORT" ]]; then
		echo "No port is defined, running script with default port : '5701'"
		PORT="5701"
	fi

	if [[ -z "$GROUPNAME" ]]; then
		echo "No groupname is defined, running script with default groupname : 'dev'"
		GROUPNAME="dev"
	fi

	if [[ -z "$PASSWORD" ]]; then
		echo "No password is defined, running script with default password : 'dev-pass'"
		PASSWORD="dev-pass"
	fi
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
else 

	if [ $SCOPE = "member" ]; then 
		if [ ! -f ${PID_FILE} ]; then
			echo "No hazelcast instance is running."
	  		exit -1
		fi
		PID=$(cat ${PID_FILE});
		if [[ -z "${PID}" ]]; then
		    echo "No hazelcast instance is running."
		    exit -1
		else
		   kill ${PID}
		   rm ${PID_FILE}
		   echo "Hazelcast Instance with PID ${PID} shutdown."
		   exit 0
		fi
	else
		echo "Unknown scope. Please define scope with --scope member || cluster"
	fi

fi
