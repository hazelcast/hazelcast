#!/bin/bash

if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "parameters: "
    echo "  -o, --operation   : Executes cluster-wide operation. Operation can be 'get-state','change-state','shutdown','force-start','partial-start','get-cluster-version','change-cluster-version'."
    echo "  -s, --state       : Updates state of the cluster to new state. New state can be 'active', 'frozen', 'passive', 'no_migration'."
    echo "  -a, --address     : Defines which ip address hazelcast is running. Default value is '127.0.0.1'."
    echo "  -p, --port        : Defines which port hazelcast is running. Default value is '5701'."
    echo "  -c, --clustername : Defines clustername of the cluster. Default value is 'dev'."
    echo "  -P, --password    : Defines password of the cluster. Default value is 'dev-pass'."
    echo "  -v, --version     : Defines the cluster version to change to. To be used in conjunction with '-o change-cluster-version'."
    echo "  -d, --debug       : Prints error output."
    echo
    echo "HTTPs related (when TLS is enabled):"
    echo "      --https       : Uses HTTPs protocol for REST calls. (no parameter value expected)"
    echo "      --cacert      : Defines trusted PEM-encoded certificate file path. It's used to verify member certificates."
    echo "      --cert        : Defines PEM-encoded client certificate file path. Only needed when client certificate authentication is used."
    echo "      --key         : Defines PEM-encoded client private key file path. Only needed when client certificate authentication is used."
    echo "      --insecure    : Disables member certificate verification. (no parameter value expected)"
    exit 0
fi

URL_SCHEME="http"
CURL_ARGS="--silent"

while [ $# -ge 1 ]
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
    -c|--clustername)
    CLUSTERNAME="$2"
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
    -v|--version)
    CLUSTER_VERSION="$2"
    shift # past argument
    ;;
    -d|--debug)
    CURL_ARGS="$CURL_ARGS --show-error"
    ;;
    --https)
    URL_SCHEME="https"
    ;;
    --cert|--key)
    CURL_ARGS="$CURL_ARGS $1 $2"
    shift # past argument
    ;;
    --cacert)
    CURL_ARGS="$CURL_ARGS --capath /dev/null $1 $2"
    shift # past argument
    ;;
    --insecure)
    echo "WARNING: You're using the insecure switch. Hazelcast member TLS certificates will not be verified!" >&2
    CURL_ARGS="$CURL_ARGS $1"
    ;;
    *)
esac
shift # past argument or value
done

if [ -z "$OPERATION" ]; then
    echo "No operation is defined, running script with default operation: 'get-state'."
    OPERATION="get-state"
fi

if [ -z "$PORT" ]; then
    echo "No port is defined, running script with default port: '5701'."
    PORT="5701"
fi

if [ -z "$CLUSTERNAME" ]; then
    echo "No clustername is defined, running script with default clustername: 'dev'."
    CLUSTERNAME="dev"
fi

if [ -z "$ADDRESS" ]; then
    echo "No specific ip address is defined, running script with default ip: '127.0.0.1'."
    ADDRESS="127.0.0.1"
fi

command -v curl >/dev/null 2>&1 || { echo >&2 "Cluster state script requires curl but it's not installed. Aborting."; exit -1; }

URL_BASE="${URL_SCHEME}://${ADDRESS}:${PORT}/hazelcast/rest/management/cluster"
CURL_CMD="curl $CURL_ARGS"

if [ "$OPERATION" != "get-state" ] && [ "$OPERATION" != "change-state" ] && [ "$OPERATION" != "shutdown" ] &&  [ "$OPERATION" != "force-start" ] && [ "$OPERATION" != "partial-start" ] && [ "$OPERATION" != "get-cluster-version" ] && [ "$OPERATION" != "change-cluster-version" ]; then
    echo "Not a valid cluster operation, valid operations  are 'get-state' || 'change-state' || 'shutdown' || 'force-start' || 'partial-start' || 'get-cluster-version' || 'change-cluster-version'"
    exit 2
fi

if [ "$OPERATION" = "get-state" ]; then
    echo "Getting cluster state on address ${ADDRESS}:${PORT}"
    response=$(${CURL_CMD} --data "${CLUSTERNAME}&${PASSWORD}" "${URL_BASE}/state")
    CURL_EXIT_CODE=$?
    STATUS=$(echo "${response}" | sed -e 's/^.*"status"[ ]*:[ ]*"//' -e 's/".*//')

    if [ "$STATUS" = "success" ];then
        CURRSTATE=$(echo "${response}" | sed -e 's/^.*"state"[ ]*:[ ]*"//' -e 's/".*//')
        echo "Cluster is in ${CURRSTATE} state."
        exit 0
    fi

elif [ "$OPERATION" = "change-state" ]; then

    if [ -z "$STATE" ]; then
        echo "No new state is defined. Please define new state with --state 'active', 'no_migration, 'frozen', 'passive'"
        exit 2
    fi

    if [ "$STATE" != "frozen" ] && [ "$STATE" != "active" ] && [ "$STATE" != "no_migration" ] && [ "$STATE" != "passive" ]; then
        echo "Not a valid cluster state, valid states  are 'active' || 'frozen' || 'passive' || 'no_migration'"
        exit 2
    fi

    echo "Changing cluster state to ${STATE} on address ${ADDRESS}:${PORT}"
    response=$(${CURL_CMD} --data "${CLUSTERNAME}&${PASSWORD}&${STATE}" "${URL_BASE}/changeState")
    CURL_EXIT_CODE=$?
    STATUS=$(echo "${response}" | sed -e 's/^.*"status"[ ]*:[ ]*"//' -e 's/".*//')

    if [ "$STATUS" = "fail" ];then
       NEWSTATE=$(echo "${response}" | sed -e 's/^.*"state"[ ]*:[ ]*"//' -e 's/".*//')
       if [ "$NEWSTATE" != "null" ]; then
           echo "Cluster is already in ${STATE} state}"
       else
            echo "An error occured while changing cluster state!";
       fi
       exit 3
    fi

    if [ "$STATUS" = "success" ];then
        NEWSTATE=$(echo "${response}" | sed -e 's/^.*"state"[ ]*:[ ]*"//' -e 's/".*//')
        echo "State of the cluster changed to ${NEWSTATE} state"
        exit 0
    fi

elif [ "$OPERATION" = "force-start" ]; then

    echo "Force-start makes cluster operational if cluster start is blocked by problematic members."
    echo "Starting cluster from member on address ${ADDRESS}:${PORT}"

    response=$(${CURL_CMD} --data "${CLUSTERNAME}&${PASSWORD}" "${URL_BASE}/forceStart")
    CURL_EXIT_CODE=$?
    STATUS=$(echo "${response}" | sed -e 's/^.*"status"[ ]*:[ ]*"//' -e 's/".*//')

    if [ "$STATUS" = "success" ];then
        echo "Cluster force-start completed!"
        exit 0
    fi

elif [ "$OPERATION" = "partial-start" ]; then

    echo "Partial-start makes cluster operational by starting only a portion of available members if cluster start is blocked by problematic members."
    echo "Starting cluster from member on address ${ADDRESS}:${PORT}"

    response=$(${CURL_CMD} --data "${CLUSTERNAME}&${PASSWORD}" "${URL_BASE}/partialStart")
    CURL_EXIT_CODE=$?
    STATUS=$(echo "${response}" | sed -e 's/^.*"status"[ ]*:[ ]*"//' -e 's/".*//')

    if [ "$STATUS" = "success" ];then
        echo "Cluster partial-start completed!"
        exit 0
    fi

elif [ "$OPERATION" = "shutdown" ]; then

    echo "You are shutting down the cluster."
    echo "Shutting down from member on address ${ADDRESS}:${PORT}"

    response=$(${CURL_CMD} --data "${CLUSTERNAME}&${PASSWORD}" "${URL_BASE}/clusterShutdown")
    CURL_EXIT_CODE=$?
    STATUS=$(echo "${response}" | sed -e 's/^.*"status"[ ]*:[ ]*"//' -e 's/".*//')

    if [ "$STATUS" = "success" ];then
        echo "Cluster shutdown completed!"
        exit 0
    fi

elif [ "$OPERATION" = "get-cluster-version" ]; then

    echo "Getting cluster version on address ${ADDRESS}:${PORT}"
    response=$(${CURL_CMD} "${URL_BASE}/version")
    CURL_EXIT_CODE=$?
    STATUS=$(echo "${response}" | sed -e 's/^.*"status"[ ]*:[ ]*"//' -e 's/".*//')

    if [ "$STATUS" = "success" ];then
        CURRVERSION=$(echo "${response}" | sed -e 's/^.*"version"[ ]*:[ ]*"//' -e 's/".*//')
        echo "Cluster operates in version ${CURRVERSION}."
        exit 0
    fi

elif [ "$OPERATION" = "change-cluster-version" ]; then

    if [ -z "$CLUSTER_VERSION" ]; then
        echo "No new cluster version is defined. Please define new cluster version with --version MAJOR.MINOR (for example --version 3.8)"
        exit 2
    fi

    echo "Changing cluster version to ${CLUSTER_VERSION} on address ${ADDRESS}:${PORT}"
    response=$(${CURL_CMD} --data "${CLUSTERNAME}&${PASSWORD}&${CLUSTER_VERSION}" "${URL_BASE}/version")
    CURL_EXIT_CODE=$?
    STATUS=$(echo "${response}" | sed -e 's/^.*"status"[ ]*:[ ]*"//' -e 's/".*//')

    if [ "$STATUS" = "fail" ]; then
       MESSAGE=$(echo "${response}" | sed -e 's/^.*"message"[ ]*:[ ]*"//' -e 's/".*//')
       # when there is no "message" element in response, above sed process returns "{"
       if [ "$MESSAGE" != "{" ]; then
           echo "Cluster version change failed: ${MESSAGE}"
       else
            echo "An error occured while changing cluster version!";
       fi
       exit 3
    fi

    if [ "$STATUS" = "success" ]; then
        NEWVERSION=$(echo "${response}" | sed -e 's/^.*"version"[ ]*:[ ]*"//' -e 's/".*//')
        echo "Cluster version changed to ${NEWVERSION}."
        exit 0
    fi

fi

if [ "$STATUS" = "fail" ];then
   echo "An error occurred while performing HTTP REST operation on the Hazelcast cluster!";
   exit 3
fi

if [ "$STATUS" = "forbidden" ];then
    echo "Please make sure you provide valid group name (or valid username/password combination when the Hazelcast security is enabled).";
    exit 4
fi

if [ $CURL_EXIT_CODE -ne 0 ]; then
    echo "HTTP REST call on address ${ADDRESS}:${PORT} failed. "
    exit $CURL_EXIT_CODE
fi