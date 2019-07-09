#!/bin/bash

if [[ "$1" = "--help" ]] || [[ "$1" = "-h" ]]; then
    echo "Sends CP subsystem management operations to a Hazelcast instance."
    echo "Parameters:"
    echo "  -o, --operation   : Operation to be called."
    echo "  -c, --group       : Name of the CP group. Must be provided for 'get-group', 'force-destroy-group', 'get-sessions', 'force-close-session'."
    echo "  -m, --member      : UUID of the CP member. Must be provided for 'remove-member'."
    echo "  -s, --session-id  : CP Session ID. Must be provided for 'force-close-session'."
    echo "  -a, --address     : Defines which ip address hazelcast is running. Default value is '127.0.0.1'."
    echo "  -p, --port        : Defines which port hazelcast is running. Default value is '5701'."
    echo "  -g, --groupname   : Defines groupname of the cluster. Default value is 'dev'."
    echo "  -P, --password    : Defines password of the cluster. Default value is 'dev-pass'."
    echo "  -d, --debug       : Prints curl error output."
    echo "HTTPs related (TLS enabled):"
    echo "      --https       : Uses HTTPs protocol for REST calls. (no parameter value expected)"
    echo "      --cacert      : Defines trusted PEM-encoded certificate file path. It's used to verify member certificates."
    echo "      --cert        : Defines PEM-encoded client certificate file path. Only needed when client certificate authentication is used."
    echo "      --key         : Defines PEM-encoded client private key file path. Only needed when client certificate authentication is used."
    echo "      --insecure    : Disables member certificate verification. (no parameter value expected)"
    echo "Operations:"
    echo "  - 'get-local-member' returns the local CP member information from the accessed Hazelcast member."
    echo "  - 'get-groups' returns the list of active CP groups."
    echo "  - 'get-group' expects a CP group name (-c or --group) and returns a its details."
    echo "  - 'force-destroy-group' destroys a CP group (-c or --group) non-gracefully."
    echo "     It must be called only when a CP group loses its majority."
    echo "     All CP data structure proxies created before the force-destroy step will fail."
    echo "     If you create a new proxy for a CP data structure that is mapped to the destroyed CP group, the CP group will be initialized from scratch."
    echo "     Please note that you cannot force-destroy the METADATA CP group. If you lose majority of the METADATA CP group, you have to restart the CP subsystem."
    echo "  - 'get-members' returns the list of active CP members in the cluster."
    echo "     Please note that even if a CP member has left the cluster, it is not automatically removed from the active CP member list immediately."
    echo "  - 'remove-member' removes the given CP member (-m) from the active CP member list."
    echo "     The removed member will be removed from the CP groups as well."
    echo "     Before removing a CP member, please make sure that the missing member is actually crashed, not partitioned away."
    echo "  - 'promote-member' promotes the contacted Hazelcast member to the CP member role."
    echo "  - 'get-sessions' returns the list of CP sessions created in the requested CP group (-c)."
    echo "  - 'force-close-session' closes the given CP session (-s) on the given CP group (-c)."
    echo "     Once the CP session is closed, all CP resources (locks, semaphore permits, etc.) will be released."
    echo "     Before force-closing a CP session, please make sure that owner endpoint of the CP session is crashed and will not show up."
    echo "  - 'restart' wipes out all CP subsystem state and restarts it from scratch."
    echo "     Please call this API only on the Hazelcast master member (i.e., the first member in the Hazelcast cluster member list)."
    echo "     Please make sure that you call this API only once. Once you make the call, please observe the cluster to see if the CP subsystem initialization is successful."
    echo "If you query a non-existing CP group or a CP session, the call fails with 'Not found'."
    echo "If you trigger an operation with invalid credentials, the call fails with 'Invalid credentials'."
    echo "If you trigger an operation with invalid parameters, for instance destroying a non-existing CP group or removing a non-existing CP member, the call fails with 'Bad request'."
    exit 0
fi

INVALID_ARGUMENT_RETURN_VALUE=1
MISSING_ARGUMENT_RETURN_VALUE=2
INVALID_CREDENTIALS_RETURN_VALUE=3
NOT_FOUND_RETURN_VALUE=4
BAD_REQUEST_RETURN_VALUE=5
INTERNAL_ERROR_RETURN_VALUE=6

URL_SCHEME="http"
CURL_ARGS="--silent -w \n%{http_code}"

while [[ $# -ge 1 ]]
do
key="$1"
case "$key" in
    -o|--operation)
    OPERATION="$2"
    shift # past argument
    ;;
    -c|--group)
    CP_GROUP_NAME="$2"
    shift # past argument
    ;;
    -m|--member)
    CP_MEMBER_UID="$2"
    shift # past argument
    ;;
    -s|--session-id)
    CP_SESSION_ID="$2"
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


if [[ -z "$OPERATION" ]]; then
    echo "No operation is defined, running script with default operation: 'get-local-member'."
    OPERATION="get-local-member"
fi


if [[ -z "$PORT" ]]; then
    echo "No port is defined, running script with default port: '5701'."
    PORT="5701"
fi

if [[ -z "$GROUPNAME" ]]; then
    echo "No groupname is defined, running script with default groupname: 'dev'."
    GROUPNAME="dev"
fi

if [[ -z "$PASSWORD" ]]; then
    echo "No password is defined, running script with default password: 'dev-pass'."
    PASSWORD="dev-pass"
fi

if [[ -z "$ADDRESS" ]]; then
    echo "No specific ip address is defined, running script with default ip: '127.0.0.1'."
    ADDRESS="127.0.0.1"
fi

command -v curl >/dev/null 2>&1 || { echo >&2 "Cluster state script requires curl but it's not installed. Aborting."; exit -1; }

URL_BASE="${URL_SCHEME}://${ADDRESS}:${PORT}/hazelcast/rest/cp-subsystem"
CURL_CMD="curl $CURL_ARGS"

if [[ "$OPERATION" = "get-local-member" ]]; then
    echo "Getting local CP member information on ${ADDRESS}:${PORT}."
    response=$(${CURL_CMD} "${URL_BASE}/members/local")
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

    if [[ "$status_code" = "200" ]];then
        echo "${json}\nOK"
        exit 0
    fi
    if [[ "$status_code" = "404" ]];then
        echo "Not found";
        exit ${NOT_FOUND_RETURN_VALUE}
    fi

    echo "Status Code: ${status_code}\nResponse: ${json}\nInternal error!"
    exit ${INTERNAL_ERROR_RETURN_VALUE}
fi

if [[ "$OPERATION" = "get-groups" ]]; then
    echo "Getting CP group IDs on ${ADDRESS}:${PORT}."
    response=$(${CURL_CMD} "${URL_BASE}/groups")
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

    if [[ "$status_code" = "200" ]];then
        echo "${json}\nOK"
        exit 0
    fi

    echo "Status Code: ${status_code}\nResponse: ${json}\nInternal error!"
    exit 6
fi

if [[ "$OPERATION" = "get-group" ]]; then
    if [[ -z "$CP_GROUP_NAME" ]]; then
        echo "No CP group name is defined! You must provide a CP group name with -c\nMissing argument!"
        exit ${MISSING_ARGUMENT_RETURN_VALUE}
    fi

    echo "Getting CP group: ${CP_GROUP_NAME} on ${ADDRESS}:${PORT}."
    response=$(${CURL_CMD} "${URL_BASE}/groups/${CP_GROUP_NAME}")
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

    if [[ "$status_code" = "200" ]];then
        echo "${json}\nOK"
        exit 0
    fi
    if [[ "$status_code" = "404" ]];then
        echo "Not found"
        exit ${NOT_FOUND_RETURN_VALUE}
    fi

    echo "Status Code: ${status_code}\nResponse: ${json}\nInternal error!"
    exit ${INTERNAL_ERROR_RETURN_VALUE}
fi

if [[ "$OPERATION" = "get-members" ]]; then
    echo "Getting CP members on ${ADDRESS}:${PORT}."
    response=$(${CURL_CMD} "${URL_BASE}/members")
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

    if [[ "$status_code" = "200" ]];then
        echo "${json}\nOK"
        exit 0
    fi

    echo "Internal error!\nStatus Code: ${status_code}\nResponse: ${json}"
    exit ${INTERNAL_ERROR_RETURN_VALUE}
fi

if [[ "$OPERATION" = "get-sessions" ]]; then
    if [[ -z "$CP_GROUP_NAME" ]]; then
        echo "No CP group name is defined! You must provide a CP group name with -c\nMissing argument!"
        exit ${MISSING_ARGUMENT_RETURN_VALUE}
    fi

    echo "Getting CP sessions in CP group: ${CP_GROUP_NAME} on ${ADDRESS}:${PORT}."
    response=$(${CURL_CMD} "${URL_BASE}/groups/${CP_GROUP_NAME}/sessions")
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

    if [[ "$status_code" = "200" ]];then
        echo "${json}\nOK"
        exit 0
    fi
    if [[ "$status_code" = "404" ]];then
        echo "Not found"
        exit ${NOT_FOUND_RETURN_VALUE}
    fi

    echo "Status Code: ${status_code}\nResponse: ${json}\nInternal error!"
    exit ${INTERNAL_ERROR_RETURN_VALUE}
fi

if [[ "$OPERATION" = "force-destroy-group" ]]; then
    if [[ -z "$CP_GROUP_NAME" ]]; then
        echo "No CP group name is defined! You must provide a CP group name with -c\nMissing argument!"
        exit ${MISSING_ARGUMENT_RETURN_VALUE}
    fi

    echo "Force-destroying CP group: ${CP_GROUP_NAME} on ${ADDRESS}:${PORT}."
    response=$(${CURL_CMD} --data "${GROUPNAME}&${PASSWORD}" "${URL_BASE}/groups/${CP_GROUP_NAME}/remove")
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

    if [[ "$status_code" = "200" ]];then
        echo "OK"
        exit 0
    fi
    if [[ "$status_code" = "400" ]];then
        echo "Bad request"
        exit ${BAD_REQUEST_RETURN_VALUE}
    fi
    if [[ "$status_code" = "403" ]];then
        echo "Invalid credentials"
        exit 3
    fi

    echo "Status Code: ${status_code}\nResponse: ${json}\nInternal error!"
    exit ${INTERNAL_ERROR_RETURN_VALUE}
fi

if [[ "$OPERATION" = "promote-member" ]]; then
    echo "Promoting to CP member on ${ADDRESS}:${PORT}."
    response=$(${CURL_CMD} --data "${GROUPNAME}&${PASSWORD}" "${URL_BASE}/members")
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

    if [[ "$status_code" = "200" ]];then
        echo "OK"
        exit 0
    fi
    if [[ "$status_code" = "403" ]];then
        echo "Invalid credentials"
        exit ${INVALID_CREDENTIALS_RETURN_VALUE}
    fi

    echo "Status Code: ${status_code}\nResponse: ${json}\nInternal error!"
    exit ${INTERNAL_ERROR_RETURN_VALUE}
fi

if [[ "$OPERATION" = "remove-member" ]]; then
    if [[ -z "$CP_MEMBER_UID" ]]; then
        echo "No CP member is defined! You must provide a CP member UUID with -m\nMissing argument!"
        exit ${MISSING_ARGUMENT_RETURN_VALUE}
    fi

    echo "Removing CP member: ${CP_MEMBER_UID} on ${ADDRESS}:${PORT}."
    response=$(${CURL_CMD} --data "${GROUPNAME}&${PASSWORD}" "${URL_BASE}/members/${CP_MEMBER_UID}/remove")
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

    if [[ "$status_code" = "200" ]];then
        echo "OK"
        exit 0
    fi
    if [[ "$status_code" = "400" ]];then
        echo "Bad request"
        exit ${BAD_REQUEST_RETURN_VALUE}
    fi
    if [[ "$status_code" = "403" ]];then
        echo "Invalid credentials"
        exit ${INVALID_CREDENTIALS_RETURN_VALUE}
    fi

    echo "Status Code: ${status_code}\nResponse: ${json}\nInternal error!"
    exit ${INTERNAL_ERROR_RETURN_VALUE}
fi

if [[ "$OPERATION" = "force-close-session" ]]; then
    if [[ -z "$CP_GROUP_NAME" ]]; then
        echo "No CP group name is defined! You must provide a CP group name with -c\nMissing argument!"
        exit ${MISSING_ARGUMENT_RETURN_VALUE}
    fi

    if [[ -z "$CP_SESSION_ID" ]]; then
        echo "No CP session id is defined! You must provide a CP session id with -s\nMissing argument!"
        exit ${MISSING_ARGUMENT_RETURN_VALUE}
    fi

    echo "Closing CP session: ${CP_SESSION_ID} in CP group: ${CP_GROUP_NAME} ${ADDRESS}:${PORT}."
    response=$(${CURL_CMD} --data "${GROUPNAME}&${PASSWORD}" "${URL_BASE}/groups/${CP_GROUP_NAME}/sessions/${CP_SESSION_ID}/remove")
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

    if [[ "$status_code" = "200" ]];then
        echo "OK"
        exit 0
    fi
    if [[ "$status_code" = "400" ]];then
        echo "Bad request"
        exit ${BAD_REQUEST_RETURN_VALUE}
    fi
    if [[ "$status_code" = "403" ]];then
        echo "Invalid credentials"
        exit ${INVALID_CREDENTIALS_RETURN_VALUE}
    fi

    echo "Status Code: ${status_code}\nResponse: ${json}\nInternal error!"
    exit ${INTERNAL_ERROR_RETURN_VALUE}
fi

if [[ "$OPERATION" = "restart" ]]; then
    echo "Restarting the CP subsystem on ${ADDRESS}:${PORT}."
    response=$(${CURL_CMD} --data "${GROUPNAME}&${PASSWORD}" "${URL_BASE}/restart")
    json=$(echo "$response" | head -n 1)
    status_code=$(echo "$response" | tail -n1)

    if [[ "$status_code" = "200" ]];then
        echo "OK"
        exit 0
    fi
    if [[ "$status_code" = "403" ]];then
        echo "Invalid credentials"
        exit ${INVALID_CREDENTIALS_RETURN_VALUE}
    fi

    echo "Status Code: ${status_code}\nResponse: ${json}\nInternal error!"
    exit ${INTERNAL_ERROR_RETURN_VALUE}
fi

echo "Not a valid CP subsystem operation! Operations: 'get-local-member' | 'get-groups' || 'get-group' || 'force-destroy-group' || 'get-members' || 'remove-member' || 'promote-member' || 'get-sessions' || 'force-close-session' || 'restart'"
exit ${INVALID_ARGUMENT_RETURN_VALUE}
