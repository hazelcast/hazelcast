#!/bin/bash

print_help() {
    echo "Parameters: "
    echo "  -o, --operation   : Health check operation. Operation can be 'all', 'node-state','cluster-state','cluster-safe','migration-queue-size','cluster-size'."
    echo "  -a, --address     : Defines which ip address hazelcast node is running on. Default value is '127.0.0.1'."
    echo "  -p, --port        : Defines which port hazelcast node is running on. Default value is '5701'."
    echo "  -d, --debug       : Prints curl error output."
    echo "HTTPs related (TLS enabled):"
    echo "      --https       : Uses HTTPs protocol for REST calls. (no parameter value expected)"
    echo "      --cacert      : Defines trusted PEM-encoded certificate file path. It's used to verify member certificates."
    echo "      --cert        : Defines PEM-encoded client certificate file path. Only needed when client certificate authentication is used."
    echo "      --key         : Defines PEM-encoded client private key file path. Only needed when client certificate authentication is used."
    echo "      --insecure    : Disables member certificate verification. (no parameter value expected)"

}

command -v curl >/dev/null 2>&1 || { echo >&2 "Cluster state script requires 'curl' but it's not installed. Aborting."; exit 1; }
command -v grep >/dev/null 2>&1 || { echo >&2 "Cluster state script requires 'grep' but it's not installed. Aborting."; exit 1; }
command -v sed >/dev/null 2>&1 || { echo >&2 "Cluster state script requires 'sed' but it's not installed. Aborting."; exit 1; }

if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    print_help
    exit 0
fi

URL_SCHEME="http"
CURL_ARGS="--silent"

while [ $# -gt 0 ]; do
    key="$1"
    case "$key" in
        -o|--operation)
            OPERATION="$2"
            shift
            ;;
        -p|--port)
            PORT="$2"
            shift
            ;;
        -a|--address)
            ADDRESS="$2"
            shift
            ;;
        -d|--debug)
            CURL_ARGS="$CURL_ARGS --show-error"
            ;;
        --https)
            URL_SCHEME="https"
            ;;
        --cert|--key)
            CURL_ARGS="$CURL_ARGS $1 $2"
            shift
            ;;
        --cacert)
            CURL_ARGS="$CURL_ARGS --capath /dev/null $1 $2"
            shift
            ;;
        --insecure)
            echo "WARNING: You're using the insecure switch. Hazelcast member TLS certificates will not be verified!" >&2
            CURL_ARGS="$CURL_ARGS $1"
            ;;
        *)
            echo "Unsupported parameter: '${1}'"
            print_help
            exit 1
    esac
    shift
done

if [ -z "$OPERATION" ]; then
 	OPERATION="all"
fi

if [ -z "$PORT" ]; then
    PORT="5701"
fi

if [ -z "$ADDRESS" ]; then
    ADDRESS="127.0.0.1"
fi

case "$OPERATION" in
    node-state)
        KEYWORD="NodeState" ;;
    cluster-state)
        KEYWORD="ClusterState" ;;
    cluster-safe)
        KEYWORD="ClusterSafe" ;;
    migration-queue-size)
        KEYWORD="MigrationQueueSize" ;;
    cluster-size)
        KEYWORD="ClusterSize" ;;
    all) ;;
    *)
        echo "Unsupported operation, valid operations  are: 'node-state', 'cluster-state', 'cluster-safe', 'migration-queue-size', 'cluster-size'."
        exit 1
esac

URL="${URL_SCHEME}://${ADDRESS}:${PORT}/hazelcast/health"
HTTP_RESPONSE=$(curl ${CURL_ARGS} --write-out "HTTPSTATUS:%{http_code}" $URL)
HTTP_BODY=$(echo "${HTTP_RESPONSE}" | sed -e 's/HTTPSTATUS\:.*//g')
HTTP_STATUS=$(echo "${HTTP_RESPONSE}" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')

if [ $HTTP_STATUS -ne 200 ]; then
    echo "Error while checking health of hazelcast cluster on ip ${ADDRESS} on port ${PORT}."
    echo "Please check that cluster is running and that health check is enabled in REST API configuration."
    exit 1
fi

if [ "$OPERATION" = "all" ]; then
    echo "${HTTP_BODY}"
    exit 0
fi

PREFIX_REGEX="Hazelcast\:\:${KEYWORD}\="
RESULT=$(echo $HTTP_BODY | grep -Eo "${PREFIX_REGEX}[A-Z_0-9]*" | sed "s/${PREFIX_REGEX}//")

echo "${RESULT}"
exit 0
