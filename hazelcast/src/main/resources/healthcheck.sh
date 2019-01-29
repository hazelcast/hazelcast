#!/bin/sh

print_help() {
    echo "Parameters: "
    echo "	-o, --operation	    : Health check operation. Operation can be 'all', 'node-state','cluster-state','cluster-safe','migration-queue-size','cluster-size'."
    echo "	-a, --address  	    : Defines which ip address hazelcast node is running on. Default value is '127.0.0.1'."
    echo "	-p, --port  	    : Defines which port hazelcast node is running on. Default value is '5701'."
}

command -v curl >/dev/null 2>&1 || { echo >&2 "Cluster state script requires 'curl' but it's not installed. Aborting."; exit 1; }
command -v grep >/dev/null 2>&1 || { echo >&2 "Cluster state script requires 'grep' but it's not installed. Aborting."; exit 1; }
command -v sed >/dev/null 2>&1 || { echo >&2 "Cluster state script requires 'sed' but it's not installed. Aborting."; exit 1; }

if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    print_help
    exit 0
fi

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

URL="http://${ADDRESS}:${PORT}/hazelcast/health"
HTTP_RESPONSE=$(curl --silent --write-out "HTTPSTATUS:%{http_code}" $URL)
HTTP_BODY=$(echo "${HTTP_RESPONSE}" | sed -e 's/HTTPSTATUS\:.*//g')
HTTP_STATUS=$(echo "${HTTP_RESPONSE}" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')

if [ $HTTP_STATUS -ne 200 ]; then
    echo "Error while checking health of hazelcast cluster on ip ${ADDRESS} on port ${PORT}."
    echo "Please check that cluster is running and that health check is enabled (property set to true: 'hazelcast.http.healthcheck.enabled' or 'hazelcast.rest.enabled')."
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
