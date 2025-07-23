#!/bin/bash

# Configuration
KAFKA_BROKER="localhost:9092"
KAFKA_TOPIC="servicenow_updates"
NULL_CHANCE=30   # Percent chance to make a value null

# Value pools
OPERATIONS=("insert" "update" "delete")
SOURCE_TABLES=("cmdb_ci_service_auto" "cmdb_ci_server" "cmdb_ci_database" "cmdb_ci_web_service")
ATTESTATION_STATUS=("Not yet reviewed" "Approved" "Rejected" "Pending")
BUSINESS_CRITICALITY=("1 - most critical" "2 - highly critical" "3 - moderately critical" "4 - not critical")

# Helper functions
generate_random_string() {
    tr -dc A-Za-z0-9 </dev/urandom | head -c 16
}

random_choice() {
    local arr=("$@")
    echo "${arr[$RANDOM % ${#arr[@]}]}"
}

maybe_null_string() {
    if (( RANDOM % 100 < NULL_CHANCE )); then
        echo "null"
    else
        echo "\"$(generate_random_string)\""
    fi
}

maybe_null_enum() {
    if (( RANDOM % 100 < NULL_CHANCE )); then
        echo "null"
    else
        echo "\"$(random_choice "$@")\""
    fi
}

generate_data_json() {
    echo -n "\"aliases\": null,"
    echo -n "\"asset\": $(maybe_null_string),"
    echo -n "\"asset_tag\": $(maybe_null_string),"
    echo -n "\"assigned_to\": $(maybe_null_string),"
    echo -n "\"attestation_status\": $(maybe_null_enum "${ATTESTATION_STATUS[@]}"),"
    echo -n "\"business_criticality\": $(maybe_null_enum "${BUSINESS_CRITICALITY[@]}")"

    for ((i=1; i<=125; i++)); do
        key="key_$i"
        if (( RANDOM % 100 < NULL_CHANCE )); then
            value="null"
        else
            value="\"$(generate_random_string)\""
        fi
        echo -n ",\"$key\": $value"
    done
}

# Trap for clean shutdown
trap "echo -e '\nTerminating...'; exit" SIGINT

# Infinite loop to generate and send messages
i=1
while true; do
    OPERATION=$(random_choice "${OPERATIONS[@]}")
    SOURCE_TABLE=$(random_choice "${SOURCE_TABLES[@]}")
    PAYLOAD_SYS_ID=$(generate_random_string)
    TIMESTAMP=$(date +%s%3N)

    DATA_JSON=$(generate_data_json)

    # Full JSON message, properly escaped
    MESSAGE=$(cat <<EOF
{"type":"record","operation":"$OPERATION","source_table":"$SOURCE_TABLE","payload_sys_id":"$PAYLOAD_SYS_ID","timestamp":$TIMESTAMP,"data":{${DATA_JSON}}}
EOF
)

    echo "[$(date)] Sending message #$i → $KAFKA_TOPIC"
    echo "$MESSAGE" | kafka-console-producer.sh --broker-list "$KAFKA_BROKER" --topic "$KAFKA_TOPIC" > /dev/null
    ((i++))
    sleep 1
done
