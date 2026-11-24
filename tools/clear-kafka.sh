#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

kafka() {
  local command=$1
  shift
  docker compose exec -T kafka "/opt/kafka/bin/$command.sh" \
    --bootstrap-server kafka:29092 "$@"
}

topics=$(kafka kafka-topics --list --exclude-internal)
if [[ -z $topics ]]; then
  echo '삭제할 Kafka 토픽이 없습니다.'
  exit 0
fi

offsets=$(kafka kafka-get-offsets --time -1)
request=$(jq -Rn --arg topics "$topics" '
  ($topics | split("\n")) as $allowed
  | {version: 1, partitions: [
      inputs | split(":")
      | select(length == 3 and (.[0] as $topic | $allowed | index($topic)) != null)
      | {topic: .[0], partition: (.[1] | tonumber), offset: (.[2] | tonumber)}
    ]}
' <<< "$offsets")

if [[ $(jq '.partitions | length' <<< "$request") -eq 0 ]]; then
  echo '삭제할 Kafka 파티션이 없습니다.'
  exit 0
fi

docker compose exec -T kafka sh -c '
  set -e
  request=$(mktemp)
  trap '\''unlink "$request"'\'' EXIT
  cat > "$request"
  /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server kafka:29092 --offset-json-file "$request"
' <<< "$request"
