#!/usr/bin/env bash
# 校验指定富友回放时间窗在 Ingress 与 Pipeline 的幂等结果；只读取远端和本机持久化数据。
set -Eeuo pipefail
umask 077

usage() {
    cat <<'EOF'
用法:
  ./verify-replay-idempotency.sh <order|payment> <start> <end> [wait_seconds]

时间格式:
  yyyy-MM-ddTHH:mm:ss，end 为不包含边界；时间范围与 replay-fuiou-push.sh 一致。

示例:
  ./verify-replay-idempotency.sh order 2026-07-20T00:00:00 2026-07-21T00:00:00
  ./verify-replay-idempotency.sh payment 2026-07-20T00:00:00 2026-07-21T00:00:00 180

校验内容:
  Ingress  每个来源幂等键恰好一条 event、原文 SHA 一致、已经被 Pipeline ACK。
  Pipeline order 每个 orderNo 只有一个快照，版本不落后，等版本时原文 SHA 一致。
  Pipeline payment 每个 paySsn 只有一条流水，原文 SHA 一致。

可选环境变量:
  REPLAY_SSH_TARGET      源库 SSH 目标，默认 ubuntu@81.70.59.128
  TRADE_DB_HOST          本机 Trade MySQL 地址，默认 127.0.0.1（仅允许本机）
  TRADE_DB_PORT          默认 3306
  TRADE_DB_USER          默认 root
  TRADE_DB_PASSWORD      默认空
  VERIFY_WAIT_SECONDS    默认等待 120 秒，可被第四个参数覆盖
EOF
    exit 1
}

fail() {
    echo "[错误] $*" >&2
    exit 1
}

TYPE="${1:-}"
START="${2:-}"
END="${3:-}"
WAIT_SECONDS="${4:-${VERIFY_WAIT_SECONDS:-120}}"
[[ -n "$TYPE" && -n "$START" && -n "$END" ]] || usage

case "$TYPE" in
    order)
        SOURCE_TABLE="oms_order_push_log"
        EVENT_TABLE="trade_order_event"
        PIPELINE_TABLE_PATTERN='^oms_order_[0-9]{4}$'
        ;;
    payment)
        SOURCE_TABLE="oms_payment_push_log"
        EVENT_TABLE="trade_payment_event"
        PIPELINE_TABLE_PATTERN='^oms_payment_[0-9]{4}$'
        ;;
    *) fail "type 只能为 order 或 payment" ;;
esac

TIME_PATTERN='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$'
[[ "$START" =~ $TIME_PATTERN ]] || fail "start 格式必须为 yyyy-MM-ddTHH:mm:ss"
[[ "$END" =~ $TIME_PATTERN ]] || fail "end 格式必须为 yyyy-MM-ddTHH:mm:ss"
[[ "$START" < "$END" ]] || fail "end 必须晚于 start"
[[ "$WAIT_SECONDS" =~ ^[0-9]+$ ]] && (( WAIT_SECONDS <= 3600 )) \
    || fail "wait_seconds 必须为 0~3600"

for command_name in ssh mysql jq openssl awk paste wc tr mktemp; do
    command -v "$command_name" >/dev/null 2>&1 || fail "缺少命令: $command_name"
done

SSH_TARGET="${REPLAY_SSH_TARGET:-ubuntu@81.70.59.128}"
SOURCE_DB_USER="ubuntu"
SOURCE_DB_PASSWORD='D!9dC83d4F7'
SOURCE_DB_NAME="fuioupay-cloud"
DB_HOST="${TRADE_DB_HOST:-127.0.0.1}"
DB_PORT="${TRADE_DB_PORT:-3306}"
DB_USER="${TRADE_DB_USER:-root}"
DB_PASSWORD="${TRADE_DB_PASSWORD:-}"

case "$DB_HOST" in
    127.0.0.1|localhost|::1) ;;
    *) fail "幂等校验默认只允许连接本机 Trade MySQL" ;;
esac
[[ "$DB_PORT" =~ ^[0-9]+$ ]] && (( DB_PORT >= 1 && DB_PORT <= 65535 )) \
    || fail "TRADE_DB_PORT 无效"

MYSQL_ARGS=(
    --host="$DB_HOST"
    --port="$DB_PORT"
    --user="$DB_USER"
    --protocol=TCP
    --connect-timeout=5
    --batch
    --skip-column-names
)

run_mysql() {
    if [[ -n "$DB_PASSWORD" ]]; then
        MYSQL_PWD="$DB_PASSWORD" mysql "${MYSQL_ARGS[@]}" "$@"
    else
        mysql "${MYSQL_ARGS[@]}" "$@"
    fi
}

START_SQL="${START/T/ }"
END_SQL="${END/T/ }"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/trade-idempotency.XXXXXX")"
SOURCE_FILE="${WORK_DIR}/source.jsonl"
FIELDS_FILE="${WORK_DIR}/fields.tsv"
SHA_FILE="${WORK_DIR}/sha256.txt"
VALUES_FILE="${WORK_DIR}/values.sql"
CHECK_SQL="${WORK_DIR}/check.sql"
trap 'rm -rf "$WORK_DIR"' EXIT INT TERM

echo "读取源报文: ${TYPE}, [${START_SQL}, ${END_SQL})"
ssh -o ServerAliveInterval=30 "$SSH_TARGET" \
    "MYSQL_PWD='${SOURCE_DB_PASSWORD}' /usr/bin/mysql --user='${SOURCE_DB_USER}' --database='${SOURCE_DB_NAME}' --batch --raw --quick --skip-column-names --execute=\"SELECT push_body FROM ${SOURCE_TABLE} WHERE push_receive_time >= '${START_SQL}' AND push_receive_time < '${END_SQL}' AND push_body IS NOT NULL AND push_body <> ''\"" \
    > "$SOURCE_FILE"

SOURCE_ROWS="$(wc -l < "$SOURCE_FILE" | tr -d ' ')"
if (( SOURCE_ROWS == 0 )); then
    echo "时间范围内无源数据"
    exit 0
fi

if [[ "$TYPE" == "order" ]]; then
    jq -r '
      def required($name):
        if . == null or (tostring | length) == 0 then error($name + " is required") else tostring end;
      (.keySign | required("keySign")) as $eventKey |
      (.recUpdTm | required("recUpdTm")) as $version |
      (.orderNo | required("orderNo")) as $businessKey |
      if ($version | test("^[0-9]+$")) and ($businessKey | test("^[0-9]+$"))
      then [($eventKey | @base64), $version, ($businessKey | @base64)] | @tsv
      else error("recUpdTm/orderNo must be numeric") end
    ' "$SOURCE_FILE" > "$FIELDS_FILE" || fail "订单源报文 JSON 或幂等字段无效"
else
    jq -r '
      def required($name):
        if . == null or (tostring | length) == 0 then error($name + " is required") else tostring end;
      (.paySsn | required("paySsn")) as $eventKey |
      (.payTm | required("payTm")) as $payTime |
      if ($payTime | test("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$"))
      then [($eventKey | @base64), ($payTime | @base64), ($eventKey | @base64)] | @tsv
      else error("payTm format is invalid") end
    ' "$SOURCE_FILE" > "$FIELDS_FILE" || fail "支付源报文 JSON 或幂等字段无效"
fi

while IFS= read -r body || [[ -n "$body" ]]; do
    printf '%s' "$body" | openssl dgst -sha256 -r | awk '{print $1}'
done < "$SOURCE_FILE" > "$SHA_FILE"

FIELD_ROWS="$(wc -l < "$FIELDS_FILE" | tr -d ' ')"
SHA_ROWS="$(wc -l < "$SHA_FILE" | tr -d ' ')"
[[ "$FIELD_ROWS" == "$SOURCE_ROWS" && "$SHA_ROWS" == "$SOURCE_ROWS" ]] \
    || fail "源报文解析数量不一致: source=${SOURCE_ROWS}, fields=${FIELD_ROWS}, sha=${SHA_ROWS}"

paste "$FIELDS_FILE" "$SHA_FILE" | while IFS=$'\t' read -r event_key_b64 version_value business_key_b64 sha256; do
    if [[ "$TYPE" == "order" ]]; then
        message_version="$version_value"
    else
        message_version="CAST(UNIX_TIMESTAMP(STR_TO_DATE(CONVERT(FROM_BASE64('${version_value}') USING utf8mb4), '%Y-%m-%d %H:%i:%s')) * 1000 AS UNSIGNED)"
    fi
    printf "(CONVERT(FROM_BASE64('%s') USING utf8mb4),%s,UNHEX('%s'),CONVERT(FROM_BASE64('%s') USING utf8mb4))\n" \
        "$event_key_b64" "$message_version" "$sha256" "$business_key_b64"
done > "$VALUES_FILE"

PIPELINE_TABLES=()
while IFS= read -r table_name; do
    [[ -n "$table_name" ]] && PIPELINE_TABLES+=("$table_name")
done < <(run_mysql --execute="
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'trade_pipeline'
  AND table_type = 'BASE TABLE'
  AND table_name REGEXP '${PIPELINE_TABLE_PATTERN}'
ORDER BY table_name")
(( ${#PIPELINE_TABLES[@]} > 0 )) || fail "未找到 ${TYPE} Pipeline 年度物理表"

{
    cat <<'SQL'
SET SESSION time_zone = '+08:00';
CREATE TEMPORARY TABLE expected_source (
  event_key VARCHAR(128) NOT NULL,
  message_version BIGINT UNSIGNED NOT NULL,
  payload_sha256 BINARY(32) NOT NULL,
  business_key VARCHAR(128) NOT NULL
);
SQL
    awk '
      NR % 500 == 1 { printf "INSERT INTO expected_source VALUES\n" }
      { printf "%s%s", (NR % 500 == 1 ? "" : ",\n"), $0 }
      NR % 500 == 0 { print ";" }
      END { if (NR % 500 != 0) print ";" }
    ' "$VALUES_FILE"
    cat <<'SQL'
CREATE INDEX idx_expected_event ON expected_source(event_key, message_version);
CREATE INDEX idx_expected_business ON expected_source(business_key);

CREATE TEMPORARY TABLE expected_event AS
SELECT event_key, message_version, MIN(payload_sha256) AS payload_sha256
FROM expected_source
GROUP BY event_key, message_version;
CREATE INDEX idx_event_key ON expected_event(event_key, message_version);

CREATE TEMPORARY TABLE expected_business AS
SELECT business_key, MAX(message_version) AS message_version
FROM expected_source
GROUP BY business_key;
CREATE INDEX idx_business_key ON expected_business(business_key);

CREATE TEMPORARY TABLE actual_ingress AS
SELECT e.third_event_key AS event_key, e.message_version, e.payload_sha256, e.acked
FROM trade_flow.__EVENT_TABLE__ e
JOIN expected_event x
  ON x.event_key = e.third_event_key
 AND x.message_version = e.message_version
WHERE e.source_system = 1;
CREATE INDEX idx_actual_ingress ON actual_ingress(event_key, message_version);

CREATE TEMPORARY TABLE actual_business AS
SQL
    first=1
    for table_name in "${PIPELINE_TABLES[@]}"; do
        if (( first == 0 )); then
            printf '\nUNION ALL\n'
        fi
        first=0
        if [[ "$TYPE" == "order" ]]; then
            printf "SELECT CAST(a.order_no AS CHAR) AS business_key, CAST(ROUND(UNIX_TIMESTAMP(a.source_update_time) * 1000) AS UNSIGNED) AS message_version, a.payload_sha256, '%s' AS shard_name FROM trade_pipeline.\`%s\` a JOIN expected_business x ON a.order_no = CAST(x.business_key AS UNSIGNED)" "$table_name" "$table_name"
        else
            printf "SELECT a.pay_ssn AS business_key, 0 AS message_version, a.payload_sha256, '%s' AS shard_name FROM trade_pipeline.\`%s\` a JOIN expected_business x ON a.pay_ssn = x.business_key" "$table_name" "$table_name"
        fi
    done
    cat <<'SQL'
;
CREATE INDEX idx_actual_business ON actual_business(business_key);

SELECT 'source_rows', COUNT(*) FROM expected_source;
SELECT 'source_distinct_events', COUNT(*) FROM expected_event;
SELECT 'source_exact_repeats',
       (SELECT COUNT(*) FROM expected_source) - (SELECT COUNT(*) FROM expected_event);
SELECT 'source_event_key_conflicts', COUNT(*) FROM (
  SELECT event_key, message_version
  FROM expected_source
  GROUP BY event_key, message_version
  HAVING COUNT(DISTINCT payload_sha256) > 1
) conflicts;
SELECT 'ingress_missing_events', COUNT(*)
FROM expected_event x
LEFT JOIN actual_ingress a
  ON a.event_key = x.event_key AND a.message_version = x.message_version
WHERE a.event_key IS NULL;
SELECT 'ingress_duplicate_events', COUNT(*) FROM (
  SELECT event_key, message_version
  FROM actual_ingress
  GROUP BY event_key, message_version
  HAVING COUNT(*) > 1
) duplicates;
SELECT 'ingress_sha_mismatches', COUNT(*)
FROM expected_event x
JOIN actual_ingress a
  ON a.event_key = x.event_key AND a.message_version = x.message_version
WHERE a.payload_sha256 <> x.payload_sha256;
SELECT 'ingress_unacked_events', COUNT(*)
FROM expected_event x
JOIN actual_ingress a
  ON a.event_key = x.event_key AND a.message_version = x.message_version
WHERE a.acked <> 1;
SELECT 'pipeline_missing_business_rows', COUNT(*)
FROM expected_business x
LEFT JOIN actual_business a ON a.business_key = x.business_key
WHERE a.business_key IS NULL;
SELECT 'pipeline_duplicate_business_rows', COUNT(*) FROM (
  SELECT business_key
  FROM actual_business
  GROUP BY business_key
  HAVING COUNT(*) > 1
) duplicates;
SQL
    if [[ "$TYPE" == "order" ]]; then
        cat <<'SQL'
SELECT 'source_orders_with_multiple_versions', COUNT(*) FROM (
  SELECT business_key
  FROM expected_source
  GROUP BY business_key
  HAVING COUNT(DISTINCT message_version) > 1
) versions;
SELECT 'pipeline_version_behind', COUNT(*)
FROM expected_business x
JOIN (
  SELECT business_key, MAX(message_version) AS message_version
  FROM actual_business
  GROUP BY business_key
) a ON a.business_key = x.business_key
WHERE a.message_version < x.message_version;
SELECT 'pipeline_latest_sha_mismatches', COUNT(DISTINCT x.business_key)
FROM expected_business x
JOIN expected_source s
  ON s.business_key = x.business_key AND s.message_version = x.message_version
JOIN actual_business a
  ON a.business_key = x.business_key AND a.message_version = x.message_version
WHERE a.payload_sha256 <> s.payload_sha256;
SELECT 'hard_failures',
  (SELECT COUNT(*) FROM (
     SELECT event_key, message_version FROM expected_source GROUP BY event_key, message_version
     HAVING COUNT(DISTINCT payload_sha256) > 1
  ) c1)
  + (SELECT COUNT(*) FROM expected_event x LEFT JOIN actual_ingress a ON a.event_key=x.event_key AND a.message_version=x.message_version WHERE a.event_key IS NULL)
  + (SELECT COUNT(*) FROM (SELECT event_key,message_version FROM actual_ingress GROUP BY event_key,message_version HAVING COUNT(*)>1) d)
  + (SELECT COUNT(*) FROM expected_event x JOIN actual_ingress a ON a.event_key=x.event_key AND a.message_version=x.message_version WHERE a.payload_sha256<>x.payload_sha256 OR a.acked<>1)
  + (SELECT COUNT(*) FROM expected_business x LEFT JOIN actual_business a ON a.business_key=x.business_key WHERE a.business_key IS NULL)
  + (SELECT COUNT(*) FROM (SELECT business_key FROM actual_business GROUP BY business_key HAVING COUNT(*)>1) d)
  + (SELECT COUNT(*) FROM expected_business x JOIN (SELECT business_key,MAX(message_version) message_version FROM actual_business GROUP BY business_key) a ON a.business_key=x.business_key WHERE a.message_version<x.message_version)
  + (SELECT COUNT(DISTINCT x.business_key) FROM expected_business x JOIN expected_source s ON s.business_key=x.business_key AND s.message_version=x.message_version JOIN actual_business a ON a.business_key=x.business_key AND a.message_version=x.message_version WHERE a.payload_sha256<>s.payload_sha256);
SQL
    else
        cat <<'SQL'
SELECT 'source_business_key_conflicts', COUNT(*) FROM (
  SELECT business_key
  FROM expected_source
  GROUP BY business_key
  HAVING COUNT(DISTINCT payload_sha256) > 1
) conflicts;
SELECT 'pipeline_sha_mismatches', COUNT(DISTINCT x.business_key)
FROM expected_business x
JOIN expected_source s ON s.business_key = x.business_key
JOIN actual_business a ON a.business_key = x.business_key
WHERE a.payload_sha256 <> s.payload_sha256;
SELECT 'hard_failures',
  (SELECT COUNT(*) FROM (SELECT event_key,message_version FROM expected_source GROUP BY event_key,message_version HAVING COUNT(DISTINCT payload_sha256)>1) c1)
  + (SELECT COUNT(*) FROM (SELECT business_key FROM expected_source GROUP BY business_key HAVING COUNT(DISTINCT payload_sha256)>1) c2)
  + (SELECT COUNT(*) FROM expected_event x LEFT JOIN actual_ingress a ON a.event_key=x.event_key AND a.message_version=x.message_version WHERE a.event_key IS NULL)
  + (SELECT COUNT(*) FROM (SELECT event_key,message_version FROM actual_ingress GROUP BY event_key,message_version HAVING COUNT(*)>1) d)
  + (SELECT COUNT(*) FROM expected_event x JOIN actual_ingress a ON a.event_key=x.event_key AND a.message_version=x.message_version WHERE a.payload_sha256<>x.payload_sha256 OR a.acked<>1)
  + (SELECT COUNT(*) FROM expected_business x LEFT JOIN actual_business a ON a.business_key=x.business_key WHERE a.business_key IS NULL)
  + (SELECT COUNT(*) FROM (SELECT business_key FROM actual_business GROUP BY business_key HAVING COUNT(*)>1) d)
  + (SELECT COUNT(DISTINCT x.business_key) FROM expected_business x JOIN expected_source s ON s.business_key=x.business_key JOIN actual_business a ON a.business_key=x.business_key WHERE a.payload_sha256<>s.payload_sha256);
SQL
    fi
} > "$CHECK_SQL"

# 表名来自固定分支，替换后 SQL 仍通过 stdin 执行，避免报文键出现在命令行。
sed "s/__EVENT_TABLE__/${EVENT_TABLE}/g" "$CHECK_SQL" > "${CHECK_SQL}.resolved"
mv "${CHECK_SQL}.resolved" "$CHECK_SQL"

deadline=$(( $(date +%s) + WAIT_SECONDS ))
attempt=0
while true; do
    attempt=$((attempt + 1))
    RESULT="$(run_mysql < "$CHECK_SQL")" || fail "本机幂等校验 SQL 执行失败"
    HARD_FAILURES="$(printf '%s\n' "$RESULT" | awk -F '\t' '$1 == "hard_failures" {print $2}' | tail -1)"
    [[ "$HARD_FAILURES" =~ ^[0-9]+$ ]] || fail "无法读取校验结果"
    if (( HARD_FAILURES == 0 || $(date +%s) >= deadline )); then
        break
    fi
    echo "Pipeline 尚未收敛，5 秒后重试（第 ${attempt} 次，当前异常 ${HARD_FAILURES}）..."
    sleep 5
done

echo
echo "幂等校验结果 (${TYPE})"
printf '%-38s %s\n' "指标" "数量"
printf '%s\n' "$RESULT" | awk -F '\t' '{printf "%-38s %s\n", $1, $2}'

if (( HARD_FAILURES > 0 )); then
    echo "结论: ❌ 未通过；存在缺失、重复、SHA/版本冲突或尚未 ACK 的数据。"
    exit 2
fi
echo "结论: ✅ Ingress 与 Pipeline 在该源时间范围内的幂等结果已收敛。"
