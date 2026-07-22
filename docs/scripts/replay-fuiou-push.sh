#!/usr/bin/env bash
# 富友推流回放工具：通过 SSH 流式读取远端 MySQL，并回放到本机 Ingress。
set -Eeuo pipefail
umask 077

# xargs 启动的内部 worker。报文只通过临时文件传递，不出现在进程命令行。
if [[ "${1:-}" == "__send-one" ]]; then
    payload_file="${2:-}"
    [[ -n "$payload_file" && -f "$payload_file" ]] || exit 0
    [[ -n "${REPLAY_WORKER_TARGET:-}" \
        && -d "${REPLAY_RESULT_DIR:-}" \
        && -d "${REPLAY_ACTIVE_DIR:-}" ]] || exit 1

    result_name="$(basename "$payload_file")"
    active_file="${REPLAY_ACTIVE_DIR}/${result_name}.json"
    ln -s "$payload_file" "$active_file"

    response="$(curl -sS -w '\n%{http_code}' \
        --connect-timeout 3 --max-time 30 \
        -X POST "$REPLAY_WORKER_TARGET" \
        -H 'Content-Type: application/json' \
        --data-binary @"$payload_file" 2>/dev/null || true)"
    http_code="${response##*$'\n'}"
    response_body="${response%$'\n'*}"
    rm -f "$active_file"

    if [[ "$http_code" == "200" ]] \
            && [[ "$response_body" =~ \"resultCode\"[[:space:]]*:[[:space:]]*\"000000\" ]]; then
        result_file="${REPLAY_RESULT_DIR}/success.${result_name}"
    else
        result_file="${REPLAY_RESULT_DIR}/fail.${result_name}"
    fi
    printf 'http_code=%s\nresponse=%s\npayload_file=%s\n' \
        "$http_code" "$response_body" "$payload_file" > "$result_file"

    sleep "${REPLAY_WORKER_DELAY:-0}"
    exit 0
fi

usage() {
    cat <<'EOF'
用法:
  ./docs/scripts/replay-fuiou-push.sh <order|payment> <start> <end> [parallel] [base_url]

时间格式:
  yyyy-MM-ddTHH:mm:ss，end 为不包含边界。

示例:
  ./docs/scripts/replay-fuiou-push.sh order 2026-07-20T00:00:00 2026-07-21T00:00:00
  REPLAY_RPS=10 ./docs/scripts/replay-fuiou-push.sh payment 2026-07-20T00:00:00 2026-07-20T01:00:00 4

可选环境变量:
  REPLAY_SSH_TARGET      SSH 目标，默认 ubuntu@81.70.59.128
  REPLAY_RPS             全局近似限速，默认 20 条/秒，最大 100
  REPLAY_ALLOW_REMOTE_TARGET=yes  允许回放到非本机地址
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
PARALLEL="${4:-4}"
BASE_URL="${5:-http://127.0.0.1:8115/trade-ingress}"

[[ -n "$TYPE" && -n "$START" && -n "$END" ]] || usage

case "$TYPE" in
    order)
        TABLE="oms_order_push_log"
        ENDPOINT="/order/store-push"
        ;;
    payment)
        TABLE="oms_payment_push_log"
        ENDPOINT="/payment/store-push"
        ;;
    *) fail "type 只能为 order 或 payment" ;;
esac

TIME_PATTERN='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$'
[[ "$START" =~ $TIME_PATTERN ]] || fail "start 格式必须为 yyyy-MM-ddTHH:mm:ss"
[[ "$END" =~ $TIME_PATTERN ]] || fail "end 格式必须为 yyyy-MM-ddTHH:mm:ss"
[[ "$START" < "$END" ]] || fail "end 必须晚于 start"
[[ "$PARALLEL" =~ ^[0-9]+$ ]] && (( PARALLEL >= 1 && PARALLEL <= 16 )) \
    || fail "parallel 必须为 1~16"

RPS="${REPLAY_RPS:-20}"
[[ "$RPS" =~ ^[0-9]+$ ]] && (( RPS >= 1 && RPS <= 100 )) \
    || fail "REPLAY_RPS 必须为 1~100"
[[ "$BASE_URL" =~ ^https?:// ]] || fail "base_url 必须以 http:// 或 https:// 开头"
if [[ ! "$BASE_URL" =~ ^https?://(localhost|127\.0\.0\.1|\[::1\])([:/]|$) ]] \
        && [[ "${REPLAY_ALLOW_REMOTE_TARGET:-no}" != "yes" ]]; then
    fail "默认只允许回放到本机；远端目标需显式设置 REPLAY_ALLOW_REMOTE_TARGET=yes"
fi

for command_name in ssh curl xargs awk find; do
    command -v "$command_name" >/dev/null 2>&1 || fail "缺少命令: $command_name"
done

SSH_TARGET="${REPLAY_SSH_TARGET:-ubuntu@81.70.59.128}"
DB_USER="ubuntu"
DB_PASSWORD='D!9dC83d4F7'
DB_NAME="fuioupay-cloud"

START_SQL="${START/T/ }"
END_SQL="${END/T/ }"
TARGET="${BASE_URL%/}${ENDPOINT}"
SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/trade-replay.XXXXXX")"
PAYLOAD_DIR="${WORK_DIR}/payloads"
RESULT_DIR="${WORK_DIR}/results"
ACTIVE_DIR="${WORK_DIR}/active"
PROGRESS_PID=""
KEEP_WORK_DIR=0

cleanup() {
    if [[ -n "$PROGRESS_PID" ]] && kill -0 "$PROGRESS_PID" 2>/dev/null; then
        kill "$PROGRESS_PID" 2>/dev/null || true
        wait "$PROGRESS_PID" 2>/dev/null || true
    fi
    if (( KEEP_WORK_DIR == 0 )); then
        rm -rf "$WORK_DIR"
    else
        echo "调试目录已保留: $WORK_DIR" >&2
    fi
}
trap cleanup EXIT
trap 'KEEP_WORK_DIR=1; exit 130' INT TERM

mkdir -p "$PAYLOAD_DIR" "$RESULT_DIR" "$ACTIVE_DIR"

# 每个 worker 的间隔 = 并发数 / 全局 RPS，合计近似限制到 REPLAY_RPS。
WORKER_DELAY="$(awk -v parallel="$PARALLEL" -v rps="$RPS" \
    'BEGIN { printf "%.3f", parallel / rps }')"
export REPLAY_WORKER_TARGET="$TARGET"
export REPLAY_WORKER_DELAY="$WORKER_DELAY"
export REPLAY_RESULT_DIR="$RESULT_DIR"
export REPLAY_ACTIVE_DIR="$ACTIVE_DIR"

echo "流式读取并回放: ${TYPE}, [${START_SQL}, ${END_SQL})"
echo "目标: ${TARGET}；并发: ${PARALLEL}；限速: 约 ${RPS} 条/秒"
echo "运行明细: ${WORK_DIR}"

START_TS="$(date +%s)"

(
    while true; do
        sleep 2
        READ_COUNT="$(find "$PAYLOAD_DIR" -type f | wc -l | tr -d ' ')"
        ACTIVE_COUNT="$(find "$ACTIVE_DIR" -type l | wc -l | tr -d ' ')"
        SUCCESS_COUNT="$(find "$RESULT_DIR" -type f -name 'success.*' | wc -l | tr -d ' ')"
        FAIL_COUNT="$(find "$RESULT_DIR" -type f -name 'fail.*' | wc -l | tr -d ' ')"
        PROCESSED_COUNT=$((SUCCESS_COUNT + FAIL_COUNT))
        ELAPSED=$(( $(date +%s) - START_TS ))
        (( ELAPSED > 0 )) || ELAPSED=1
        echo "进度: 已读取 ${READ_COUNT}，请求中 ${ACTIVE_COUNT}，已处理 ${PROCESSED_COUNT}，成功 ${SUCCESS_COUNT}，失败 ${FAIL_COUNT}，耗时 ${ELAPSED}s"
    done
) &
PROGRESS_PID="$!"

# 保持为前台管道，使 Ctrl-C 同时中断 ssh、读取循环、xargs 和 curl worker。
PIPELINE_STATUS=0
ssh -o ServerAliveInterval=30 "$SSH_TARGET" \
    "MYSQL_PWD='${DB_PASSWORD}' /usr/bin/mysql --user='${DB_USER}' --database='${DB_NAME}' --batch --raw --quick --skip-column-names --execute=\"SELECT push_body FROM ${TABLE} WHERE push_receive_time >= '${START_SQL}' AND push_receive_time < '${END_SQL}' AND push_body IS NOT NULL AND push_body <> ''\"" \
| while IFS= read -r body || [[ -n "$body" ]]; do
    [[ -n "$body" ]] || continue
    payload_file="$(mktemp "${PAYLOAD_DIR}/payload.XXXXXX")"
    printf '%s' "$body" > "$payload_file"
    printf '%s\0' "$payload_file"
done \
| xargs -0 -n 1 -P "$PARALLEL" "$SCRIPT_PATH" __send-one \
|| PIPELINE_STATUS=$?

kill "$PROGRESS_PID" 2>/dev/null || true
wait "$PROGRESS_PID" 2>/dev/null || true
PROGRESS_PID=""

if (( PIPELINE_STATUS != 0 )); then
    KEEP_WORK_DIR=1
    fail "线上读取或回放进程异常中止"
fi

TOTAL="$(find "$PAYLOAD_DIR" -type f | wc -l | tr -d ' ')"
TOTAL_SUCCESS="$(find "$RESULT_DIR" -type f -name 'success.*' | wc -l | tr -d ' ')"
TOTAL_FAIL="$(find "$RESULT_DIR" -type f -name 'fail.*' | wc -l | tr -d ' ')"
ELAPSED=$(( $(date +%s) - START_TS ))
(( ELAPSED > 0 )) || ELAPSED=1

if (( TOTAL == 0 )); then
    echo "时间范围内无数据"
    exit 0
fi

echo "完成：读取 ${TOTAL}，成功 ${TOTAL_SUCCESS}，失败 ${TOTAL_FAIL}，耗时 ${ELAPSED}s，平均 $((TOTAL / ELAPSED)) 条/秒"
if (( TOTAL_FAIL > 0 )); then
    KEEP_WORK_DIR=1
    exit 2
fi
