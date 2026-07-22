#!/bin/bash
# ===================================================================
# 第三方推流回放测试工具 (Shell 版)
# 从线上 MySQL 读取历史 push_body，回放到本地接口
# ===================================================================
set -e

usage() {
    cat << 'EOF'
用法:
  ./replay.sh <type> <start> <end> [parallel] [base_url]

参数:
  type      推送类型: order | payment
  start     开始时间，格式: yyyy-MM-ddTHH:mm:ss
  end       结束时间
  parallel  并发数 (可选，默认 8)
  base_url  目标地址 (可选，默认 http://localhost:8115/trade-ingress)

示例:
  ./replay.sh order 2026-07-20T00:00:00 2026-07-22T23:59:59
  ./replay.sh payment 2026-07-20T00:00:00 2026-07-22T23:59:59 4
EOF
    exit 1
}

# ---- 参数 ----
TYPE="${1:-}"; START="${2:-}"; END="${3:-}"
PARALLEL="${4:-8}"
BASE_URL="${5:-http://localhost:8115/trade-ingress}"

[ -z "$TYPE" ] && usage
[ -z "$START" ] && usage
[ -z "$END" ] && usage

case "$TYPE" in
    order)   TABLE="oms_order_push_log";   ENDPOINT="/order/store-push"   ;;
    payment) TABLE="oms_payment_push_log"; ENDPOINT="/payment/store-push" ;;
    *)       echo "[错误] type 只能为 order 或 payment"; exit 1 ;;
esac

START_SQL="${START/T/ }"
END_SQL="${END/T/ }"
TARGET="${BASE_URL}${ENDPOINT}"
DB_USER="ubuntu"
DB_PASS="D!9dC83d4F7"
DB_NAME="fuioupay-cloud"

echo "============================================"
echo "推流回放测试工具"
echo "============================================"
echo "类型:     $TYPE  ->  $TARGET"
echo "时间范围: $START_SQL  ~  $END_SQL"
echo "并发数:   $PARALLEL"
echo "============================================"

# ---- 1. 查总数 ----
echo -n "统计中... "
TOTAL=$(ssh 81 "mysql -u ${DB_USER} -p'${DB_PASS}' ${DB_NAME} -N -B -e \
  \"SELECT COUNT(*) FROM ${TABLE} \
    WHERE push_receive_time >= '${START_SQL}' \
      AND push_receive_time < '${END_SQL}' \
      AND push_body IS NOT NULL AND push_body != ''\" 2>/dev/null")
echo "${TOTAL} 条"
[ "$TOTAL" -eq 0 ] && { echo "无数据，退出"; exit 0; }

# ---- 2. 导出到临时文件 ----
TMPFILE=$(mktemp -t replay_XXXXXX)
trap "rm -rf $TMPFILE ${TMPFILE}.chunks ${TMPFILE}.results 2>/dev/null" EXIT

echo "导出中..."
ssh 81 "mysql -u ${DB_USER} -p'${DB_PASS}' ${DB_NAME} -N -B --quick \
  -e \"SELECT push_body FROM ${TABLE} \
       WHERE push_receive_time >= '${START_SQL}' \
         AND push_receive_time < '${END_SQL}' \
         AND push_body IS NOT NULL AND push_body != ''\" 2>/dev/null" > "$TMPFILE"
echo "已导出 $(wc -l < "$TMPFILE" | tr -d ' ') 条"

# ---- 3. 分片并行推送 ----
echo "开始推送..."
START_TS=$(date +%s)

CHUNK_DIR="${TMPFILE}.chunks"
mkdir -p "$CHUNK_DIR"
LINES_PER_CHUNK=$(( (TOTAL + PARALLEL - 1) / PARALLEL ))
split -l "$LINES_PER_CHUNK" "$TMPFILE" "${CHUNK_DIR}/chunk_"

RESULT_DIR="${TMPFILE}.results"
mkdir -p "$RESULT_DIR"

for chunk in "${CHUNK_DIR}"/chunk_*; do
    chunk_name=$(basename "$chunk")
    (
        success=0; fail=0
        while IFS= read -r body; do
            [ -z "$body" ] && continue
            http_code=$(curl -s -o /dev/null -w "%{http_code}" \
                -X POST "$TARGET" \
                -H "Content-Type: application/json" \
                -d "$body" 2>/dev/null) || http_code="000"
            if [ "$http_code" = "200" ]; then
                success=$((success + 1))
            else
                fail=$((fail + 1))
            fi
        done < "$chunk"
        echo "$success $fail" > "${RESULT_DIR}/${chunk_name}"
    ) &
done

# ---- 4. 等完成 + 进度显示 ----
while jobs -r | grep -q .; do
    sleep 2
    done_count=0
    for rf in "${RESULT_DIR}"/chunk_*; do
        [ -f "$rf" ] && read s f < "$rf" && done_count=$((done_count + s + f))
    done
    [ "$done_count" -gt 0 ] || continue
    elapsed=$(($(date +%s) - START_TS))
    [ "$elapsed" -eq 0 ] && elapsed=1
    echo "进度: ${done_count}/${TOTAL} ($((done_count * 100 / TOTAL))%) | $((done_count / elapsed)) 条/秒"
done
wait

# ---- 5. 汇总统计 ----
TOTAL_SUCCESS=0; TOTAL_FAIL=0
for rf in "${RESULT_DIR}"/chunk_*; do
    [ -f "$rf" ] && read s f < "$rf" && TOTAL_SUCCESS=$((TOTAL_SUCCESS + s)) && TOTAL_FAIL=$((TOTAL_FAIL + f))
done

ELAPSED=$(($(date +%s) - START_TS))
[ "$ELAPSED" -eq 0 ] && ELAPSED=1

echo "============================================"
echo "完成!"
echo "  成功: ${TOTAL_SUCCESS}  失败: ${TOTAL_FAIL}"
echo "  耗时: ${ELAPSED}s  ($(( (TOTAL_SUCCESS + TOTAL_FAIL) / ELAPSED )) 条/秒)"
echo "============================================"
