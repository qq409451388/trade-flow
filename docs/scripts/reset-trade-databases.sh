#!/usr/bin/env bash
# 删除并按 docs/sql 最终脚本重建本机 Trade MySQL。
set -Eeuo pipefail
umask 077

usage() {
    cat <<'EOF'
用法:
  ./reset-trade-databases.sh --execute

默认连接:
  host=127.0.0.1 port=3306 user=root

可选环境变量:
  TRADE_DB_HOST       MySQL 地址，默认 127.0.0.1
  TRADE_DB_PORT       MySQL 端口，默认 3306
  TRADE_DB_USER       MySQL 用户，默认 root
  TRADE_DB_PASSWORD   MySQL 密码，默认空
  TRADE_DB_CONFIRM    设置为 YES 可跳过交互确认

脚本会 DROP 并重新创建以下数据库：
  trade_flow
  trade_pipeline

警告：全部本地数据都会删除且不能恢复。
EOF
    exit 1
}

fail() {
    echo "[错误] $*" >&2
    exit 1
}

[[ "${1:-}" == "--execute" ]] || usage

DB_HOST="${TRADE_DB_HOST:-127.0.0.1}"
DB_PORT="${TRADE_DB_PORT:-3306}"
DB_USER="${TRADE_DB_USER:-root}"
DB_PASSWORD="${TRADE_DB_PASSWORD:-}"

case "$DB_HOST" in
    127.0.0.1|localhost|::1) ;;
    *) fail "为防止误清线上库，只允许连接本机 MySQL" ;;
esac
[[ "$DB_PORT" =~ ^[0-9]+$ ]] && (( DB_PORT >= 1 && DB_PORT <= 65535 )) \
    || fail "TRADE_DB_PORT 无效"
command -v mysql >/dev/null 2>&1 || fail "缺少 mysql 客户端"

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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SQL_DIR="$(cd "$SCRIPT_DIR/../sql" && pwd)"

echo "即将清空本机 MySQL：$DB_HOST:$DB_PORT"
echo "数据库：trade_flow、trade_pipeline"
echo "请先停止 trade-ingress 和 trade-pipeline。"

if [[ "${TRADE_DB_CONFIRM:-}" != "YES" ]]; then
    read -r -p "输入 REBUILD trade_flow trade_pipeline 继续: " confirmation
    [[ "$confirmation" == "REBUILD trade_flow trade_pipeline" ]] || fail "已取消"
fi
run_mysql --execute="DROP DATABASE IF EXISTS trade_flow; DROP DATABASE IF EXISTS trade_pipeline;"
run_mysql < "$SQL_DIR/trade-databases.sql"
run_mysql trade_flow < "$SQL_DIR/trade-flow-base-schema.sql"
run_mysql trade_flow < "$SQL_DIR/trade-storage-shards.sql"
run_mysql trade_pipeline < "$SQL_DIR/trade-pipeline-base-schema.sql"
run_mysql trade_pipeline < "$SQL_DIR/trade-pipeline-year-shards.sql"
echo "完成：已按 docs/sql 最终脚本重建 trade_flow 和 trade_pipeline。"
echo "提示：Redis 未清理；开始完整重放前请单独检查 database 5 的 Stream。"
