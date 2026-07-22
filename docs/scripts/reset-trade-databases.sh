#!/usr/bin/env bash
# 清空本机 Trade MySQL 数据，保留表结构并恢复必要种子记录。
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

脚本会 TRUNCATE 以下数据库中的全部 BASE TABLE：
  trade_flow
  trade_pipeline

警告：TRUNCATE 会立即提交，不能回滚。
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

SCHEMA_COUNT="$(run_mysql --execute="SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name IN ('trade_flow', 'trade_pipeline')")"
[[ "$SCHEMA_COUNT" == "2" ]] \
    || fail "未同时找到 trade_flow 和 trade_pipeline，请先执行 docs/sql 下的建库脚本"

TRUNCATE_SQL="$(run_mysql --execute="
SELECT CONCAT('TRUNCATE TABLE ', table_schema, '.', table_name, ';')
FROM information_schema.tables
WHERE table_schema IN ('trade_flow', 'trade_pipeline')
  AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name")"

[[ -n "$TRUNCATE_SQL" ]] || fail "两个 Trade 数据库中没有可清理的表"
TABLE_COUNT="$(printf '%s\n' "$TRUNCATE_SQL" | wc -l | tr -d ' ')"

echo "即将清空本机 MySQL：$DB_HOST:$DB_PORT"
echo "数据库：trade_flow、trade_pipeline"
echo "表数量：$TABLE_COUNT"
echo "请先停止 trade-ingress 和 trade-pipeline。"

if [[ "${TRADE_DB_CONFIRM:-}" != "YES" ]]; then
    read -r -p "输入 TRUNCATE trade_flow trade_pipeline 继续: " confirmation
    [[ "$confirmation" == "TRUNCATE trade_flow trade_pipeline" ]] || fail "已取消"
fi

PLAN_FILE="$(mktemp "${TMPDIR:-/tmp}/trade-reset.XXXXXX.sql")"
trap 'rm -f "$PLAN_FILE"' EXIT INT TERM

{
    echo 'SET FOREIGN_KEY_CHECKS = 0;'
    printf '%s\n' "$TRUNCATE_SQL"
    echo 'SET FOREIGN_KEY_CHECKS = 1;'
    cat <<'SQL'
INSERT INTO `trade_flow`.`trade_event_delivery_control`
  (`content_type`, `circuit_status`, `failure_count`, `health_success_count`,
   `recovery_cursor_id`, `recovery_cutoff_id`, `version`)
VALUES
  (1, 0, 0, 0, 0, 0, 0),
  (2, 0, 0, 0, 0, 0, 0);

INSERT INTO `trade_pipeline`.`pipeline_event_pull_control` (`content_type`)
VALUES (1), (2);
SQL
} > "$PLAN_FILE"

run_mysql < "$PLAN_FILE"
echo "完成：已清空 $TABLE_COUNT 张表，并恢复事件投递及主动拉取控制记录。"
echo "提示：Redis 未清理；开始完整重放前请单独检查 database 5 的 Stream。"
