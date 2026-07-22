#!/usr/bin/env bash

set -uo pipefail

TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"
HOST="$(hostname -s 2>/dev/null || hostname)"
REPORT="/tmp/server-health-${HOST}-${TIMESTAMP}.log"

# MySQL 可通过环境变量覆盖
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_HOST="${MYSQL_HOST:-}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_SOCKET="${MYSQL_SOCKET:-}"
MYSQL_DEFAULTS_FILE="${MYSQL_DEFAULTS_FILE:-}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"

# Redis 可通过环境变量覆盖
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-0}"
REDIS_SOCKET="${REDIS_SOCKET:-}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"

WARNINGS=()
MYSQL_OK=0
REDIS_OK=0

exec > >(tee "$REPORT") 2>&1

section() {
    echo
    echo "================================================================"
    echo "$1"
    echo "================================================================"
}

run() {
    echo
    echo "\$ $*"
    timeout 30 "$@" 2>&1 || true
}

warn() {
    WARNINGS+=("$1")
}

section "检查基本信息"

echo "报告文件   : $REPORT"
echo "检查时间   : $(date '+%F %T %z')"
echo "主机名     : $(hostname -f 2>/dev/null || hostname)"
echo "当前用户   : $(id)"
echo "运行时长   : $(uptime -p 2>/dev/null || uptime)"
echo "内核版本   : $(uname -a)"

if [[ -r /etc/os-release ]]; then
    cat /etc/os-release
fi

section "系统负载"

run uptime
run nproc
run free -h
run vmstat 1 5

if command -v iostat >/dev/null 2>&1; then
    run iostat -xz 1 3
else
    echo "未安装 iostat，可安装 sysstat：sudo apt install sysstat"
fi

echo
echo "--- CPU 压力 ---"
cat /proc/pressure/cpu 2>/dev/null || true

echo
echo "--- 内存压力 ---"
cat /proc/pressure/memory 2>/dev/null || true

echo
echo "--- IO 压力 ---"
cat /proc/pressure/io 2>/dev/null || true

section "内存和 Swap 评估"

MEM_TOTAL_KB="$(awk '/MemTotal/{print $2}' /proc/meminfo)"
MEM_AVAILABLE_KB="$(awk '/MemAvailable/{print $2}' /proc/meminfo)"
SWAP_TOTAL_KB="$(awk '/SwapTotal/{print $2}' /proc/meminfo)"
SWAP_FREE_KB="$(awk '/SwapFree/{print $2}' /proc/meminfo)"

if [[ "$MEM_TOTAL_KB" -gt 0 ]]; then
    MEM_AVAILABLE_PERCENT=$((MEM_AVAILABLE_KB * 100 / MEM_TOTAL_KB))
    echo "可用内存比例：${MEM_AVAILABLE_PERCENT}%"

    if [[ "$MEM_AVAILABLE_PERCENT" -lt 10 ]]; then
        warn "可用内存低于 10%"
    elif [[ "$MEM_AVAILABLE_PERCENT" -lt 20 ]]; then
        warn "可用内存低于 20%，需要关注"
    fi
fi

if [[ "$SWAP_TOTAL_KB" -gt 0 ]]; then
    SWAP_USED_KB=$((SWAP_TOTAL_KB - SWAP_FREE_KB))
    SWAP_USED_PERCENT=$((SWAP_USED_KB * 100 / SWAP_TOTAL_KB))
    echo "Swap 使用比例：${SWAP_USED_PERCENT}%"

    if [[ "$SWAP_USED_PERCENT" -gt 20 ]]; then
        warn "Swap 使用超过 20%"
    fi
else
    echo "未配置 Swap"
fi

run swapon --show
run sysctl vm.swappiness

section "磁盘和文件系统"

run df -hT
run df -ih
run lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS

while read -r mount usage; do
    usage_number="${usage%\%}"

    if [[ "$usage_number" =~ ^[0-9]+$ ]] && [[ "$usage_number" -ge 85 ]]; then
        warn "磁盘分区 $mount 使用率达到 ${usage}"
    fi
done < <(df -P -x tmpfs -x devtmpfs 2>/dev/null | awk 'NR>1 {print $6, $5}')

section "系统进程"

echo "--- 内存占用最高的进程 ---"
ps aux --sort=-rss | head -20

echo
echo "--- CPU 占用最高的进程 ---"
ps aux --sort=-%cpu | head -20

echo
echo "--- 系统失败服务 ---"
if command -v systemctl >/dev/null 2>&1; then
    systemctl --failed --no-pager 2>&1 || true
fi

section "网络和端口"

run ss -s
run ss -lntp
run ss -ant state established

section "内核异常和 OOM"

if command -v journalctl >/dev/null 2>&1; then
    journalctl -k \
        --since "24 hours ago" \
        --no-pager 2>/dev/null |
        grep -Ei 'oom|out of memory|killed process|segfault|i/o error|ext4.*error|xfs.*error' |
        tail -100 || true
else
    dmesg -T 2>/dev/null |
        grep -Ei 'oom|out of memory|killed process|segfault|i/o error' |
        tail -100 || true
fi

section "Java 进程检查"

JAVA_PIDS="$(pgrep -x java 2>/dev/null || true)"

if [[ -z "$JAVA_PIDS" ]]; then
    echo "没有发现 Java 进程"
    warn "没有发现 Java 进程"
else
    for PID in $JAVA_PIDS; do
        [[ -d "/proc/$PID" ]] || continue

        echo
        echo "----------------------------------------------------------------"
        echo "Java PID: $PID"
        echo "----------------------------------------------------------------"

        echo "启动命令："
        tr '\0' ' ' < "/proc/$PID/cmdline" 2>/dev/null
        echo

        ps -p "$PID" \
            -o pid,ppid,user,%cpu,%mem,rss,vsz,etime,lstart,nlwp,stat,cmd \
            --no-headers 2>/dev/null || true

        echo
        echo "--- 进程状态 ---"
        grep -E \
            '^(Name|State|VmPeak|VmSize|VmRSS|RssAnon|RssFile|VmSwap|Threads):' \
            "/proc/$PID/status" 2>/dev/null || true

        FD_COUNT="$(find "/proc/$PID/fd" -maxdepth 1 -type l 2>/dev/null | wc -l)"
        echo "文件描述符数量：$FD_COUNT"

        echo
        echo "--- 进程限制 ---"
        grep -E \
            'Max open files|Max processes|Max stack size|Max locked memory' \
            "/proc/$PID/limits" 2>/dev/null || true

        echo
        echo "--- Java 网络连接 ---"
        ss -ntp 2>/dev/null | grep "pid=$PID," || true

        if command -v jcmd >/dev/null 2>&1; then
            echo
            echo "--- JVM 版本 ---"
            timeout 15 jcmd "$PID" VM.version 2>&1 || true

            echo
            echo "--- JVM 启动参数 ---"
            timeout 15 jcmd "$PID" VM.command_line 2>&1 || true

            echo
            echo "--- JVM Flags ---"
            timeout 15 jcmd "$PID" VM.flags 2>&1 || true

            echo
            echo "--- JVM 堆状态 ---"
            timeout 15 jcmd "$PID" GC.heap_info 2>&1 || true

            echo
            echo "--- 类加载情况 ---"
            timeout 15 jcmd "$PID" VM.classloader_stats 2>&1 | head -100 || true
        else
            echo "当前环境没有 jcmd，无法读取 JVM 内部状态"
        fi

        if command -v jstat >/dev/null 2>&1; then
            echo
            echo "--- GC 连续采样，间隔1秒，共5次 ---"
            timeout 15 jstat -gcutil "$PID" 1000 5 2>&1 || true
        fi

        if command -v journalctl >/dev/null 2>&1; then
            echo
            echo "--- 最近 Java 进程日志 ---"
            journalctl "_PID=$PID" \
                --since "2 hours ago" \
                -n 100 \
                --no-pager 2>/dev/null || true
        fi
    done
fi

section "MySQL 进程检查"

MYSQL_PIDS="$(pgrep -x mysqld 2>/dev/null || pgrep -x mariadbd 2>/dev/null || true)"

if [[ -z "$MYSQL_PIDS" ]]; then
    echo "没有发现 mysqld/mariadbd 进程"
    warn "没有发现 MySQL 进程"
else
    ps -p "$(echo "$MYSQL_PIDS" | head -1)" \
        -o pid,ppid,user,%cpu,%mem,rss,vsz,etime,lstart,nlwp,stat,cmd 2>/dev/null || true
fi

if command -v systemctl >/dev/null 2>&1; then
    for UNIT in mysql mysqld mariadb; do
        if systemctl list-unit-files "$UNIT.service" \
            --no-legend 2>/dev/null |
            grep -q "$UNIT"; then

            echo
            echo "--- systemctl status $UNIT ---"
            systemctl status "$UNIT" --no-pager -l 2>&1 | head -100 || true
        fi
    done
fi

MYSQL_ARGS=()

if [[ -n "$MYSQL_DEFAULTS_FILE" ]]; then
    MYSQL_ARGS+=("--defaults-extra-file=$MYSQL_DEFAULTS_FILE")
elif [[ -r /etc/mysql/debian.cnf ]]; then
    MYSQL_ARGS+=("--defaults-extra-file=/etc/mysql/debian.cnf")
fi

MYSQL_ARGS+=("--connect-timeout=5")

if [[ -n "$MYSQL_SOCKET" ]]; then
    MYSQL_ARGS+=("-S" "$MYSQL_SOCKET")
elif [[ -n "$MYSQL_HOST" ]]; then
    MYSQL_ARGS+=("-h" "$MYSQL_HOST" "-P" "$MYSQL_PORT")
fi

MYSQL_ARGS+=("-u" "$MYSQL_USER")

if [[ -n "$MYSQL_PASSWORD" ]]; then
    export MYSQL_PWD="$MYSQL_PASSWORD"
fi

mysql_exec() {
    timeout 30 mysql "${MYSQL_ARGS[@]}" -e "$1" 2>&1
}

if command -v mysql >/dev/null 2>&1; then
    echo
    echo "--- MySQL 连通性 ---"

    if mysql_exec "SELECT NOW() AS current_time, VERSION() AS version;" ; then
        MYSQL_OK=1
        echo "MySQL 连接正常"
    else
        warn "MySQL 进程存在，但监控脚本无法连接数据库"
    fi
else
    echo "没有安装 mysql 客户端"
    warn "没有 mysql 客户端，无法检查数据库内部状态"
fi

if [[ "$MYSQL_OK" -eq 1 ]]; then
    section "MySQL 配置"

    mysql_exec "
        SHOW VARIABLES
        WHERE Variable_name IN (
            'innodb_buffer_pool_size',
            'innodb_log_file_size',
            'innodb_log_buffer_size',
            'innodb_flush_log_at_trx_commit',
            'innodb_flush_method',
            'max_connections',
            'table_open_cache',
            'thread_cache_size',
            'tmp_table_size',
            'max_heap_table_size',
            'slow_query_log',
            'long_query_time',
            'log_bin',
            'binlog_format',
            'sync_binlog',
            'server_id'
        );
    " || true

    section "MySQL 运行状态"

    mysql_exec "
        SHOW GLOBAL STATUS
        WHERE Variable_name IN (
            'Uptime',
            'Threads_connected',
            'Threads_running',
            'Threads_cached',
            'Max_used_connections',
            'Connections',
            'Aborted_connects',
            'Aborted_clients',
            'Questions',
            'Queries',
            'Slow_queries',
            'Created_tmp_tables',
            'Created_tmp_disk_tables',
            'Open_tables',
            'Opened_tables',
            'Table_locks_waited',
            'Innodb_buffer_pool_read_requests',
            'Innodb_buffer_pool_reads',
            'Innodb_buffer_pool_pages_total',
            'Innodb_buffer_pool_pages_data',
            'Innodb_buffer_pool_pages_free',
            'Innodb_buffer_pool_wait_free',
            'Innodb_log_waits',
            'Innodb_row_lock_current_waits',
            'Innodb_row_lock_time',
            'Innodb_row_lock_time_max',
            'Innodb_row_lock_waits'
        );
    " || true

    section "MySQL Buffer Pool 命中率"

    mysql_exec "
        SELECT
            ROUND(
                (
                    1 -
                    CAST(r.VARIABLE_VALUE AS DECIMAL(30,6)) /
                    NULLIF(CAST(q.VARIABLE_VALUE AS DECIMAL(30,6)), 0)
                ) * 100,
                4
            ) AS buffer_pool_hit_rate
        FROM performance_schema.global_status r
        JOIN performance_schema.global_status q
          ON r.VARIABLE_NAME = 'Innodb_buffer_pool_reads'
         AND q.VARIABLE_NAME = 'Innodb_buffer_pool_read_requests';
    " || true

    section "MySQL 当前连接"

    mysql_exec "
        SELECT
            ID,
            USER,
            HOST,
            DB,
            COMMAND,
            TIME,
            STATE,
            LEFT(INFO, 200) AS SQL_TEXT
        FROM information_schema.PROCESSLIST
        ORDER BY TIME DESC
        LIMIT 30;
    " || true

    section "MySQL 数据库空间"

    mysql_exec "
        SELECT
            TABLE_SCHEMA,
            ROUND(SUM(DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS size_mb
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA NOT IN (
            'information_schema',
            'performance_schema',
            'mysql',
            'sys'
        )
        GROUP BY TABLE_SCHEMA
        ORDER BY size_mb DESC;
    " || true

    section "MySQL 最大表"

    mysql_exec "
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            TABLE_ROWS,
            ROUND(DATA_LENGTH / 1024 / 1024, 2) AS data_mb,
            ROUND(INDEX_LENGTH / 1024 / 1024, 2) AS index_mb
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA NOT IN (
            'information_schema',
            'performance_schema',
            'mysql',
            'sys'
        )
        ORDER BY DATA_LENGTH + INDEX_LENGTH DESC
        LIMIT 20;
    " || true

    section "InnoDB 状态"

    mysql_exec "SHOW ENGINE INNODB STATUS\G" || true

    section "MySQL 主从状态"

    mysql_exec "SHOW REPLICA STATUS\G" ||
        mysql_exec "SHOW SLAVE STATUS\G" ||
        true

    MYSQL_ERROR_LOG="$(
        mysql "${MYSQL_ARGS[@]}" -N -B \
            -e "SELECT @@global.log_error;" 2>/dev/null |
            head -1
    )"

    if [[ -n "$MYSQL_ERROR_LOG" && "$MYSQL_ERROR_LOG" != "stderr" ]]; then
        echo
        echo "MySQL 错误日志：$MYSQL_ERROR_LOG"

        if [[ -r "$MYSQL_ERROR_LOG" ]]; then
            tail -200 "$MYSQL_ERROR_LOG"
        else
            echo "当前用户无权读取该日志"
        fi
    fi
fi

unset MYSQL_PWD 2>/dev/null || true

section "Redis 进程检查"

REDIS_PIDS="$(pgrep -x redis-server 2>/dev/null || true)"

if [[ -z "$REDIS_PIDS" ]]; then
    echo "没有发现 redis-server 进程"
    warn "没有发现 Redis 进程"
else
    ps -p "$(echo "$REDIS_PIDS" | head -1)" \
        -o pid,ppid,user,%cpu,%mem,rss,vsz,etime,lstart,nlwp,stat,cmd 2>/dev/null || true
fi

if command -v systemctl >/dev/null 2>&1; then
    for UNIT in redis redis-server; do
        if systemctl list-unit-files "$UNIT.service" \
            --no-legend 2>/dev/null |
            grep -q "$UNIT"; then

            echo
            echo "--- systemctl status $UNIT ---"
            systemctl status "$UNIT" --no-pager -l 2>&1 | head -100 || true
        fi
    done
fi

REDIS_ARGS=()

if [[ -n "$REDIS_SOCKET" ]]; then
    REDIS_ARGS+=("-s" "$REDIS_SOCKET")
else
    REDIS_ARGS+=("-h" "$REDIS_HOST" "-p" "$REDIS_PORT")
fi

REDIS_ARGS+=("-n" "$REDIS_DB" "--no-auth-warning")

if [[ -n "$REDIS_PASSWORD" ]]; then
    export REDISCLI_AUTH="$REDIS_PASSWORD"
fi

redis_exec() {
    timeout 30 redis-cli "${REDIS_ARGS[@]}" "$@" 2>&1
}

if command -v redis-cli >/dev/null 2>&1; then
    echo
    echo "--- Redis 连通性 ---"

    REDIS_PING="$(redis_exec PING | tail -1)"

    if [[ "$REDIS_PING" == "PONG" ]]; then
        REDIS_OK=1
        echo "Redis 连接正常"
    else
        echo "$REDIS_PING"
        warn "Redis 进程存在，但监控脚本无法连接 Redis"
    fi
else
    echo "没有安装 redis-cli"
    warn "没有 redis-cli，无法读取 Redis 内部状态"
fi

if [[ "$REDIS_OK" -eq 1 ]]; then
    section "Redis Server"

    redis_exec INFO server

    section "Redis Clients"

    redis_exec INFO clients

    section "Redis Memory"

    redis_exec INFO memory
    redis_exec MEMORY STATS

    section "Redis Stats"

    redis_exec INFO stats
    redis_exec INFO commandstats
    redis_exec INFO errorstats

    section "Redis Persistence"

    redis_exec INFO persistence

    section "Redis Replication"

    redis_exec INFO replication

    section "Redis CPU"

    redis_exec INFO cpu

    section "Redis Keyspace"

    redis_exec INFO keyspace
    redis_exec DBSIZE

    section "Redis 慢日志"

    redis_exec SLOWLOG LEN
    redis_exec SLOWLOG GET 20

    section "Redis Latency"

    redis_exec LATENCY LATEST

    section "Redis 配置摘要"

    redis_exec CONFIG GET maxmemory
    redis_exec CONFIG GET maxmemory-policy
    redis_exec CONFIG GET appendonly
    redis_exec CONFIG GET appendfsync
    redis_exec CONFIG GET save
    redis_exec CONFIG GET timeout
    redis_exec CONFIG GET tcp-keepalive

    EVICTED_KEYS="$(
        redis-cli "${REDIS_ARGS[@]}" INFO stats 2>/dev/null |
        awk -F: '/^evicted_keys:/{gsub("\r","",$2); print $2}'
    )"

    REJECTED_CONNECTIONS="$(
        redis-cli "${REDIS_ARGS[@]}" INFO stats 2>/dev/null |
        awk -F: '/^rejected_connections:/{gsub("\r","",$2); print $2}'
    )"

    if [[ "${EVICTED_KEYS:-0}" =~ ^[0-9]+$ ]] &&
       [[ "${EVICTED_KEYS:-0}" -gt 0 ]]; then
        warn "Redis 已发生 Key 淘汰：evicted_keys=$EVICTED_KEYS"
    fi

    if [[ "${REJECTED_CONNECTIONS:-0}" =~ ^[0-9]+$ ]] &&
       [[ "${REJECTED_CONNECTIONS:-0}" -gt 0 ]]; then
        warn "Redis 已拒绝连接：rejected_connections=$REJECTED_CONNECTIONS"
    fi
fi

unset REDISCLI_AUTH 2>/dev/null || true

section "最近服务日志"

if command -v journalctl >/dev/null 2>&1; then
    for UNIT in mysql mysqld mariadb redis redis-server; do
        if systemctl list-unit-files "$UNIT.service" \
            --no-legend 2>/dev/null |
            grep -q "$UNIT"; then

            echo
            echo "--- $UNIT 最近两小时异常日志 ---"
            journalctl -u "$UNIT" \
                --since "2 hours ago" \
                -p warning \
                -n 100 \
                --no-pager 2>/dev/null || true
        fi
    done
fi

section "检查结论"

if [[ "$MYSQL_OK" -eq 1 ]]; then
    echo "[正常] MySQL 可以连接"
else
    echo "[异常] MySQL 无法完成数据库内部检查"
fi

if [[ "$REDIS_OK" -eq 1 ]]; then
    echo "[正常] Redis 可以连接"
else
    echo "[异常] Redis 无法完成内部检查"
fi

if [[ -n "$JAVA_PIDS" ]]; then
    echo "[正常] 检测到 Java 进程：$(echo "$JAVA_PIDS" | tr '\n' ' ')"
else
    echo "[异常] 没有检测到 Java 进程"
fi

if [[ "${#WARNINGS[@]}" -eq 0 ]]; then
    echo
    echo "没有发现脚本能够直接判定的明显异常。"
else
    echo
    echo "发现以下风险："

    for ITEM in "${WARNINGS[@]}"; do
        echo "- $ITEM"
    done
fi

echo
echo "完整报告：$REPORT"