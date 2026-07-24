#!/usr/bin/env python3
"""
富友假数据生成与发送脚本 (亿级支持)

核心改进：
1. 流式处理：生成一批 → 签名 → 发送 → 丢弃，内存占用恒定 (O(batch_size))
2. 日期范围：支持 --start-date / --end-date，模拟年度分表 + Storage 100 分片
3. 断点续传：进度写入 progress.json，中断后 --resume 可跳过已完成日期/批次
4. 失败重试：单条请求自动重试，失败记录写入 errors.jsonl
5. 全局限速：可选 --rate-limit 防止压垮服务端

用法：
  # 生成 1 亿订单，分布在 2025-01-01 ~ 2025-06-30 (约 55 万/天)
  python3 main.py --total-orders 100000000 --start-date 2025-01-01 --end-date 2025-06-30

  # 指定每日订单量 (总量 = 每日量 × 天数)
  python3 main.py --orders-per-day 500000 --start-date 2025-01-01 --end-date 2025-06-30

  # 小批量测试 (单日 1000 条)
  python3 main.py --total-orders 1000 --start-date 2026-07-24 --end-date 2026-07-24

  # 仅生成 JSONL 文件 (不发送，适合小批量验证数据格式)
  python3 main.py --total-orders 1000 --start-date 2026-07-24 --end-date 2026-07-24 --only-generate

  # 断点续传
  python3 main.py --total-orders 100000000 --start-date 2025-01-01 --end-date 2025-06-30 --resume

依赖：pip3 install aiohttp --break-system-packages
"""

import asyncio
import hashlib
import json
import os
import random
import re
import sys
import time
import tempfile
from datetime import datetime, timedelta
from typing import Any, Optional

import aiohttp

from config import *

# ============================================================
# 日期工具
# ============================================================

def generate_date_list(start_date: str, end_date: str) -> list[str]:
    """生成日期列表 [start, end] (含两端)"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    if end < start:
        raise ValueError(f"end-date {end_date} 早于 start-date {start_date}")
    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return dates


def date_to_int(date_str: str) -> int:
    """日期字符串 -> 整数: '2025-01-01' -> 20250101"""
    return int(datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d"))


# ============================================================
# 通用工具函数
# ============================================================

def weighted_choice(items: list[tuple[Any, float]]) -> Any:
    """按权重随机选择"""
    total = sum(w for _, w in items)
    r = random.random() * total
    cumulative = 0
    for item, weight in items:
        cumulative += weight
        if r < cumulative:
            return item
    return items[-1][0]


def random_ts_in_range(date_str: str, start_hour: int, end_hour: int) -> int:
    """在指定日期的小时范围内生成随机毫秒时间戳"""
    date = datetime.strptime(date_str, "%Y-%m-%d")
    start = date.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    end = date.replace(hour=end_hour - 1, minute=59, second=59, microsecond=999000)
    delta = (end - start).total_seconds()
    random_seconds = random.uniform(0, delta)
    dt = start + timedelta(seconds=random_seconds)
    return int(dt.timestamp() * 1000)


def ms_to_datetime_str(ts_ms: int, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """毫秒时间戳 -> 日期时间字符串"""
    return datetime.fromtimestamp(ts_ms / 1000).strftime(fmt)


def generate_order_no(date_str: str, daily_seq: int) -> int:
    """
    生成唯一订单号: date_int * 10_000_000 + daily_seq
    支持每天最多 1000 万单，远超实际需求
    """
    return date_to_int(date_str) * 10_000_000 + daily_seq


def generate_pay_ssn(order_no: int, seq: int = 0) -> str:
    """生成支付流水号"""
    return f"PAY{order_no}{seq:02d}"


# ============================================================
# 签名
# ============================================================

def remove_key_sign_field(json_str: str) -> tuple[str, str]:
    """
    从 JSON 字符串中提取 keySign 值并移除 keySign 字段
    与 Java 端 FuiouSignUtils.removeKeySignField 逻辑一致
    """
    m = re.search(r'"keySign"\s*:\s*"([0-9a-fA-F]{32})"', json_str)
    key_sign = m.group(1) if m else ""
    result = re.sub(r',\s*"keySign"\s*:\s*"[0-9a-fA-F]{32}"', '', json_str)
    result = re.sub(r'"keySign"\s*:\s*"[0-9a-fA-F]{32}"\s*,', '', result)
    return result, key_sign


def sign_json(json_dict: dict) -> str:
    """给 JSON dict 添加正确签名并返回完整 JSON 字符串"""
    json_dict["keySign"] = "00000000000000000000000000000000"
    json_str = json.dumps(json_dict, ensure_ascii=False, separators=(',', ':'))
    body_no_sign, _ = remove_key_sign_field(json_str)
    sign = hashlib.md5((SECRET + body_no_sign).encode("utf-8")).hexdigest()
    json_dict["keySign"] = sign
    return json.dumps(json_dict, ensure_ascii=False, separators=(',', ':'))


# ============================================================
# 数据生成：订单 + 支付 (一体生成，无全局状态)
# ============================================================

def generate_order_and_payments(
    current_date: str, daily_seq: int
) -> tuple[Optional[dict], list[dict]]:
    """
    生成一条订单及其关联的支付/退款记录。
    返回 (order_dict, [payment_dict, ...])
    """
    # -- 选择商户和门店 --
    merchant = random.choice(MERCHANTS)
    shop = random.choice(merchant["shops"])

    # -- 基本属性 --
    order_no = generate_order_no(current_date, daily_seq)
    order_type = weighted_choice([(k, v) for k, v in ORDER_TYPE_DIST.items()])
    channel_type = CHANNEL_TYPE_MAP.get(order_type, "01")
    order_state = weighted_choice([(k, v) for k, v in ORDER_STATE_DIST.items()])

    # -- 时间线 --
    hour_range = weighted_choice([((s, e), w) for s, e, w in TIME_DISTRIBUTION])
    order_create_ts = random_ts_in_range(current_date, hour_range[0], hour_range[1])

    pay_ts = order_create_ts + random.randint(60_000, 1_800_000)
    if order_type == "02":
        finish_ts = pay_ts + random.randint(1_200_000, 5_400_000)
    else:
        finish_ts = pay_ts + random.randint(300_000, 3_600_000)
    source_update_ts = max(finish_ts, pay_ts) + random.randint(0, 60_000)

    # -- 商品明细 --
    item_count = weighted_choice(ITEM_COUNT_DIST)
    selected_items = random.choices(PRODUCT_CATALOG, k=min(item_count, len(PRODUCT_CATALOG)))
    if len(selected_items) < item_count:
        extra = random.choices(PRODUCT_CATALOG, k=item_count - len(selected_items))
        selected_items.extend(extra)

    if order_type in ("01", "03") and random.random() < 0.6:
        rice = next((p for p in PRODUCT_CATALOG if p["goodsId"] == 30001), None)
        if rice and rice not in selected_items:
            selected_items.append(rice)

    order_amt = 0
    order_detail_infos = []
    for idx, product in enumerate(selected_items):
        quantity = max(1, int(random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]))
        item_base_price = product["price"]
        item_total_price = item_base_price * quantity

        detail_no = order_no * 100 + idx + 1
        item = {
            "detailNo": detail_no,
            "orderNo": order_no,
            "shopId": shop["shopId"],
            "mchntCd": merchant["mchntCd"],
            "goodsId": product["goodsId"],
            "goodsName": product["name"],
            "goodsUnit": product["unit"],
            "goodsBasePrice": item_base_price,
            "goodsDisPrice": item_base_price,
            "goodsPrice": item_total_price,
            "goodsNumber": float(quantity),
            "goodsPayAmt": item_total_price,
            "goodsRealPayAmt": item_total_price,
            "crtTm": order_create_ts,
            "dishUpdTm": order_create_ts,
            "dishState": 1,
            "isDishConfirm": 1,
            "dishHasFinish": 1,
        }
        order_detail_infos.append(item)
        order_amt += item_total_price

    # 外卖额外费用
    express_amt = 0
    lunch_box_fee = 0
    express_company = ""
    if order_type == "02":
        express_amt = random.randint(*EXPRESS_FEE_RANGE)
        express_company = random.choice(EXPRESS_COMPANIES)
        lunch_box_fee = random.randint(0, 200) * len(selected_items)

    order_dis_amt = order_amt
    pay_amt = order_amt + express_amt + lunch_box_fee

    # -- 用户信息 --
    surname = random.choice(COMMON_SURNAMES)
    phone_prefix = random.choice(COMMON_PHONE_PREFIXES)
    phone = phone_prefix + "".join([str(random.randint(0, 9)) for _ in range(8)])
    user_id = random.randint(100000, 999999)
    member_name = surname + random.choice(["小明", "小红", "大伟", "丽丽", "建国", "秀英", "志强", "美玲"])

    # -- 构建订单 JSON --
    order = {
        "keySign": "00000000000000000000000000000000",
        "orderNo": order_no,
        "mchntCd": merchant["mchntCd"],
        "crtTm": order_create_ts,
        "recUpdTm": source_update_ts,
        "payDeadlineTm": order_create_ts + 900_000,
        "payTm": pay_ts,
        "finshTm": finish_ts,
        "orderDetailInfos": order_detail_infos,
        "cashierConfirmTm": pay_ts - random.randint(10_000, 120_000) if order_type == "03" else None,
        "deliverStartTm": pay_ts + random.randint(60_000, 300_000) if order_type == "02" else None,
        "commentTm": finish_ts + random.randint(3_600_000, 86_400_000) if random.random() < 0.3 else None,
        "refundTm": None,
        "openTableTm": order_create_ts - random.randint(300_000, 1_800_000) if order_type == "01" else None,
        "mealTm": order_create_ts + random.randint(120_000, 600_000) if order_type == "01" else None,
        "maybeFinshTm": finish_ts + random.randint(300_000, 1_800_000),
        "reverseTm": None,
        "deliverTm": str(source_update_ts) if order_type == "02" else "",
        "shopId": shop["shopId"],
        "userId": user_id,
        "tmFuiouId": shop["tmFuiouId"],
        "termName": shop["termName"],
        "orderType": order_type,
        "channelType": channel_type,
        "orderState": order_state,
        "orderPayState": 1,
        "expressState": 0,
        "orderAmt": order_amt,
        "orderDisAmt": order_dis_amt,
        "payAmt": pay_amt,
        "payAmtExtra": 0,
        "cashReceivedAmt": pay_amt if random.random() < 0.05 else 0,
        "refundAmt": 0,
        "expressAmt": express_amt,
        "mchntExpressCost": int(express_amt * 0.85) if express_amt else 0,
        "lunchBoxFee": lunch_box_fee,
        "invoiceAmt": 0,
        "thirdMchntIncome": int(pay_amt * 0.7) if order_type == "02" else 0,
        "paySsn": "",
        "payType": "",
        "payTypeExtra": "",
        "thirdOrderNo": f"EXT{order_no}" if order_type == "02" else "",
        "appOpenId": f"openid_{user_id}",
        "outUserId": str(user_id),
        "mealCode": f"{random.randint(100, 999)}" if order_type == "01" else "",
        "userMemo": random.choice(["少放辣", "不要香菜", "多放醋", "", ""]),
        "orderCancelReason": "",
        "orderComment": "",
        "commentState": 0,
        "commentLevel": "",
        "phone": phone,
        "contactMobile": phone,
        "orderAddrId": random.randint(1000, 9999) if order_type == "02" else None,
        "expressCompany": express_company,
        "expressId": random.randint(100000, 999999) if order_type == "02" else None,
        "cashierId": f"CSH{random.randint(100, 999)}" if order_type == "03" else "",
        "cashierDisAmt": 0,
        "singleGoodsDisAmt": 0,
        "fullOrderDisAmt": 0,
        "cashierDiscount": None,
        "cashierDisId": None,
        "cashierDisName": "",
        "discountType": "",
        "discountTypeExtra": "",
        "couponId": None,
        "couponRealId": None,
        "specialCouponId": "",
        "specialMchntType": "",
        "integral": 0,
        "integralDeductionAmt": 0,
        "couponAmt": 0,
        "fullMinusAmt": 0,
        "memberLevelDisAmt": 0,
        "memberPriceDisAmt": 0,
        "memberDayDisAmt": 0,
        "unionpayDisAmt": 0,
        "timesCardDisAmt": 0,
        "packagePriceDisAmt": 0,
        "groupPayDisAmt": 0,
        "platHongBao": 0,
        "wipeZeroAmt": 0,
        "notInDiscountAmt": 0,
        "freeConsumeAmt": 0,
        "fixedPriceAmt": 0,
        "groupPayNum": 0,
        "groupPayNumExtra": 0,
        "isMembership": 1 if random.random() < 0.3 else 0,
        "isMealOrder": 0,
        "isOrderLocked": 0,
        "hasReverse": 0,
        "isPadConfirm": 0,
        "isAccountOrder": 0,
        "printSettleStatus": 0,
        "thirdBasketStatus": 0,
        "invoiceState": 0,
        "mqttSendState": 0,
        "mealConfirmChannel": "",
        "tableFuiouId": shop["tmFuiouId"] if order_type == "01" else "",
        "tableTermName": shop["termName"] if order_type == "01" else "",
        "promoterNo": "",
        "accountMemo": "",
        "memberPhone": phone,
        "memberName": member_name,
        "memberPoints": random.randint(0, 5000),
        "userBalance": random.randint(0, 100000),
        "guestsCount": random.randint(1, 8),
        "orderVersion": random.randint(0, 5),
    }

    order = {k: v for k, v in order.items() if v is not None}

    # -- 支付关联信息 (局部变量，不存全局 map) --
    order_info = {
        "payAmt": pay_amt,
        "orderCreateTime": ms_to_datetime_str(order_create_ts),
        "payTime": ms_to_datetime_str(pay_ts),
        "mchntCd": merchant["mchntCd"],
        "shopId": shop["shopId"],
        "shopName": shop["name"],
        "orderType": order_type,
        "channelType": channel_type,
        "phone": phone,
        "memberName": member_name,
    }

    # -- 决定支付策略并生成支付记录 --
    fate = weighted_choice([(k, v) for k, v in PAY_STATE_DIST.items()])
    payments = []

    # 正常支付
    pay = _generate_payment(order_no, order_info, "pay", 0)
    if pay:
        payments.append(pay)

    # 退款
    if fate in ("partial_refund", "full_refund"):
        refund = _generate_payment(order_no, order_info, "refund", 1, fate)
        if refund:
            payments.append(refund)

    return order, payments


def _generate_payment(
    order_no: int,
    order_info: dict,
    pay_type: str,
    seq: int,
    fate: str = "paid_only",
) -> Optional[dict]:
    """生成单条支付/退款 JSON"""
    pay_ssn = generate_pay_ssn(order_no, seq)
    pay_method = random.choice(PAY_METHODS)
    pay_tm = order_info["payTime"]

    if pay_type == "pay":
        pay_state = 1
        pay_amt = order_info["payAmt"]
        refund_amt = 0
        refund_tm = None
        source_pay_ssn = ""
    elif pay_type == "refund":
        pay_state = 2
        source_pay_ssn = generate_pay_ssn(order_no, 0)
        pay_tm_ts = int(datetime.strptime(pay_tm, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        refund_tm = ms_to_datetime_str(pay_tm_ts + random.randint(600_000, 86_400_000))
        pay_amt = order_info["payAmt"]
        if fate == "full_refund":
            refund_amt = order_info["payAmt"]
        else:
            refund_amt = random.randint(int(pay_amt * 0.1), int(pay_amt * 0.8))
    else:
        return None

    payment = {
        "keySign": "00000000000000000000000000000000",
        "paySsn": pay_ssn,
        "sourcePaySsn": source_pay_ssn,
        "orderNo": order_no,
        "mchntCd": order_info["mchntCd"],
        "shopId": order_info["shopId"],
        "shopName": order_info["shopName"],
        "payTm": pay_tm,
        "refundTm": refund_tm,
        "payType": pay_method["payType"],
        "payName": pay_method["payName"],
        "payState": pay_state,
        "payAmt": pay_amt,
        "feeAmt": 0,
        "refundAmt": refund_amt,
        "balanceDisAmt": 0,
        "faceAmt": 0,
        "fySettle": 1,
        "thirdOrderNo": f"EXT{order_no}",
        "channelTradeNo": f"CH{order_no}{random.randint(10, 99)}",
        "bigCategory": "01",
        "smallCategory": "01",
        "orderSource": "01",
        "openId": f"openid_{random.randint(100000, 999999)}",
        "orderType": order_info["orderType"],
        "channelType": order_info["channelType"],
        "memberName": order_info["memberName"],
        "phone": order_info["phone"],
        "memberCardNo": "",
        "memberLevel": str(random.choice([1, 2, 3, 4, 5])),
        "acntNoInfoList": [
            {
                "custAcntTp": random.choice(["G", "S"]),
                "mchntCd": order_info["mchntCd"],
                "shopId": order_info["shopId"],
                "outAcntNm": f"{order_info['shopName']}结算账户",
                "outAcntNo": f"6222{''.join([str(random.randint(0, 9)) for _ in range(12)])}",
            }
        ],
    }

    payment = {k: v for k, v in payment.items() if v is not None}
    return payment


# ============================================================
# HTTP 发送 (带重试)
# ============================================================

async def send_payload(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    endpoint: str,
    payload_str: str,
    max_retries: int = MAX_RETRIES,
) -> tuple[bool, str]:
    """发送一条 JSON，带重试。返回 (是否成功, 错误信息)"""
    url = f"{INGRESS_BASE_URL}{endpoint}"
    async with semaphore:
        for attempt in range(max_retries + 1):
            try:
                async with session.post(
                    url,
                    data=payload_str,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                ) as resp:
                    body = await resp.text()
                    if resp.status == 200:
                        return True, ""
                    if attempt < max_retries:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    return False, f"HTTP {resp.status}: {body[:200]}"
            except asyncio.TimeoutError:
                if attempt < max_retries:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                return False, f"Timeout after {REQUEST_TIMEOUT}s"
            except aiohttp.ClientError as e:
                if attempt < max_retries:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                return False, str(e)[:200]
        return False, "Exhausted retries"


async def send_batch(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    endpoint: str,
    payloads: list[str],
    label: str,
    error_file=None,
) -> dict:
    """批量发送 JSON，返回统计信息"""
    stats = {"success": 0, "failed": 0, "total": len(payloads)}
    if not payloads:
        return stats

    tasks = [
        send_payload(session, semaphore, endpoint, p)
        for p in payloads
    ]
    results = await asyncio.gather(*tasks)

    for ok, msg in results:
        if ok:
            stats["success"] += 1
        else:
            stats["failed"] += 1
            if error_file and stats["failed"] <= 100:
                error_file.write(f'{{"label":"{label}","error":"{msg}"}}\n')

    return stats


# ============================================================
# 进度管理 (断点续传)
# ============================================================

def load_progress(progress_file: str) -> dict:
    """加载进度文件"""
    if os.path.exists(progress_file):
        try:
            with open(progress_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def save_progress(progress_file: str, progress: dict):
    """原子写入进度文件 (临时文件 + rename)"""
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=os.path.dirname(os.path.abspath(progress_file)) or ".",
        suffix=".tmp",
    )
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(progress, f, indent=2)
        os.replace(tmp_path, progress_file)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


# ============================================================
# CLI 参数解析
# ============================================================

def parse_args():
    import argparse
    parser = argparse.ArgumentParser(
        description="富友假数据生成与发送 (亿级支持)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 1 亿订单，2025-01-01 ~ 2025-06-30
  python3 main.py --total-orders 100000000 --start-date 2025-01-01 --end-date 2025-06-30

  # 指定每日 50 万，跨年测试分表
  python3 main.py --orders-per-day 500000 --start-date 2024-12-15 --end-date 2025-01-15

  # 小批量验证
  python3 main.py --total-orders 100 --start-date 2026-07-24 --end-date 2026-07-24

  # 断点续传
  python3 main.py --total-orders 100000000 --start-date 2025-01-01 --end-date 2025-06-30 --resume
        """,
    )
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--total-orders", type=int, default=None,
                   help=f"订单总量 (默认按 ORDERS_PER_DAY={ORDERS_PER_DAY} × 天数计算)")
    g.add_argument("--orders-per-day", type=int, default=ORDERS_PER_DAY,
                   help=f"每日订单量 (默认: {ORDERS_PER_DAY})")

    parser.add_argument("--start-date", type=str, default=START_DATE,
                        help=f"起始日期 (默认: {START_DATE})")
    parser.add_argument("--end-date", type=str, default=END_DATE,
                        help=f"结束日期 (默认: {END_DATE})")
    parser.add_argument("--concurrency", type=int, default=CONCURRENT_WORKERS,
                        help=f"HTTP 并发数 (默认: {CONCURRENT_WORKERS})")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"每批订单数 (默认: {BATCH_SIZE})")
    parser.add_argument("--rate-limit", type=int, default=RATE_LIMIT_RPS,
                        help=f"全局限速 条/秒 (默认: {RATE_LIMIT_RPS}, 0=不限速)")
    parser.add_argument("--only-generate", action="store_true",
                        help="仅生成 JSONL 文件，不发送 HTTP")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="JSONL 输出目录 (仅 --only-generate 模式)")
    parser.add_argument("--resume", action="store_true",
                        help="从 progress.json 断点续传")
    parser.add_argument("--progress-file", type=str, default="progress.json",
                        help="进度文件路径 (默认: progress.json)")
    parser.add_argument("--error-file", type=str, default="errors.jsonl",
                        help="错误记录文件路径 (默认: errors.jsonl)")
    return parser.parse_args()


# ============================================================
# 主流程 (流式)
# ============================================================

async def main():
    args = parse_args()

    # -- 计算日期范围和每日订单量 --
    dates = generate_date_list(args.start_date, args.end_date)
    num_days = len(dates)

    if args.total_orders is not None:
        orders_per_day = args.total_orders // num_days
        remainder = args.total_orders - orders_per_day * num_days
        total_orders = args.total_orders
    else:
        orders_per_day = args.orders_per_day
        total_orders = orders_per_day * num_days
        remainder = 0

    # -- 加载进度 --
    progress = {}
    if args.resume:
        progress = load_progress(args.progress_file)
        if progress:
            print(f"  [Resume] 加载进度: 已完成 {progress.get('total_sent', 0):,} 条")
        else:
            print(f"  [Resume] 未找到进度文件，从头开始")

    # 已完成的日期集合
    completed_dates: set[str] = set(progress.get("completed_dates", []))
    resume_date = progress.get("current_date")
    resume_daily_seq = progress.get("daily_seq", 0)

    # -- 打印概要 --
    print(f"\n{'='*60}")
    print(f"  富友假数据生成器 (亿级)")
    print(f"  日期范围: {dates[0]} ~ {dates[-1]}  ({num_days} 天)")
    print(f"  每日订单: {orders_per_day:,}")
    print(f"  总订单量: {total_orders:,}")
    print(f"  并发数:   {args.concurrency}")
    print(f"  批大小:   {args.batch_size}")
    if args.rate_limit > 0:
        print(f"  限速:     {args.rate_limit} 条/秒")
    if args.only_generate:
        print(f"  模式:     仅生成 JSONL (不发送)")
    elif args.resume:
        print(f"  模式:     断点续传")
    print(f"{'='*60}\n")

    # ============================================================
    # 仅生成模式
    # ============================================================
    if args.only_generate:
        output_dir = args.output_dir or f"./fake_data_{dates[0]}_{dates[-1]}"
        os.makedirs(output_dir, exist_ok=True)
        order_file = os.path.join(output_dir, "orders.jsonl")
        pay_file = os.path.join(output_dir, "payments.jsonl")

        if total_orders > 1_000_000:
            print(f"  [警告] 生成 {total_orders:,} 条到磁盘可能占用大量空间 (>200GB)")
            resp = input("  继续? (y/n): ")
            if resp.lower() != 'y':
                print("  已取消")
                return

        print(f"[Generate] 输出目录: {output_dir}")
        t_start = time.time()
        total_generated = 0

        with open(order_file, "w", encoding="utf-8") as fo, \
             open(pay_file, "w", encoding="utf-8") as fp:
            for date in dates:
                daily_count = orders_per_day + (1 if date == dates[0] and remainder else 0)
                for seq in range(1, daily_count + 1):
                    order, payments = generate_order_and_payments(date, seq)
                    if order:
                        fo.write(sign_json(order) + "\n")
                    for p in payments:
                        fp.write(sign_json(p) + "\n")
                    total_generated += 1
                    if total_generated % 10000 == 0:
                        elapsed = time.time() - t_start
                        rate = total_generated / max(1, elapsed)
                        print(f"  生成: {total_generated:,}/{total_orders:,} "
                              f"({total_generated/total_orders*100:.1f}%) "
                              f"| {rate:.0f} 条/s")

        elapsed = time.time() - t_start
        print(f"\n  生成完成: {total_generated:,} 条, 耗时 {elapsed:.1f}s")
        print(f"  订单文件: {order_file}")
        print(f"  支付文件: {pay_file}")
        return

    # ============================================================
    # 流式生成 + 发送
    # ============================================================
    semaphore = asyncio.Semaphore(args.concurrency)
    connector = aiohttp.TCPConnector(
        limit=args.concurrency * 2,
        limit_per_host=args.concurrency * 2,
        force_close=False,
        keepalive_timeout=60,
    )

    # 全局统计 (断点续传时从进度恢复)
    total_order_sent = progress.get("total_order_sent", 0) if args.resume else 0
    total_order_fail = progress.get("total_order_fail", 0) if args.resume else 0
    total_pay_sent = progress.get("total_pay_sent", 0) if args.resume else 0
    total_pay_fail = progress.get("total_pay_fail", 0) if args.resume else 0
    t_start = time.time()
    batch_count = 0
    # 本 session 已发送量 (用于限速计算，不含 resume 恢复的历史计数)
    session_sent = 0
    session_start = time.time()

    error_file = open(args.error_file, "a", encoding="utf-8")

    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            for di, date in enumerate(dates):
                # 跳过已完成的日期
                if date in completed_dates:
                    print(f"  [Skip] {date} 已完成，跳过")
                    continue

                # 当日订单量 (前 remainder 天各多 1 条，保证总量精确)
                daily_target = orders_per_day + (1 if di < remainder else 0)

                # 断点续传：跳过当日已完成的序号
                start_seq = 1
                if args.resume and date == resume_date and resume_daily_seq > 0:
                    start_seq = resume_daily_seq + 1
                    print(f"  [Resume] {date} 从序号 {start_seq} 继续 (已完成 {resume_daily_seq:,})")
                else:
                    print(f"\n  [{di+1}/{num_days}] {date} — 目标 {daily_target:,} 单")

                seq = start_seq
                while seq <= daily_target:
                    batch_size = min(args.batch_size, daily_target - seq + 1)

                    # -- 生成批次 --
                    order_payloads = []
                    payment_payloads = []
                    for _ in range(batch_size):
                        order, payments = generate_order_and_payments(date, seq)
                        if order:
                            order_payloads.append(sign_json(order))
                        for p in payments:
                            payment_payloads.append(sign_json(p))
                        seq += 1

                    # -- 发送订单 --
                    stat_o = await send_batch(
                        session, semaphore, ORDER_ENDPOINT,
                        order_payloads, "ORDER", error_file,
                    )
                    total_order_sent += stat_o["success"]
                    total_order_fail += stat_o["failed"]

                    # -- 发送支付 --
                    stat_p = await send_batch(
                        session, semaphore, PAYMENT_ENDPOINT,
                        payment_payloads, "PAYMENT", error_file,
                    )
                    total_pay_sent += stat_p["success"]
                    total_pay_fail += stat_p["failed"]
                    session_sent += stat_o["success"] + stat_p["success"]

                    batch_count += 1

                    # -- 进度打印 --
                    elapsed = time.time() - t_start
                    total_sent = total_order_sent + total_pay_sent
                    total_target = total_orders + int(total_orders * 0.15)  # 粗估含支付
                    rate = total_order_sent / max(1, elapsed)
                    eta_sec = (total_orders - total_order_sent) / max(1, rate) if rate > 0 else 0
                    eta_str = str(timedelta(seconds=int(eta_sec)))

                    daily_done = seq - 1
                    print(
                        f"    {date} | 日进度: {daily_done:,}/{daily_target:,} "
                        f"| 总订单: {total_order_sent:,}/{total_orders:,} "
                        f"| 失败: O={total_order_fail} P={total_pay_fail} "
                        f"| {rate:.0f}单/s | ETA: {eta_str}"
                    )

                    # -- 限速 (基于本 session 已发送量) --
                    if args.rate_limit > 0:
                        session_elapsed = time.time() - session_start
                        expected_time = session_sent / args.rate_limit
                        if session_elapsed < expected_time:
                            await asyncio.sleep(expected_time - session_elapsed)

                    # -- 保存进度 --
                    if batch_count % PROGRESS_SAVE_INTERVAL == 0:
                        save_progress(args.progress_file, {
                            "current_date": date,
                            "daily_seq": daily_done,
                            "completed_dates": list(completed_dates),
                            "total_sent": total_sent,
                            "total_order_sent": total_order_sent,
                            "total_pay_sent": total_pay_sent,
                            "total_order_fail": total_order_fail,
                            "total_pay_fail": total_pay_fail,
                            "last_save": datetime.now().isoformat(),
                        })

                # 当日完成
                completed_dates.add(date)
                save_progress(args.progress_file, {
                    "current_date": date,
                    "daily_seq": daily_target,
                    "completed_dates": list(completed_dates),
                    "total_sent": total_order_sent + total_pay_sent,
                    "total_order_sent": total_order_sent,
                    "total_pay_sent": total_pay_sent,
                    "total_order_fail": total_order_fail,
                    "total_pay_fail": total_pay_fail,
                    "last_save": datetime.now().isoformat(),
                })
                print(f"  ✓ {date} 完成 ({daily_target:,} 单)")

    except KeyboardInterrupt:
        print(f"\n\n  [中断] 保存进度中...")
        save_progress(args.progress_file, {
            "current_date": date if 'date' in dir() else dates[0],
            "daily_seq": daily_done if 'daily_done' in dir() else 0,
            "completed_dates": list(completed_dates),
            "total_sent": total_order_sent + total_pay_sent,
            "total_order_sent": total_order_sent,
            "total_pay_sent": total_pay_sent,
            "total_order_fail": total_order_fail,
            "total_pay_fail": total_pay_fail,
            "last_save": datetime.now().isoformat(),
        })
        print(f"  进度已保存至 {args.progress_file}，可用 --resume 继续")
    finally:
        error_file.close()

    # ============================================================
    # 最终汇总
    # ============================================================
    t_total = time.time() - t_start
    total_sent = total_order_sent + total_pay_sent
    total_fail = total_order_fail + total_pay_fail

    print(f"\n{'='*60}")
    print(f"  执行完成")
    print(f"  总耗时:     {t_total:.1f}s ({timedelta(seconds=int(t_total))})")
    print(f"  订单: 成功 {total_order_sent:,} / 失败 {total_order_fail}")
    print(f"  支付: 成功 {total_pay_sent:,} / 失败 {total_pay_fail}")
    print(f"  合计: 成功 {total_sent:,} / 失败 {total_fail}")
    print(f"  整体速率: {total_order_sent/max(1, t_total):.0f} 单/s")
    if total_fail > 0:
        print(f"  错误详情: {args.error_file}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
