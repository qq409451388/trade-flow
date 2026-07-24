"""
富友假数据生成器配置 (亿级支持)
可根据需要修改此文件中的参数
"""

# ============================================================
# Ingress 服务配置
# 注意：trade-ingress 的 application.yml 配置了 server.servlet.context-path=/trade-ingress，
# 因此 BASE_URL 必须带上 /trade-ingress 前缀，否则会 404。
# ============================================================
INGRESS_BASE_URL = "http://localhost:8115/trade-ingress"
ORDER_ENDPOINT = "/order/store-push"
PAYMENT_ENDPOINT = "/payment/store-push"

# 富友验签密钥 (与 application-dev.yml 中 trade.thirdparty.fuiou.secret 一致)
SECRET = "rFXFhj8Z6GA96NQDqgg3N4djE0Dp54nj"

# ============================================================
# 数据量与日期范围配置
# ============================================================
# 默认日期范围 — 用于模拟分表分库（年度分表 + Storage 100 分片）
# 1 亿订单 / 180 天 ≈ 55.5 万/天
START_DATE = "2025-01-01"
END_DATE = "2025-06-30"

# 每日订单量 (与 --total-orders 二选一)
# 如果 --total-orders 指定了总量，则自动按日期天数均摊
ORDERS_PER_DAY = 550000

# ============================================================
# 性能配置 (亿级调优)
# ============================================================
CONCURRENT_WORKERS = 50        # HTTP 并发请求数
REQUEST_TIMEOUT = 30           # 单次请求超时(秒)
BATCH_SIZE = 1000              # 每批生成+发送的订单数 (流式处理，内存占用 = BATCH_SIZE 条)
PROGRESS_SAVE_INTERVAL = 10    # 每 N 批保存一次进度文件
MAX_RETRIES = 3                # 单条请求失败重试次数
RETRY_DELAY = 0.5              # 重试延迟(秒)
RATE_LIMIT_RPS = 0             # 全局限速(条/秒)，0 = 不限速

# ============================================================
# 时间分布 (每天内的订单时间分布权重)
# ============================================================
TIME_DISTRIBUTION = [
    # (开始小时, 结束小时, 权重)
    (7, 11, 0.10),    # 早餐/上午: 7:00-10:59
    (11, 13, 0.35),   # 午餐高峰: 11:00-12:59
    (13, 17, 0.15),   # 下午: 13:00-16:59
    (17, 20, 0.35),   # 晚餐高峰: 17:00-19:59
    (20, 24, 0.05),   # 晚间: 20:00-23:59
]

# ============================================================
# 商户/门店配置
# ============================================================
MERCHANTS = [
    {
        "mchntCd": "MCH001",
        "name": "好吃餐厅旗舰店",
        "shops": [
            {"shopId": 1001, "name": "中山路店", "tmFuiouId": "TM001001", "termName": "A区1号桌"},
            {"shopId": 1002, "name": "人民路店", "tmFuiouId": "TM001002", "termName": "B区3号桌"},
            {"shopId": 1003, "name": "解放路店", "tmFuiouId": "TM001003", "termName": "C区5号桌"},
        ]
    },
    {
        "mchntCd": "MCH002",
        "name": "美味小厨",
        "shops": [
            {"shopId": 2001, "name": "科技园店", "tmFuiouId": "TM002001", "termName": "1号餐桌"},
            {"shopId": 2002, "name": "大学城店", "tmFuiouId": "TM002002", "termName": "2号餐桌"},
        ]
    },
    {
        "mchntCd": "MCH003",
        "name": "老味道家常菜",
        "shops": [
            {"shopId": 3001, "name": "万达广场店", "tmFuiouId": "TM003001", "termName": "窗边1号"},
            {"shopId": 3002, "name": "火车站店", "tmFuiouId": "TM003002", "termName": "大厅2号"},
            {"shopId": 3003, "name": "机场店", "tmFuiouId": "TM003003", "termName": "贵宾区1号"},
        ]
    },
    {
        "mchntCd": "MCH004",
        "name": "蜀味火锅",
        "shops": [
            {"shopId": 4001, "name": "春熙路店", "tmFuiouId": "TM004001", "termName": "大厅A区"},
            {"shopId": 4002, "name": "天府新区店", "tmFuiouId": "TM004002", "termName": "包厢1号"},
        ]
    },
    {
        "mchntCd": "MCH005",
        "name": "粤点茶餐厅",
        "shops": [
            {"shopId": 5001, "name": "环市路店", "tmFuiouId": "TM005001", "termName": "靠窗桌"},
            {"shopId": 5002, "name": "体育西店", "tmFuiouId": "TM005002", "termName": "卡座区"},
            {"shopId": 5003, "name": "珠江新城店", "tmFuiouId": "TM005003", "termName": "露天区"},
            {"shopId": 5004, "name": "佛山店", "tmFuiouId": "TM005004", "termName": "大厅中央"},
        ]
    },
]

# ============================================================
# 订单类型分布
# ============================================================
# "01" = 小程序堂食, "02" = 外卖, "03" = 收银机支付
ORDER_TYPE_DIST = {"01": 0.60, "02": 0.30, "03": 0.10}

# 对应渠道类型
CHANNEL_TYPE_MAP = {"01": "01", "02": "02", "03": "03"}

# 订单状态 (已完成订单为主)
ORDER_STATE_DIST = {"01": 0.90, "02": 0.05, "03": 0.03, "04": 0.02}

# ============================================================
# 支付状态分布
# ============================================================
# 85% 已支付, 10% 部分退款, 5% 已退款
PAY_STATE_DIST = {
    "paid_only": 0.85,         # 只有支付，无退款
    "partial_refund": 0.10,    # 支付 + 部分退款
    "full_refund": 0.05,       # 支付 + 全额退款
}

# ============================================================
# 商品目录 (名称 -> 单价，单位：分)
# ============================================================
PRODUCT_CATALOG = [
    # 热菜
    {"goodsId": 10001, "name": "红烧肉", "price": 4800, "unit": "份", "category": "热菜"},
    {"goodsId": 10002, "name": "宫保鸡丁", "price": 3800, "unit": "份", "category": "热菜"},
    {"goodsId": 10003, "name": "鱼香肉丝", "price": 3200, "unit": "份", "category": "热菜"},
    {"goodsId": 10004, "name": "麻婆豆腐", "price": 2200, "unit": "份", "category": "热菜"},
    {"goodsId": 10005, "name": "糖醋排骨", "price": 5800, "unit": "份", "category": "热菜"},
    {"goodsId": 10006, "name": "干煸四季豆", "price": 2600, "unit": "份", "category": "热菜"},
    {"goodsId": 10007, "name": "回锅肉", "price": 3500, "unit": "份", "category": "热菜"},
    {"goodsId": 10008, "name": "番茄炒蛋", "price": 1800, "unit": "份", "category": "热菜"},
    {"goodsId": 10009, "name": "酸菜鱼", "price": 6800, "unit": "份", "category": "热菜"},
    {"goodsId": 10010, "name": "水煮牛肉", "price": 5500, "unit": "份", "category": "热菜"},
    # 凉菜
    {"goodsId": 20001, "name": "凉拌黄瓜", "price": 1200, "unit": "份", "category": "凉菜"},
    {"goodsId": 20002, "name": "皮蛋豆腐", "price": 1500, "unit": "份", "category": "凉菜"},
    {"goodsId": 20003, "name": "口水鸡", "price": 3200, "unit": "份", "category": "凉菜"},
    # 主食
    {"goodsId": 30001, "name": "白米饭", "price": 200, "unit": "碗", "category": "主食"},
    {"goodsId": 30002, "name": "蛋炒饭", "price": 1500, "unit": "份", "category": "主食"},
    {"goodsId": 30003, "name": "牛肉面", "price": 2200, "unit": "碗", "category": "主食"},
    {"goodsId": 30004, "name": "馒头", "price": 100, "unit": "个", "category": "主食"},
    # 饮品
    {"goodsId": 40001, "name": "可口可乐", "price": 500, "unit": "罐", "category": "饮品"},
    {"goodsId": 40002, "name": "冰红茶", "price": 600, "unit": "瓶", "category": "饮品"},
    {"goodsId": 40003, "name": "青岛啤酒", "price": 1200, "unit": "瓶", "category": "饮品"},
    {"goodsId": 40004, "name": "矿泉水", "price": 300, "unit": "瓶", "category": "饮品"},
    # 火锅 (蜀味火锅专用)
    {"goodsId": 50001, "name": "毛肚", "price": 4800, "unit": "份", "category": "火锅"},
    {"goodsId": 50002, "name": "鸭肠", "price": 3200, "unit": "份", "category": "火锅"},
    {"goodsId": 50003, "name": "肥牛卷", "price": 4500, "unit": "份", "category": "火锅"},
    {"goodsId": 50004, "name": "虾滑", "price": 3800, "unit": "份", "category": "火锅"},
    # 茶餐厅
    {"goodsId": 60001, "name": "虾饺皇", "price": 2800, "unit": "笼", "category": "茶点"},
    {"goodsId": 60002, "name": "叉烧包", "price": 1800, "unit": "笼", "category": "茶点"},
    {"goodsId": 60003, "name": "肠粉", "price": 1500, "unit": "份", "category": "茶点"},
    {"goodsId": 60004, "name": "凤爪", "price": 2200, "unit": "笼", "category": "茶点"},
]

# 每单商品数量分布
ITEM_COUNT_DIST = [
    (1, 0.10),
    (2, 0.25),
    (3, 0.25),
    (4, 0.20),
    (5, 0.10),
    (6, 0.05),
    (7, 0.03),
    (8, 0.02),
]

# ============================================================
# 支付方式
# ============================================================
PAY_METHODS = [
    {"payType": "WX", "payName": "微信支付"},
    {"payType": "ALI", "payName": "支付宝"},
    {"payType": "UNP", "payName": "银联支付"},
    {"payType": "CASH", "payName": "现金"},
    {"payType": "CARD", "payName": "银行卡"},
]

# ============================================================
# 用户模拟数据
# ============================================================
COMMON_SURNAMES = ["张", "李", "王", "赵", "陈", "杨", "黄", "周", "吴", "郑",
                   "刘", "林", "孙", "朱", "马", "胡", "郭", "何", "高", "罗"]
COMMON_PHONE_PREFIXES = ["138", "139", "150", "151", "152", "186", "187", "188", "130", "131"]

# ============================================================
# 配送配置 (外卖订单)
# ============================================================
EXPRESS_COMPANIES = ["01", "02"]  # 01=达达, 02=自配送
EXPRESS_FEE_RANGE = (300, 800)    # 配送费 3-8 元 (分)
