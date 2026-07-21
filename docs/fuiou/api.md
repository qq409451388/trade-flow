
### 订单推送接口
#### 请求地址
需用户提供推送地址，富友这边配置。<br>
商户号，商户名称，接收推送的地址，并说明下是配置“订单推送”回调地址；
验签所需秘钥可一并申请，富友给到。<br>
发邮件到service_saas@fuioupay.com

#### 请求报文
| 序号| 参数名| 含义| 类型| 备注| 
| :-----| :-------| :-----| :-----| :-----| 
|1|orderNo|订单号|Long|
|2|pushType|推送类型|String|1.支付成功推送，3.退款推送
|3|userId|用户id|Long|
|4|mchntCd|商户号|String|
|5|shopId|门店ID|Long|
|6|expressId|快递单id|Long|
|7|tmFuiouId|桌贴号(终端号)|String|
|8|termName|桌贴名称(终端别名)|String|
|9|orderType|订单类型|String|<br>01小程序堂食</br><br>02外卖</br><br>03收银机支付</br>
|10|orderAmt|订单金额(分) |Long|商品打折前的价格，只做记录，没有任何实际意义
|11|orderDisAmt|订单金额(分) |Long|订单原单金额，不包含快递费，优惠券，积分优惠金额，手动打折金额
|12|payAmt|实付金额|Long|实际支付金额(分)=商品折扣后价格+快递费-优惠金额（包含收银员手动整单打折金额、优惠券优惠金额、积分优惠金额、会员等级优惠金额、会员价折扣优惠金额、会员日折扣优惠金额等）
|13|cashierDisAmt|收银员手输打折价(分)|Long|收银员手输整单打折金额
|14|paySsn|支付流水号|String|
|15|appOpenId|用户openId|String|
|16|mealCode|取餐码|String|001-999循环
|17|crtTm|订单创建时间|时间戳(毫秒)|
|18|payDeadlineTm|最晚支付时间|时间戳(毫秒)|默认下单成功后15分钟内，超过15分钟未支付自动关单
|19|payTm|订单支付时间|时间戳(毫秒)|
|20|cashierConfirmTm|收银员确认时间|时间戳(毫秒)|
|21|deliverStartTm|开始配送时间|时间戳(毫秒)|
|22|commentTm|评价时间|时间戳(毫秒)|
|23|finshTm|订单完成时间|时间戳(毫秒)|状态变成05的时间，用户收货时间(快递)/收银员完成时间(堂食)
|24|recUpdTm|订单最近完成时间|时间戳(毫秒)|
|25|payType|支付方式|String|<br>LETPAY 小程序支付</br><br>YFK 会员卡余额支付</br><br>WECHAT 微信被扫</br><br>JSAPI 微信主扫</br><br>ALIPAY 支付宝被扫</br><br>FWC 支付宝主扫</br><br>CASH 现金</br><br>TC 次卡支付</br><br>SMILEPAY 支付宝扫脸</br><br>UNIONPAY 云闪付</br><br>BRUSHCARD 银行卡支付</br><br>account 挂账</br><br>BESTPAY 翼支付</br><br>APPLEPAY Apple 支付</br><br>YQK 园区卡支付</br><br>GC 汽油卡支付</br><br>DC 柴油卡支付</br><br>NC 天然气卡支付</br>
|26|payTypeExtra|额外支付方式|String|<br>LETPAY 小程序支付</br><br>YFK 会员卡余额支付</br><br>WECHAT 微信被扫</br><br>JSAPI 微信主扫</br><br>ALIPAY 支付宝被扫</br><br>FWC 支付宝主扫</br><br>CASH 现金</br><br>TC 次卡支付</br><br>SMILEPAY 支付宝扫脸</br><br>UNIONPAY 云闪付</br><br>BRUSHCARD 银行卡支付</br><br>account 挂账</br><br>BESTPAY 翼支付</br><br>APPLEPAY Apple 支付</br><br>YQK 园区卡支付</br><br>GC 汽油卡支付</br><br>DC 柴油卡支付</br><br>NC 天然气卡支付</br><br>可能为空</br>
|27|payAmtExtra|额外支付金额|Long|
|28|userMemo|用户备注|String|
|29|couponId|优惠券id|Long|
|30|integral|抵扣积分数|Long|
|31|integralDeductionAmt|积分优惠金额|Long|
|32|couponAmt|优惠券优惠金额|Long|
|33|expressAmt|快递费|Long|
|34|fullMinusAmt|满减优惠金额|Long|
|35|discountType|优惠类型|String|<br>00 无优惠</br><br>01积分优惠</br><br>02优惠券优惠</br>
|36|orderState|订单状态|String|01 已创建</br><br>02 已支付待商户接单</br><br>03 已接单待配送</br><br>04 配送中</br><br>05 已收货待评价</br><br>06 已评价</br><br>00 订单已取消</br><br>77 (离线)订单支付待确认</br><br>88 预下单</br><br>99 已退款</br>
|37|expressState|快递状态|String|<br>0未处理</br><br>1:待快递员接单</br><br>2:快递员已接单待取货</br><br>3:快递员配送中</br><br>4:已完成</br><br>9:快递异常</br>
|38|orderComment|订单评价|String|
|39|orderCancelReason|订单取消原因|String|
|40|commentState|评价状态|String|0未评价  1已评
|41|commentLevel|评价等级|String|01一星   02二星 03三星  04四星  05五星
|42|orderAddrId|用户收货地址id|Long|
|43|phone|收货人手机号|String|
|44|mchntExpressCost|达达实收快递费|Long|
|45|cashierId|收银员账号|String|
|46|channelType|渠道类型|String|<br>00：扫码点餐</br><br>01:收银机</br><br>02:饿了么外卖</br><br>03：美团商家版外卖</br><br>04：美团聚宝盆外卖</br><br>05：三方小程序</br><br>09：抖音外卖</br><br>12：台卡订单</br><br>13：抖音小程序</br><br>14：澳觅外卖</br><br>15：抖音小时达</br><br>16：京东外卖</br><br>17：美团闪购外卖</br><br>18：keeta外卖</br><br>19：饿了么零售外卖</br>
|47|isMembership|是否是会员|String|0否   1是
|48|cashReceivedAmt|现金实收金额(分)|Long|
|49|couponRealId|优惠券真实id|Long|
|50|finshDate|订单完成日期|String|yyyy-MM-dd
|51|cashierDiscount|收银员手动整单打折折扣|decimal(11,2)|7折填70
|52|singleGoodsDisAmt|收银员单品打折价(分)|Long|
|53|cashierDisId|收银员手动折扣id|Long|
|54|cashierDisName|收银员手动折扣名|String|
|55|refundAmt|退款金额|Long|
|56|refundTm|退款时间|时间戳(毫秒)|
|57|thirdOrderNo|第三方订单号|String|
|58|fullOrderDisAmt|收银员手动整单打折金额(分)|Long|整单手动打折后价格(cashierDisAmt)-订单原单金额(orderDisAmt)-收银员单品打折金额(singleGoodsDisAmt)
|59|openTableTm|开台时间|时间戳(毫秒)|
|60|mealConfirmChannel|重餐确认订单渠道|String|<br>00：扫码点餐</br><br>01:收银机<br>
|61|tableFuiouId|就餐桌终端号|String|
|62|tableTermName|就餐桌桌贴名称|String|
|63|isMealOrder|是否为重餐订单|String|1：是，0：否 此字段用在重餐版本中
|64|platHongBao|第三方平台红包|Long|
|65|deliverTm|期望送达时间|String|第三方订单用
|66|orderVersion|订单版本号|Longt|
|67|notInDiscountAmt|不参与打折金额(分)|Long|
|68|thirdMchntIncome|第三方外卖商家预计收入|Long|
|69|isOrderLocked|订单是否已锁定|String|1:是，0否 此字段用在重餐版本中
|70|isPrintSettleList|订单是否已打印结算单|String|1:是，0否 此字段用在重餐版本中
|71|expressCompany|快递配送公司|String|01：达达，02：自配送
|72|contactMobile|联系人手机号码|String|
|73|mqttSendState|mqtt推送状态|String|<br>0:未推送</br><br>1：已推送未应答</br><br>2：已应答</br>
|74|mealTm|就餐时间|时间戳(毫秒)|
|75|memberLevelDisAmt|会员等级优惠金额|Long|
|76|memberPriceDisAmt|会员价折扣优惠金额|Long|
|77|invoiceState|开票状态|String|<br>0:未开票</br><br>1:开票成功</br><br>8:开票中</br><br>9:开票失败</br>
|78|invoiceAmt|开票金额|Long|
|79|wipeZeroAmt|抹零金额(分)|Long|如果1.7元收2元，此处填-30，如果1.7元收1元，此处填70
|80|packagePriceDisAmt|套餐折扣优惠金额|Long|
|81|groupPayDisAmt|团购券优惠金额|Long|
|82|groupPayNum|团购券支付使用数量|Long|
|83|groupPayNumExtra|额外团购券支付使用数量|Long|
|84|maybeFinshTm|预计完成时间|时间戳(毫秒)|
|85|hasReverse|是否曾经反结账|String|0:否,1:是
|86|reverseTm|反结账时间|时间戳(毫秒)|
|87|orderPayState|支付状态|String|<br>1：已支付</br><br>8：部分退款</br><br>9：已退款</br>
|88|discountTypeExtra|额外优惠类型|String|<br>00:无优惠</br><br>01:积分优惠</br><br>02:优惠券优惠</br><br>03:会员等级优惠</br><br>04:会员折扣优惠</br>
|89|promoterNo|推广员编号|String|
|90|isPadConfirm|是否为pad确认下单|String|1:是,0:否
|91|accountMemo|挂账备注|String|
|92|isAccountOrder|是否为挂账订单|String|1：是，0：否 此字段用在挂账订单中
|93|thirdIsBaskets|是否为多篮子|String|0不多篮，1多篮
|94|lunchBoxFee|商品餐盒费(分)|Long|
|95|memberPhone|会员手机号|String|
|96|memberPoints|会员所需积分|Long|商品会员价配置了所需积分时使用
|97|userBalance|用户当前订单消费后的账户余额(分)|Long|
|98|unionpayDisAmt|银联(优惠券)优惠金额|Long|
|99|timesCardDisAmt|次卡优惠金额|Long|
|100|memberName|会员名称|String|
|101|specialCouponId|特殊商户优惠券Id|String|
|102|specialMchntType|特殊商户类型:01,非码商户|String|
|103|memberDayDisAmt|会员日折扣优惠金额|Long|
|104|outUserId|校园刷脸学生outuserid|String|
|105|freeConsumeAmt|会员卡赠送账户消费金额(分)|String|
|106|fixedPriceAmt|固价商品所占扫码支付总金额(分)|String|
|107|keySign|签名|String|
|108|guestsCount|就餐人数|String|
|109|`**orderDetailInfos**`|订单详情列表|List|字段见下表
| |**` orderDetailInfos 内字段名`**|含义| 类型| 备注| 
|1|shopId|门店id|Long|
|2|mchntCd|商户代码|String|
|3|goodsBasePrice|商品基础单价(分)|Long|
|4|goodsDisPrice|商品折扣单价(分)|Long|
|5|goodsPrice|商品总单价(分)|Long|商品折扣价格(分)+规格加价(分)
|6|goodsNumber|数量|decimal(14,3)|
|7|goodsName|商品名称|String|
|8|crtTm|订单创建时间|时间戳(毫秒)|
|9|markPrint|打印标示|String|0不打印，1打印
|10|goodsPayAmt|商品实际支付金额(分)|Long|(商品折扣单价*订单数量)*(订单实际支付金额/订单总金额)
|11|specIdList|规格id列表|String|
|12|specDescList|规格描述列表|String|
|13|goodsCashierDiscount|收银员手动整单打折折扣|decimal(11,2)|7折填70
|14|cashierDisPrice|收银员手动单品打折后单价(分)|Long|如果未打折，此处=goods_price
|15|isThirdOrder|是否为第三方订单|String|0否，1是
|16|addDishChannel|重餐加菜渠道|String|00：扫码点餐，01:收银机
|17|dishUpdTm|更新时间|时间戳(毫秒)|
|18|dishState|菜品状态|String|1:正常,0:已取消 2:延后上菜
|19|disDiscountReason|菜品打折原因|String|
|20|dishUserMemo|菜品用户备注|String|
|21|dishCashierMemo|菜品收银员备注|String|
|22|dishCancelReason|菜品取消原因|String|
|23|dishFuiouId|点餐终端号|String|
|24|dishHasHurried|是否被催菜|String|0否，1是
|25|promotionId|商品促销活动参数id|Long|
|26|goodsPromotionWay|商品参与促销的方式|String|1：促销的发起方 2：促销的优惠方；比如买A送B，A为促销的发起方填1，B为优惠方填2
|27|goodsUnit|库存单位|String|
|28|goodsMemberPrice|商品总会员价(分)|Long|商品表里的会员价(分)+规格加价(分)
|29|goodsMemberDisAmt|会员价打折优惠金额(分)|Long|不打折的情况下=0
|30|goodsDisType|单品打折类型|String|0未打折,1手动打折,2会员价打折
|31|avgPurchasePrice|平均进货价|Long|
|32|isPackageGoods|是否为套餐商品|String|1：是，0：不是
|33|dishPrintState|是否打印厨打|String|1：是，0否
|34|inPkgGooodsPrice|套餐内商品原价(分)|Long|套餐内商品原价(goodsPrice)*套餐内商品数量(packageNumber)
|35|isDishConfirm|菜品是否已确认|String|1：是,0：否
|36|isOprByPad|是否为pad操作的菜品|String|1是,0否
|37|isWeighGoods|是否称重商品|String|1：是 0：否
|38|dishHasFinish|是否已上菜|String|0否，1是
|39|dishUserId|点餐用户id|Long|
|40|dishCashierId|收银员账号|String|
|41|goodsBarCode|商品条码|String|
|42|goodsBasket|所属篮子|String|默认为空
|43|goodsLunchBoxFee|商品餐盒费(分)|Long|
|44|prePackageedAmt|预包装商品总金额(分)|Long|计算模式(1:总价/数量 ,2:总价/单价)时使用(分)
|45|calcModel|计算模式|Long|0:数量*单价 ,1:总价/数量  ,2:总价/单价
|46|kitchenPrint|后厨单打印标示|String|0不打印，1打印
|47|printSerialNo|指定打印机序列号|String|
|48|goodsMemberPoints|商品会员价所需积分|Long|
|49|unionpayCouponId|银联单品券id|String|
|50|goodsRealPayAmt|商品真实支付金额(分)|Long|goodsPayAmt/Sum(goodsPayAmt)*payAmt
|51|goodsSalesCommission|商品销售提成|Long|0-100
|52|goodsEmployeeCommission|商品人员提成|Long|指定雇员取技工指定提成，未指定取技工销售提成， 0-100
|53|detailNo|订单详情编号|Long|
|54|orderNo|订单号|Long|
|55|goodsId|商品id|Long|
|56|goodsTotalRefundAmt|商品退款金额（分）|String
|57|`**orderGoodsSpecs**`|规格详情列表|List|字段见下表
| |**` orderGoodsSpecs 内字段名`**|含义| 类型| 备注| 
|1|orderSpecId|订单规格详情编号|Long|
|2|specId|规格id|Long|
|3|specName|规格中文名称|String|
|4|specDetailId|规格详情id|Long|
|5|specDetailDesc|规格详情中文名称|String|
|6|detailExtraPrice|规格额外加价金额（分）|Long|
|58|`**packageDetailList**`|套餐商品|List|字段见下表
| |**` orderGoodsSpecs 内字段名`**|含义| 类型| 备注| 
|1|packageDetailNo|订单详情编号|Long|
|2|relatePkgDetailNo|关联详情编号|Long|
|3|orderNo|订单号|Long|
|4|goodsId|商品id|Long|
|5|goodsMemberDisAmt|商品会员优惠金额(分)=|Long|会员价打折的情况下，商品会员优惠金额(分)=商品总单价(元)-商品总会员价(元)，不打折的情况下=0
|6|shopId|门店id|Long|
|7|mchntCd|商户代码|String|
|8|goodsBasePrice|商品基础单价(分)|Long|
|9|goodsDisPrice|商品折扣单价(分)|Long|
|10|goodsMemberPrice|商品会员价(分)|Long|
|11|goodsCashierDiscount|收银员手动打折折扣，7折填70|Double|
|12|cashierDisPrice|收银员手动打折后金额(分)|Long|
|13|goodsPrice|订单总单价(分)|Long|订单总单价(分)=商品折扣价格(分)+规格加价(分)
|14|goodsNumber|订单商品数量|Double|订单商品数量=单份套餐里配的商品数量(package_number)*套餐购买数
|15|packageNumber|单份套餐里配的商品数量|Double|
|16|goodsName|商品名称|String|
|17|markPrint|打印标示|String|0不打印，1打印
|18|goodsPayAmt|商品实际支付金额(分)|Long|商品实际支付金额(分)=(商品折扣单价*订单数量)*(订单实际支付金额/订单总金额)
|19|specIdList|规格id列表|String|
|20|specDescList|规格描述列表|String|
|21|crtTm|创建时间|Long|时间戳
|22|goodsUnit|商品单位|String|
|23|avgPurchasePrice|平均进货价|Double|
|24|goodsIndex|商品序列化，用来对套餐内商品排序|Int|
|25|isWeighGoods|是否称重商品|String| 1：是 0：否
|26|goodsBarCode|商品条码|String|
|27|goodsLunchBoxFee|商品的餐盒费(分)|Long|
|28|goodsMemberPoints|商品会员所需积分(分)|Long|
|29|pkgGoodsExtraPrice|套餐可选商品加价(分)|Long|
|30|pkgGoodsCopies|单份套餐里选择的商品份数|Double|
|31|thirdDetailNo|第三方详情编号|String|
|32|packageDishMemo|菜品收银员备注|String|



#### 响应报文
| 序号| 参数名| 含义| 描述| 
| :-----| :-------| :-----| :-----| 
|1|resultCode|响应码|000000成功，其他失败
|2|resultMsg|响应描述|成功



### 支付推送接口
#### 请求地址
需用户提供推送地址，富友这边配置。<br>
商户号，商户名称，接收推送的地址，并说明下是配置“订单支付推送接口”回调地址；
验签所需秘钥可一并申请，富友给到。<br>
发邮件到service_saas@fuioupay.com<br>
!>**`支付推送接口不是实时推，会延迟1个小时推！`**

#### 请求报文
| 序号| 参数名| 含义| 类型| 备注| 
| :-----| :-------| :-----| :-----| :-----| 
|1|mchntCd|商户号|String|
|2|shopId|门店ID|Long|
|3|shopName|门店名称|String|
|4|orderNo|订单号|Long|
|5|paySsn|交易流水号|String|
|6|payTm|支付时间|String|
|7|refundTm|退款时间|String|
|8|payType|支付方式|String|<br>LETPAY 小程序支付</br><br>YFK 会员卡余额支付</br><br>WECHAT 微信被扫</br><br>JSAPI 微信主扫</br><br>ALIPAY 支付宝被扫</br><br>FWC 支付宝主扫</br><br>CASH 现金</br><br>TC 次卡支付</br><br>SMILEPAY 支付宝扫脸</br><br>UNIONPAY 云闪付</br><br>BRUSHCARD 银行卡支付</br><br>account 挂账</br><br>BESTPAY 翼支付</br><br>APPLEPAY Apple 支付</br><br>YQK 园区卡支付</br><br>GC 汽油卡支付</br><br>DC 柴油卡支付</br><br>NC 天然气卡支付</br>
|9|payName|支付方式名称|String|
|10|payAmt|支付金额|Long|
|11|feeAmt|手续费|Long|
|12|refundAmt|退款金额|Long|
|13|payState|支付状态|String|1:支付成功  2：退款成功
|14|payMsg|支付信息|String|支付成功/退款成功
|15|orderSource|订单来源|String|<br>支付单来源，默认00</br><br>00：收银机SAAS订单</br><br>01：小程序SAAS订单</br><br>02：收银机充值</br><br>03：台卡小额储值</br><br>04：C端在线充值</br><br>05：小程序小额充值</br><br>06：小程序会员中心充值</br><br>07：收银机购买等级</br><br>08：小程序购买等级</br><br>09：小程序购买次卡/油卡</br><br>10：C端购买优惠券</br><br>11：小程序购买优惠券</br><br>12：C端盲盒购买</br><br>13：美团</br><br>14：饿了么</br><br>18：其他第三方</br><br>19：单笔余额调账</br><br>20：批量余额调账</br><br>21：批量充值</br><br>22：C端购买等级</br><br>23：收银机购买次卡</br><br>24：分账订单</br>
|16|fySettle|富友清算|Boolean|
|17|sourcePaySsn|退款时，原支付流水号|String|
|18|thirdOrderNo|第三方订单号|String|
|19|channelTradeNo|渠道订单号|String|
|20|bigCategory|业务大类|String|01:营业收款  02:会员充值  03:卡券礼包购买 04:其他
|21|smallCategory|业务小类|String|<br>小类要根据大类进行判断</br><br>营业收款</br>01:小程序 02:收银机 03:自营外卖 04:美团外卖 05:饿了么外卖 06:抖音外卖<br>会员充值</br>01:收银机充值 02:调账 03:会员自主充值 04:小额充值<br>卡券礼包购买</br>01:会员等级购买 02:优惠券购买 03:盲盒购买 04:次卡购买<br>其他</br> 01:总部代收款项
|22|memberName|会员名称|String|
|23|phone|会员手机号|String|
|24|memberCardNo|会员实体卡号|String|
|25|memberLevel|会员等级值|String|会员等级值为1 - 20，1表示等级1
|26|keySign|签名|String|
|27|balanceDisAmt|赠送账户优惠金额（分）|Long|
|28|faceAmt|券面金额|Long|三方团购券订单的券面值
|29|`**acntNoInfoList**`|结算信息列表|字段见下表
| |**` acntNoInfoList 内字段名`**|`字段含义`| `描述`|  
|1|custAcntTp|账户性质|String|G：对公  S：对私
|2|mchntCd|商户号|String|
|3|outAcntNm|户名|String|
|4|outAcntNo|卡号|String|加密卡号，需解密（解密密钥联系对接人获取）
|5|shopId|门店ID|String|

     


#### 响应报文
| 序号| 参数名| 含义| 描述| 
| :-----| :-------| :-----| :-----| 
|1|resultCode|响应码|000000成功，其他失败
|2|resultMsg|响应描述|成功