package com.mtx.trade.ingress.service;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.common.enums.EventAckStatus;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.entity.PaymentEventDO;
import com.mtx.trade.ingress.service.db.OrderEventDbService;
import com.mtx.trade.ingress.service.db.PaymentEventDbService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Objects;

/** Ingress 事件 ACK 服务。 */
@Service
@RequiredArgsConstructor
public class EventAckService {

    private final OrderEventDbService orderEventDbService;
    private final PaymentEventDbService paymentEventDbService;

    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public boolean ack(Integer contentType, Long eventId) {
        if (contentType == null || eventId == null || eventId <= 0) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "contentType 或 eventId 无效");
        }
        if (contentType == ContentType.ORDER.getCode()) {
            return ackOrder(eventId);
        }
        if (contentType == ContentType.PAYMENT.getCode()) {
            return ackPayment(eventId);
        }
        throw new BusinessException(ErrorCode.PARAM_INVALID, "不支持的事件类型");
    }

    private boolean ackOrder(Long eventId) {
        boolean updated = orderEventDbService.update(new LambdaUpdateWrapper<OrderEventDO>()
                .eq(OrderEventDO::getId, eventId)
                .eq(OrderEventDO::getAcked, EventAckStatus.INIT.getCode())
                .set(OrderEventDO::getAcked, EventAckStatus.ACKED.getCode())
                .set(OrderEventDO::getAckedTime, LocalDateTime.now()));
        if (updated) {
            return true;
        }
        ensureOrderAckedOrExists(eventId);
        return false;
    }

    private boolean ackPayment(Long eventId) {
        boolean updated = paymentEventDbService.update(new LambdaUpdateWrapper<PaymentEventDO>()
                .eq(PaymentEventDO::getId, eventId)
                .eq(PaymentEventDO::getAcked, EventAckStatus.INIT.getCode())
                .set(PaymentEventDO::getAcked, EventAckStatus.ACKED.getCode())
                .set(PaymentEventDO::getAckedTime, LocalDateTime.now()));
        if (updated) {
            return true;
        }
        ensurePaymentAckedOrExists(eventId);
        return false;
    }

    private void ensureOrderAckedOrExists(Long eventId) {
        OrderEventDO existing = orderEventDbService.getById(eventId);
        if (existing == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND, "订单事件不存在");
        }
        if (!Objects.equals(existing.getAcked(), EventAckStatus.ACKED.getCode())) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "订单事件 ACK 更新失败");
        }
    }

    private void ensurePaymentAckedOrExists(Long eventId) {
        PaymentEventDO existing = paymentEventDbService.getById(eventId);
        if (existing == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND, "支付事件不存在");
        }
        if (!Objects.equals(existing.getAcked(), EventAckStatus.ACKED.getCode())) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "支付事件 ACK 更新失败");
        }
    }
}
