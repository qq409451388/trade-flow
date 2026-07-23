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
import java.util.List;
import java.util.LinkedHashSet;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mtx.trade.ingress.dto.EventBatchAckResult;

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

    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public EventBatchAckResult batchAck(Integer contentType, List<Long> eventIds) {
        if (contentType == null || eventIds == null || eventIds.isEmpty() || eventIds.size() > 500) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "contentType无效或eventIds必须为1~500条");
        }
        List<Long> ids = new LinkedHashSet<>(eventIds).stream().toList();
        if (ids.stream().anyMatch(id -> id == null || id <= 0)) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "eventIds包含无效ID");
        }
        LocalDateTime now = LocalDateTime.now();
        if (contentType == ContentType.ORDER.getCode()) {
            List<OrderEventDO> existing = orderEventDbService.list(new LambdaQueryWrapper<OrderEventDO>()
                    .in(OrderEventDO::getId, ids));
            long already = existing.stream().filter(e -> Objects.equals(e.getAcked(), EventAckStatus.ACKED.getCode())).count();
            orderEventDbService.update(new LambdaUpdateWrapper<OrderEventDO>()
                    .in(OrderEventDO::getId, ids).eq(OrderEventDO::getAcked, EventAckStatus.INIT.getCode())
                    .set(OrderEventDO::getAcked, EventAckStatus.ACKED.getCode()).set(OrderEventDO::getAckedTime, now));
            return result(ids.size(), existing.size(), already);
        }
        if (contentType == ContentType.PAYMENT.getCode()) {
            List<PaymentEventDO> existing = paymentEventDbService.list(new LambdaQueryWrapper<PaymentEventDO>()
                    .in(PaymentEventDO::getId, ids));
            long already = existing.stream().filter(e -> Objects.equals(e.getAcked(), EventAckStatus.ACKED.getCode())).count();
            paymentEventDbService.update(new LambdaUpdateWrapper<PaymentEventDO>()
                    .in(PaymentEventDO::getId, ids).eq(PaymentEventDO::getAcked, EventAckStatus.INIT.getCode())
                    .set(PaymentEventDO::getAcked, EventAckStatus.ACKED.getCode()).set(PaymentEventDO::getAckedTime, now));
            return result(ids.size(), existing.size(), already);
        }
        throw new BusinessException(ErrorCode.PARAM_INVALID, "不支持的事件类型");
    }

    private static EventBatchAckResult result(int requested, int existing, long already) {
        return new EventBatchAckResult(requested, existing - (int) already, (int) already, requested - existing);
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
