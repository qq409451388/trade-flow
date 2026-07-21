package com.mtx.trade.ingress.dto;

/** Ingress 事件人工补发命令。 */
public record EventRedeliveryCommand(Integer contentType, Long eventId) {
}
