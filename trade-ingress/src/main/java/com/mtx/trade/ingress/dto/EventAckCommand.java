package com.mtx.trade.ingress.dto;

/** Pipeline 接管事件后的 ACK 命令。 */
public record EventAckCommand(Integer contentType, Long eventId) {
}
