package com.mtx.trade.receiver.dto;

/** 第三方报文中提取出的事件幂等键和数值版本。 */
public record ParsedEventVersion(String eventKey, long messageVersion) {
}
