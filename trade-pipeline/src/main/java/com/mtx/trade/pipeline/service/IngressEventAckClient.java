package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.config.OrderEventConsumerProperties;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

/** Pipeline 持久化完成后通知 Ingress 停止补发事件。 */
@Service
public class IngressEventAckClient {

    private final RestClient restClient;
    private final OrderEventConsumerProperties properties;

    public IngressEventAckClient(
        RestClient.Builder restClientBuilder,
            OrderEventConsumerProperties properties) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(properties.getIngressAckConnectTimeout());
        requestFactory.setReadTimeout(properties.getIngressAckReadTimeout());
        this.restClient = restClientBuilder.requestFactory(requestFactory).build();
        this.properties = properties;
    }

    public void ack(int contentType, long eventId) {
        ResponseData<?> response = restClient.post()
                .uri(properties.getIngressAckUrl())
                .body(new EventAckCommand(contentType, eventId))
                .retrieve()
                .body(ResponseData.class);
        if (response == null || response.getCode() == null
                || response.getCode() != ErrorCode.SUCCESS.getCode()) {
            String message = response == null ? "empty response" : response.getMessage();
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "Ingress event ACK失败: " + message);
        }
    }

    private record EventAckCommand(Integer contentType, Long eventId) {
    }
}
