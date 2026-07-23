package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.config.OrderEventConsumerProperties;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.net.Proxy;
import java.util.List;

/** Pipeline 持久化完成后通知 Ingress 完成事件接管。 */
@Service
public class IngressEventAckClient {

    private final RestClient restClient;
    private final OrderEventConsumerProperties properties;

    public IngressEventAckClient(
        RestClient.Builder restClientBuilder,
            OrderEventConsumerProperties properties) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        // Ingress 是内部服务，禁止继承开发机或宿主机的 HTTP/SOCKS 代理。
        requestFactory.setProxy(Proxy.NO_PROXY);
        requestFactory.setConnectTimeout(properties.getIngressAckConnectTimeout());
        requestFactory.setReadTimeout(properties.getIngressAckReadTimeout());
        this.restClient = restClientBuilder.requestFactory(requestFactory).build();
        this.properties = properties;
    }

    public void ack(int contentType, long eventId) {
        ResponseData<?> response = restClient.post()
                .uri(properties.getIngressAckUrl())
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .body(new EventAckCommand(contentType, eventId))
                .retrieve()
                .body(ResponseData.class);
        if (response == null || response.getCode() == null
                || response.getCode() != ErrorCode.SUCCESS.getCode()) {
            String message = response == null ? "empty response" : response.getMessage();
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "Ingress event ACK失败: " + message);
        }
    }

    public void batchAck(int contentType, List<Long> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) {
            return;
        }
        ResponseData<?> response = restClient.post()
                .uri(properties.getIngressBatchAckUrl())
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                .body(new EventBatchAckCommand(contentType, eventIds)).retrieve().body(ResponseData.class);
        if (response == null || response.getCode() == null || response.getCode() != ErrorCode.SUCCESS.getCode()) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR,
                    "Ingress event批量ACK失败: " + (response == null ? "empty response" : response.getMessage()));
        }
    }

    private record EventAckCommand(Integer contentType, Long eventId) {
    }

    private record EventBatchAckCommand(Integer contentType, List<Long> eventIds) {
    }
}
