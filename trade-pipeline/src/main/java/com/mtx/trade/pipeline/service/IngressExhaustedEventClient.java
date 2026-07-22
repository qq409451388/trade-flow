package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.config.OrderEventConsumerProperties;
import com.mtx.trade.pipeline.dto.IngressExhaustedEvent;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collections;
import java.util.List;

/** 按内容类型查询 Ingress 中自动投递已耗尽的事件。 */
@Service
public class IngressExhaustedEventClient {

    private static final ParameterizedTypeReference<ResponseData<List<IngressExhaustedEvent>>> RESPONSE_TYPE =
            new ParameterizedTypeReference<>() {
            };

    private final RestClient restClient;
    private final OrderEventConsumerProperties properties;

    public IngressExhaustedEventClient(
            RestClient.Builder restClientBuilder,
            OrderEventConsumerProperties properties) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(properties.getIngressAckConnectTimeout());
        requestFactory.setReadTimeout(properties.getIngressAckReadTimeout());
        this.restClient = restClientBuilder.requestFactory(requestFactory).build();
        this.properties = properties;
    }

    public List<IngressExhaustedEvent> list(
            int contentType, List<Long> eventIds, int limit, long afterEventId) {
        if (contentType != ContentType.ORDER.getCode() && contentType != ContentType.PAYMENT.getCode()) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "不支持的事件类型: " + contentType);
        }
        UriComponentsBuilder uriBuilder = UriComponentsBuilder
                .fromUriString(properties.getIngressExhaustedUrl())
                .queryParam("contentType", contentType)
                .queryParam("limit", limit);
        if (eventIds != null && !eventIds.isEmpty()) {
            uriBuilder.queryParam("eventIds", eventIds.toArray());
        } else if (afterEventId > 0) {
            uriBuilder.queryParam("afterEventId", afterEventId);
        }
        ResponseData<List<IngressExhaustedEvent>> response = restClient.get()
                .uri(uriBuilder.build(true).toUri())
                .retrieve()
                .body(RESPONSE_TYPE);
        if (response == null || response.getCode() == null
                || response.getCode() != ErrorCode.SUCCESS.getCode()) {
            String message = response == null ? "empty response" : response.getMessage();
            throw new BusinessException(ErrorCode.SYSTEM_ERROR, "查询Ingress耗尽事件失败: " + message);
        }
        return response.getData() == null ? Collections.emptyList() : response.getData();
    }
}
