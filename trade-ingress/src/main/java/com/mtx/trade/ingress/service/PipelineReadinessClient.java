package com.mtx.trade.ingress.service;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.config.EventDeliveryCircuitProperties;
import com.mtx.trade.ingress.dto.PipelineReadinessVO;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

/** Ingress恢复熔断前主动检查Pipeline真实消费能力。 */
@Service
public class PipelineReadinessClient {

    private static final ParameterizedTypeReference<ResponseData<PipelineReadinessVO>> RESPONSE_TYPE =
            new ParameterizedTypeReference<>() {
            };

    private final RestClient restClient;
    private final EventDeliveryCircuitProperties properties;

    public PipelineReadinessClient(
            RestClient.Builder restClientBuilder,
            EventDeliveryCircuitProperties properties) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(properties.getReadinessConnectTimeout());
        requestFactory.setReadTimeout(properties.getReadinessReadTimeout());
        this.restClient = restClientBuilder.requestFactory(requestFactory).build();
        this.properties = properties;
    }

    public boolean isReady(int contentType) {
        String uri = UriComponentsBuilder.fromUriString(properties.getPipelineReadinessUrl())
                .queryParam("contentType", contentType)
                .build(true)
                .toUriString();
        ResponseData<PipelineReadinessVO> response = restClient.get()
                .uri(uri)
                .retrieve()
                .body(RESPONSE_TYPE);
        if (response == null || response.getCode() == null
                || response.getCode() != ErrorCode.SUCCESS.getCode()) {
            throw new BusinessException(ErrorCode.SYSTEM_ERROR, "Pipeline readiness响应异常");
        }
        return response.getData() != null && response.getData().ready();
    }
}
