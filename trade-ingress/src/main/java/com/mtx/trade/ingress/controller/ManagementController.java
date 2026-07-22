package com.mtx.trade.ingress.controller;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.service.OrderEventService;
import com.mtx.trade.storage.api.StorageKey;
import com.mtx.trade.storage.api.StorageReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/m")
public class ManagementController {

    private final StorageReader storageReader;
    private final OrderEventService orderEventService;

    @GetMapping("/getBlob")
    public ResponseData<String> getBlob(@RequestParam Long id) {
        if (id == null || id <= 0) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "订单事件ID无效");
        }
        OrderEventDO orderEventDO = orderEventService.getById(id);
        if (orderEventDO == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND, "订单事件不存在");
        }
        StorageKey storageKey = new StorageKey(orderEventDO.getRawId(), orderEventDO.getPayloadSha256());
        byte[] bytes = storageReader.getContent(storageKey);
        if (bytes == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND, "订单事件原始报文不存在");
        }
        return ResponseData.success(new String(bytes, StandardCharsets.UTF_8));
    }
}
