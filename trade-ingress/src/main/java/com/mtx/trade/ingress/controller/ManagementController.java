package com.mtx.trade.ingress.controller;

import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.service.OrderEventService;
import com.mtx.trade.storage.api.StorageKey;
import com.mtx.trade.storage.api.StorageReader;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/m")
public class ManagementController {
    @Resource
    private StorageReader storageReader;
    @Resource
    private OrderEventService orderEventService;
    @GetMapping("/getBlob")
    public String getBlob(Long id) {
        OrderEventDO orderEventDO = orderEventService.getById(id);
        StorageKey storageKey = new StorageKey(orderEventDO.getRawId(), orderEventDO.getPayloadSha256());
        byte[] bytes = storageReader.getContent(storageKey);
        return new String(bytes);
    }
}
