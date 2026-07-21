package com.mtx.trade.receiver.controller;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.receiver.common.enums.SourceSystem;
import com.mtx.trade.receiver.dto.FuiouResponse;
import com.mtx.trade.storage.api.StorageMetadata;
import com.mtx.trade.storage.api.StorageReader;
import com.mtx.trade.storage.api.StorageWriteCommand;
import com.mtx.trade.storage.api.StorageWriter;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/order")
public class OrderController {
    @Resource
    private StorageReader storageReader;
    @Resource
    private StorageWriter storageWriter;

    @GetMapping("/storage-metadata")
    public ResponseData<StorageMetadata> storageMetadata(@RequestParam Long storageId) {
        return ResponseData.success(storageReader.getMetadata(storageId));
    }
    @PostMapping("/store-push")
    public FuiouResponse storePush(@RequestBody String payload) {
        storageWriter.put(new StorageWriteCommand(
                SourceSystem.FUIOU.getCode(),
                ContentType.ORDER.getCode(),
                payload.getBytes(StandardCharsets.UTF_8),
                null));
        return FuiouResponse.ok();
    }
}
