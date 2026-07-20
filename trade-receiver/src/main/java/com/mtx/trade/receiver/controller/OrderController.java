package com.mtx.trade.receiver.controller;

import com.mtx.trade.receiver.dto.FuiouResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/order")
public class OrderController {
    @PostMapping("/store-push")
    public FuiouResponse storePush(@RequestBody String payload) {
        return FuiouResponse.ok();
    }
}
