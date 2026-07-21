package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.dto.ParsedEventVersion;
import com.mtx.trade.storage.api.StorageRef;

/** Storage 成功、event 失败场景的审计服务。 */
public interface EventIngestFailureLogService {

    String STAGE_EVENT_FIELD_PARSE = "EVENT_FIELD_PARSE";
    String STAGE_EVENT_PERSIST = "EVENT_PERSIST";

    void recordFailure(
            int sourceSystem,
            int contentType,
            StorageRef storageRef,
            String failureStage,
            ParsedEventVersion parsedEvent,
            Throwable failure);
}
