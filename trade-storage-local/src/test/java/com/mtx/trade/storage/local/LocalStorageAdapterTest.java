package com.mtx.trade.storage.local;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.mtx.trade.storage.api.StorageRef;
import com.mtx.trade.storage.api.StorageWriteCommand;
import com.mtx.trade.storage.api.StorageWriteException;
import com.mtx.trade.storage.local.entity.StorageBlobDO;
import com.mtx.trade.storage.local.entity.StorageDO;
import com.mtx.trade.storage.local.service.db.StorageBlobDbService;
import com.mtx.trade.storage.local.service.db.StorageDbService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DuplicateKeyException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LocalStorageAdapterTest {

    private static final long STORAGE_ID = 190_123L;
    private final StorageDbService storageDbService = mock(StorageDbService.class);
    private final StorageBlobDbService storageBlobDbService = mock(StorageBlobDbService.class);
    private final LocalStorageAdapter adapter = new LocalStorageAdapter(
            storageDbService, storageBlobDbService, () -> STORAGE_ID);

    @Test
    void shouldUseOneStorageDomainIdForMetadataAndBlob() {
        when(storageDbService.save(any())).thenReturn(true);
        when(storageBlobDbService.save(any())).thenReturn(true);

        StorageRef result = adapter.putIfAbsent(new StorageWriteCommand(1, 1, new byte[]{1, 2, 3}, null));

        ArgumentCaptor<StorageDO> storageCaptor = ArgumentCaptor.forClass(StorageDO.class);
        ArgumentCaptor<StorageBlobDO> blobCaptor = ArgumentCaptor.forClass(StorageBlobDO.class);
        verify(storageDbService).save(storageCaptor.capture());
        verify(storageBlobDbService).save(blobCaptor.capture());
        assertThat(storageCaptor.getValue().getId()).isEqualTo(STORAGE_ID);
        assertThat(blobCaptor.getValue().getId()).isEqualTo(STORAGE_ID);
        assertThat(blobCaptor.getValue().getPayloadSha256())
                .containsExactly(storageCaptor.getValue().getPayloadSha256());
        assertThat(result.storageId()).isEqualTo(STORAGE_ID);
    }

    @Test
    void shouldFailBeforeBlobWriteWhenMetadataSaveReturnsFalse() {
        when(storageDbService.save(any())).thenReturn(false);

        assertThatThrownBy(() -> adapter.putIfAbsent(new StorageWriteCommand(1, 1, new byte[]{1}, null)))
                .isInstanceOf(StorageWriteException.class)
                .hasMessageContaining("元数据");
        verify(storageBlobDbService, never()).save(any());
    }

    @Test
    void shouldFailTransactionWhenBlobSaveReturnsFalse() {
        when(storageDbService.save(any())).thenReturn(true);
        when(storageBlobDbService.save(any())).thenReturn(false);

        assertThatThrownBy(() -> adapter.putIfAbsent(new StorageWriteCommand(1, 1, new byte[]{1}, null)))
                .isInstanceOf(StorageWriteException.class)
                .hasMessageContaining("BLOB");
    }

    @Test
    void putIfAbsentShouldReturnExistingWhenContentAlreadyStored() {
        StorageDO existing = new StorageDO();
        existing.setId(STORAGE_ID);
        existing.setPayloadSha256(new byte[32]);
        existing.setContentLength(10);
        when(storageDbService.getOne(any(Wrapper.class), anyBoolean())).thenReturn(existing);

        StorageRef result = adapter.putIfAbsent(new StorageWriteCommand(1, 1, new byte[]{1, 2, 3}, null));

        assertThat(result.storageId()).isEqualTo(STORAGE_ID);
        verify(storageDbService, never()).save(any());
        verify(storageBlobDbService, never()).save(any());
    }

    @Test
    void putIfAbsentShouldInsertWhenContentNotExists() {
        when(storageDbService.getOne(any(Wrapper.class), anyBoolean())).thenReturn(null);
        when(storageDbService.save(any())).thenReturn(true);
        when(storageBlobDbService.save(any())).thenReturn(true);

        StorageRef result = adapter.putIfAbsent(new StorageWriteCommand(1, 1, new byte[]{1, 2, 3}, null));

        assertThat(result.storageId()).isEqualTo(STORAGE_ID);
        verify(storageDbService).save(any());
        verify(storageBlobDbService).save(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void putIfAbsentShouldReturnWinnerAfterConcurrentUniqueKeyConflict() {
        StorageDO existing = new StorageDO();
        existing.setId(200_456L);
        existing.setPayloadSha256(new byte[32]);
        existing.setContentLength(10);
        when(storageDbService.getOne(any(Wrapper.class), anyBoolean()))
                .thenReturn(null, existing);
        when(storageDbService.save(any())).thenThrow(new DuplicateKeyException("duplicate sha"));

        StorageRef result = adapter.putIfAbsent(new StorageWriteCommand(1, 1, new byte[]{1, 2, 3}, null));

        assertThat(result.storageId()).isEqualTo(200_456L);
        verify(storageBlobDbService, never()).save(any());
    }
}
