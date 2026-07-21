package com.mtx.trade.storage.local;

import com.mtx.trade.storage.api.StorageRef;
import com.mtx.trade.storage.api.StorageWriteCommand;
import com.mtx.trade.storage.api.StorageWriteException;
import com.mtx.trade.storage.local.entity.StorageBlobDO;
import com.mtx.trade.storage.local.entity.StorageDO;
import com.mtx.trade.storage.local.service.db.StorageBlobDbService;
import com.mtx.trade.storage.local.service.db.StorageDbService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
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

        StorageRef result = adapter.put(new StorageWriteCommand(1, 1, new byte[]{1, 2, 3}, null));

        ArgumentCaptor<StorageDO> storageCaptor = ArgumentCaptor.forClass(StorageDO.class);
        ArgumentCaptor<StorageBlobDO> blobCaptor = ArgumentCaptor.forClass(StorageBlobDO.class);
        verify(storageDbService).save(storageCaptor.capture());
        verify(storageBlobDbService).save(blobCaptor.capture());
        assertThat(storageCaptor.getValue().getId()).isEqualTo(STORAGE_ID);
        assertThat(blobCaptor.getValue().getId()).isEqualTo(STORAGE_ID);
        assertThat(result.storageId()).isEqualTo(STORAGE_ID);
    }

    @Test
    void shouldFailBeforeBlobWriteWhenMetadataSaveReturnsFalse() {
        when(storageDbService.save(any())).thenReturn(false);

        assertThatThrownBy(() -> adapter.put(new StorageWriteCommand(1, 1, new byte[]{1}, null)))
                .isInstanceOf(StorageWriteException.class)
                .hasMessageContaining("元数据");
        verify(storageBlobDbService, never()).save(any());
    }

    @Test
    void shouldFailTransactionWhenBlobSaveReturnsFalse() {
        when(storageDbService.save(any())).thenReturn(true);
        when(storageBlobDbService.save(any())).thenReturn(false);

        assertThatThrownBy(() -> adapter.put(new StorageWriteCommand(1, 1, new byte[]{1}, null)))
                .isInstanceOf(StorageWriteException.class)
                .hasMessageContaining("BLOB");
    }
}
