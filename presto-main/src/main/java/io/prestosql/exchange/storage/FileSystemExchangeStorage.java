/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.exchange.storage;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.exchange.ExchangeSourceFile;
import io.prestosql.exchange.FileStatus;
import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;
import io.prestosql.spi.filesystem.HetuFileSystemClient;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

public interface FileSystemExchangeStorage
        extends AutoCloseable
{
    public void setFileSystemClient(HetuFileSystemClient fsClient);

    HetuFileSystemClient getFileSystemClient();

    void createDirectories(URI dir) throws IOException;

    ExchangeStorageReader createExchangeReader(Queue<ExchangeSourceFile> sourceFiles, int maxPageSize, DirectSerialisationType directSerialisationType, int directSerialisationBufferSize);

    ExchangeStorageWriter createExchangeWriter(URI file, Optional<SecretKey> secretKey, boolean exchangeCompressionEnabled, DirectSerialisationType directSerialisationType, int directSerialisationBufferSize);

    ListenableFuture<Void> createEmptyFile(URI file);

    ListenableFuture<Void> deleteRecursively(List<URI> directories);

    ListenableFuture<List<FileStatus>> listFilesRecursively(URI dir);

    int getWriterBufferSize();

    @Override
    void close() throws IOException;
}
