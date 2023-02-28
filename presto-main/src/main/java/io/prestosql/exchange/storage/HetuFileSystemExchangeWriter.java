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

import com.esotericsoftware.kryo.io.Output;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.compress.snappy.SnappyFramedOutputStream;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.exchange.FileSystemExchangeConfig;
import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;
import io.prestosql.execution.MarkerDataFileFactory;
import io.prestosql.snapshot.RecoveryUtils;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import org.openjdk.jol.info.ClassLayout;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class HetuFileSystemExchangeWriter
        implements ExchangeStorageWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HetuFileSystemExchangeWriter.class).instanceSize();
    private static final String CIPHER_TRANSFORMATION = "AES/CBC/PKCS5Padding";
    private final OutputStream outputStream;
    private final DirectSerialisationType directSerialisationType;
    private final int directSerialisationBufferSize;
    private final HetuFileSystemClient fileSystemClient;
    private final OutputStream delegateOutputStream;
    private final URI file;

    public HetuFileSystemExchangeWriter(URI file, HetuFileSystemClient fileSystemClient, Optional<SecretKey> secretKey, boolean exchangeCompressionEnabled, AlgorithmParameterSpec algorithmParameterSpec, FileSystemExchangeConfig.DirectSerialisationType directSerialisationType, int directSerialisationBufferSize)
    {
        this.directSerialisationBufferSize = directSerialisationBufferSize;
        this.directSerialisationType = directSerialisationType;
        this.fileSystemClient = fileSystemClient;
        this.file = file;
        try {
            Path path = Paths.get(file.toString());
            this.delegateOutputStream = fileSystemClient.newOutputStream(path);
            if (secretKey.isPresent() && exchangeCompressionEnabled) {
                Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
                cipher.init(Cipher.ENCRYPT_MODE, secretKey.get(), algorithmParameterSpec);
                this.outputStream = new SnappyFramedOutputStream(new CipherOutputStream(delegateOutputStream, cipher));
            }
            else if (secretKey.isPresent()) {
                Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
                cipher.init(Cipher.ENCRYPT_MODE, secretKey.get(), algorithmParameterSpec);
                this.outputStream = new CipherOutputStream(delegateOutputStream, cipher);
            }
            else if (exchangeCompressionEnabled) {
                this.outputStream = new SnappyFramedOutputStream(new OutputStreamSliceOutput(delegateOutputStream, directSerialisationBufferSize));
            }
            else {
                if (directSerialisationType == DirectSerialisationType.KRYO) {
                    this.outputStream = new Output(delegateOutputStream, directSerialisationBufferSize);
                }
                else if (directSerialisationType == DirectSerialisationType.JAVA) {
                    this.outputStream = new OutputStreamSliceOutput(delegateOutputStream, directSerialisationBufferSize);
                }
                else {
                    this.outputStream = new OutputStreamSliceOutput(delegateOutputStream, directSerialisationBufferSize);
                }
            }
        }
        catch (IOException | NoSuchAlgorithmException | InvalidAlgorithmParameterException | NoSuchPaddingException |
               InvalidKeyException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create OutputStream: " + e.getMessage(), e);
        }
    }

    @Override
    public ListenableFuture<Void> write(Slice slice)
    {
        try {
            outputStream.write(slice.getBytes());
            outputStream.flush();
            fileSystemClient.flush(delegateOutputStream);
        }
        catch (IOException | RuntimeException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> write(Page page, PagesSerde serde)
    {
        checkState(directSerialisationType != DirectSerialisationType.OFF, "Should be used with direct serialization is enabled!");
        serde.serialize(outputStream, page);
        try {
            outputStream.flush();
            fileSystemClient.flush(delegateOutputStream);
        }
        catch (IOException | RuntimeException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> finish()
    {
        try {
            outputStream.close();
        }
        catch (IOException | RuntimeException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> abort()
    {
        try {
            outputStream.close();
        }
        catch (IOException | RuntimeException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(null);
    }

    @Override
    public long getRetainedSize()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public URI getFile()
    {
        return file;
    }

    @Override
    public void writeMarkerMetadata(MarkerDataFileFactory.MarkerDataFileFooterInfo markerDataFileFooterInfo)
    {
        try {
            RecoveryUtils.serializeState(markerDataFileFooterInfo, outputStream, false);
            outputStream.flush();
            fileSystemClient.flush(delegateOutputStream);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
