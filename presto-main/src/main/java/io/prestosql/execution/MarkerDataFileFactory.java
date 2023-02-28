/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.execution;

import io.prestosql.snapshot.RecoveryUtils;
import io.prestosql.snapshot.SnapshotStateId;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.filesystem.HetuFileSystemClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.filesystem.SupportedFileAttributes.SIZE;
import static java.util.Objects.requireNonNull;

public class MarkerDataFileFactory
{
    private final URI targetOutputDirectory;
    private final HetuFileSystemClient hetuFileSystemClient;

    public MarkerDataFileFactory(URI taskOutputDirectory, HetuFileSystemClient hetuFileSystemClient)
    {
        this.targetOutputDirectory = taskOutputDirectory;
        this.hetuFileSystemClient = requireNonNull(hetuFileSystemClient, "hetuFileSystemClient is null");
    }

    public MarkerDataFileWriter createWriter(String file, boolean useKryo)
    {
        URI fileURI = targetOutputDirectory.resolve(file);
        return new MarkerDataFileWriter(hetuFileSystemClient, fileURI, useKryo);
    }

    public MarkerDataFileReader createReader(URI file, boolean useKryo)
    {
        return new MarkerDataFileReader(hetuFileSystemClient, file, useKryo);
    }

    public static class MarkerDataFileWriter
    {
        private final OutputStream outputStream;
        private final boolean useKryo;
        private HetuFileSystemClient hetuFileSystemClient;
        private Path path;
        private URI fileURI;

        public MarkerDataFileWriter(HetuFileSystemClient hetuFileSystemClient, URI fileURI, boolean useKryo)
        {
            this.hetuFileSystemClient = hetuFileSystemClient;
            this.path = Paths.get(fileURI.getPath());
            this.fileURI = fileURI;
            OutputStream localOutputStream = null;
            try {
                localOutputStream = hetuFileSystemClient.newOutputStream(path);
            }
            catch (IOException e) {
                // add logs
            }
            this.outputStream = localOutputStream;
            this.useKryo = useKryo;
        }

        public MarkerDataFileFooterInfo writeDataFile(int markerId, Map<SnapshotStateId, Object> state, long previousFooterOffset, long previousFooterSize)
        {
            Map<String, OperatorStateInfo> operatorStateInfoMap = new HashMap<>();
            try {
                for (Map.Entry<SnapshotStateId, Object> entry : state.entrySet()) {
                    long offset = (Long) hetuFileSystemClient.getAttribute(path, SIZE);
                    RecoveryUtils.serializeState(entry.getValue(), outputStream, useKryo);
                    outputStream.flush();
                    hetuFileSystemClient.flush(outputStream);
                    long size = (Long) hetuFileSystemClient.getAttribute(path, SIZE) - offset;
                    operatorStateInfoMap.put(entry.getKey().getId(), new OperatorStateInfo(offset, size));
                }
                // write footer
                MarkerDataFileFooter footer = new MarkerDataFileFooter(markerId, previousFooterOffset, previousFooterSize, state.size(), operatorStateInfoMap);
                long footerOffset = (Long) hetuFileSystemClient.getAttribute(path, SIZE);
                RecoveryUtils.serializeState(footer, outputStream, useKryo);
                outputStream.flush();
                hetuFileSystemClient.flush(outputStream);
                long footerSize = (Long) hetuFileSystemClient.getAttribute(path, SIZE) - footerOffset;
                return new MarkerDataFileFooterInfo(fileURI, footer, footerOffset, footerSize);
            }
            catch (IOException e) {
                // add logs
                System.out.println(1);
            }
            return null;
        }
    }

    public static class MarkerDataFileReader
    {
        private final boolean useKryo;
        private Path path;
        private HetuFileSystemClient hetuFileSystemClient;

        public MarkerDataFileReader(HetuFileSystemClient hetuFileSystemClient, URI fileURI, boolean useKryo)
        {
            this.path = Paths.get(fileURI.getPath());
            this.useKryo = useKryo;
            this.hetuFileSystemClient = hetuFileSystemClient;
        }

        public Map<SnapshotStateId, Object> readDataFile(long markerDataOffset, long markerDataLength)
        {
            try {
                InputStream inputStream = hetuFileSystemClient.newInputStream(path);
                inputStream.skip(markerDataOffset);
                MarkerDataFileFooter markerDataFileFooter = (MarkerDataFileFooter) RecoveryUtils.deserializeState(inputStream, useKryo);
                int operatorCount = markerDataFileFooter.getOperatorCount();
                Map<String, OperatorStateInfo> operatorStateInfoMap = markerDataFileFooter.getOperatorStateInfo();
                checkArgument(operatorCount == operatorStateInfoMap.size(), "operator data mismatch");
                inputStream.close();

                Map<SnapshotStateId, Object> states = new HashMap<>();
                for (Map.Entry<String, OperatorStateInfo> operatorStateInfoEntry : operatorStateInfoMap.entrySet()) {
                    inputStream = hetuFileSystemClient.newInputStream(path);
                    inputStream.skip(operatorStateInfoEntry.getValue().getStateOffset());
                    states.put(SnapshotStateId.fromString(operatorStateInfoEntry.getKey()), RecoveryUtils.deserializeState(inputStream, useKryo));
                    inputStream.close();
                }
                return states;
            }
            catch (Exception e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed reading data file");
            }
        }
    }

    public static class MarkerDataFileFooterInfo
            implements Serializable
    {
        private URI path;
        private MarkerDataFileFooter footer;
        private long footerOffset;
        private long footerSize;

        public MarkerDataFileFooterInfo(URI path, MarkerDataFileFooter footer, long footerOffset, long footerSize)
        {
            this.path = path;
            this.footer = footer;
            this.footerOffset = footerOffset;
            this.footerSize = footerSize;
        }

        public long getFooterOffset()
        {
            return footerOffset;
        }

        public long getFooterSize()
        {
            return footerSize;
        }

        public MarkerDataFileFooter getFooter()
        {
            return footer;
        }

        public URI getPath()
        {
            return path;
        }
    }

    public static class MarkerDataFileFooter
            implements Serializable
    {
        private int markerId;
        private int version;
        private long previousTailOffset;
        private long previousTailSize;
        private int operatorCount;
        private Map<String, OperatorStateInfo> operatorStateInfo;

        public MarkerDataFileFooter(int markerId, long previousTailOffset, long previousTailSize, int operatorCount, Map<String, OperatorStateInfo> operatorStateInfo)
        {
            this(markerId, 1, previousTailOffset, previousTailSize, operatorCount, operatorStateInfo);
        }

        public MarkerDataFileFooter(int markerId, int version, long previousTailOffset, long previousTailSize, int operatorCount, Map<String, OperatorStateInfo> operatorStateInfo)
        {
            this.markerId = markerId;
            this.version = version;
            this.previousTailOffset = previousTailOffset;
            this.operatorCount = operatorCount;
            this.operatorStateInfo = operatorStateInfo;
            this.previousTailSize = previousTailSize;
        }

        public int getMarkerId()
        {
            return markerId;
        }

        public int getVersion()
        {
            return version;
        }

        public long getPreviousTailOffset()
        {
            return previousTailOffset;
        }

        public long getPreviousTailSize()
        {
            return previousTailSize;
        }

        public int getOperatorCount()
        {
            return operatorCount;
        }

        public Map<String, OperatorStateInfo> getOperatorStateInfo()
        {
            return operatorStateInfo;
        }
    }

    public static class OperatorStateInfo
            implements Serializable
    {
        private long stateOffset;
        private long stateSize;

        public OperatorStateInfo(long stateOffset, long stateSize)
        {
            this.stateOffset = stateOffset;
            this.stateSize = stateSize;
        }

        public long getStateOffset()
        {
            return stateOffset;
        }

        public long getStateSize()
        {
            return stateSize;
        }
    }
}
