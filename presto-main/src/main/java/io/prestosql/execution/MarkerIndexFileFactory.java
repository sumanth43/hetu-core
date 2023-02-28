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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prestosql.execution.buffer.HybridSpoolingBuffer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.filesystem.HetuFileSystemClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class MarkerIndexFileFactory
{
    private final URI targetOutputDirectory;
    private final HetuFileSystemClient hetuFileSystemClient;

    public MarkerIndexFileFactory(URI taskOutputDirectory, HetuFileSystemClient hetuFileSystemClient)
    {
        this.targetOutputDirectory = taskOutputDirectory;
        this.hetuFileSystemClient = requireNonNull(hetuFileSystemClient, "hetuFileSystemClient is null");
    }

    public MarkerIndexFileWriter createWriter(String file)
    {
        Path path = Paths.get(targetOutputDirectory.resolve(file).getPath());
        return new MarkerIndexFileWriter(hetuFileSystemClient, path);
    }

    public static class MarkerIndexFileWriter
    {
        private final OutputStream outputStream;
        private final HetuFileSystemClient hetuFileSystemClient;

        public MarkerIndexFileWriter(HetuFileSystemClient hetuFileSystemClient, Path path)
        {
            OutputStream localOutputStream = null;
            try {
                localOutputStream = hetuFileSystemClient.newOutputStream(path);
            }
            catch (IOException e) {
                // add logs
            }
            outputStream = localOutputStream;
            this.hetuFileSystemClient = hetuFileSystemClient;
        }

        public void writeIndexFile(MarkerIndexFile markerIndexFile)
        {
            JsonFactory jsonFactory = new JsonFactory();
            jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
            ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
            try {
                objectMapper.writeValue(outputStream, markerIndexFile);
                outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
                outputStream.flush();
                hetuFileSystemClient.flush(outputStream);
            }
            catch (IOException e) {
                // add logs
            }
        }
    }

    public static class MarkerIndexFileReader
    {
        private final InputStream inputStream;

        public MarkerIndexFileReader(HetuFileSystemClient hetuFileSystemClient, Path path)
        {
            InputStream localInputStream = null;
            try {
                localInputStream = hetuFileSystemClient.newInputStream(path);
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create marker index file reader");
            }
            inputStream = localInputStream;
        }

        public MarkerIndexFile readIndexFile(int markerId)
        {
            JsonFactory jsonFactory = new JsonFactory();
            jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
            MarkerIndexFile markerIndexFile = null;
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line = reader.readLine();
                while (line != null) {
                    ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
                    markerIndexFile = objectMapper.readValue(line, MarkerIndexFile.class);
                    if (markerId == markerIndexFile.getMarkerId()) {
                        return markerIndexFile;
                    }
                    line = reader.readLine();
                }
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Marker ID: " + markerId + " is not present in index file");
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to read marker index file");
            }
        }
    }

    public static class MarkerIndexFile
    {
        private final int markerId;
        private final URI markerDataFile;
        private final long markerStartOffset;
        private final long markerLength;
        private final Map<URI, HybridSpoolingBuffer.SpoolingInfo> spoolingInfoMap;

        @JsonCreator
        public MarkerIndexFile(
                @JsonProperty("markerID") int markerId,
                @JsonProperty("markerDataFile") URI markerDataFile,
                @JsonProperty("markerStartOffset") long markerStartOffset,
                @JsonProperty("markerLength") long markerLength,
                @JsonProperty("spoolingInfoMap") Map<URI, HybridSpoolingBuffer.SpoolingInfo> spoolingInfoMap)
        {
            this.markerId = markerId;
            this.markerDataFile = markerDataFile;
            this.markerStartOffset = markerStartOffset;
            this.markerLength = markerLength;
            this.spoolingInfoMap = spoolingInfoMap;
        }

        public static MarkerIndexFile createMarkerIndexFile(int markerId, URI markerDataFile, long markerStartOffset, long markerLength, Map<URI, HybridSpoolingBuffer.SpoolingInfo> spoolingInfoMap)
        {
            return new MarkerIndexFile(markerId, markerDataFile, markerStartOffset, markerLength, spoolingInfoMap);
        }

        @JsonProperty
        public int getMarkerId()
        {
            return markerId;
        }

        @JsonProperty
        public URI getMarkerDataFile()
        {
            return markerDataFile;
        }

        @JsonProperty
        public long getMarkerStartOffset()
        {
            return markerStartOffset;
        }

        @JsonProperty
        public long getMarkerLength()
        {
            return markerLength;
        }

        @JsonProperty
        public Map<URI, HybridSpoolingBuffer.SpoolingInfo> getSpoolingInfoMap()
        {
            return spoolingInfoMap;
        }
    }
}
