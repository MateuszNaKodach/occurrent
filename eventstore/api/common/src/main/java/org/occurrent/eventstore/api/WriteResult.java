/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.eventstore.api;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * The result of a write to the event store.
 */
public class WriteResult {

    public final String streamId;
    public final long newStreamVersion;

    public WriteResult(String streamId, long newStreamVersion) {
        this.streamId = streamId;
        this.newStreamVersion = newStreamVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WriteResult)) return false;
        WriteResult that = (WriteResult) o;
        return newStreamVersion == that.newStreamVersion && Objects.equals(streamId, that.streamId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, newStreamVersion);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", WriteResult.class.getSimpleName() + "[", "]")
                .add("streamId='" + streamId + "'")
                .add("newStreamVersion=" + newStreamVersion)
                .toString();
    }

    public long getNewStreamVersion() {
        return newStreamVersion;
    }

    public String getStreamId() {
        return streamId;
    }
}