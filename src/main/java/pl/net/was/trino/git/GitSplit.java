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
package pl.net.was.trino.git;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record GitSplit(SchemaTableName table, Optional<List<String>> commitIds, Snapshot snapshot)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(GitSplit.class);

    public GitSplit
    {
        requireNonNull(table, "table is null");
        commitIds = requireNonNull(commitIds, "commitIds is null").map(ImmutableList::copyOf);
        requireNonNull(snapshot, "snapshot is null");
    }

    // only used by the scheduler on the coordinator
    @JsonIgnore
    @Override
    public Optional<String> getAffinityKey()
    {
        // every worker reading a repository needs its own clone, so read it on as few workers as possible
        return Optional.of(table.getSchemaName());
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + table.getRetainedSizeInBytes()
                + sizeOf(commitIds, ids -> estimatedSizeOf(ids, SizeOf::estimatedSizeOf))
                + snapshot.getRetainedSizeInBytes();
    }
}
