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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record GitSplit(String tableName, Optional<List<String>> commitIds)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(GitSplit.class);

    public GitSplit
    {
        requireNonNull(tableName, "tableName is null");
        commitIds = requireNonNull(commitIds, "commitIds is null").map(ImmutableList::copyOf);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(tableName)
                + sizeOf(commitIds, ids -> estimatedSizeOf(ids, SizeOf::estimatedSizeOf));
    }
}
