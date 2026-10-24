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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * State of a repository to read: refs pinned to commits, so all scans of a query read the same commits, or the latest
 * refs, and the refresh generation the repository must have been fetched for.
 */
public record Snapshot(long refreshGeneration, Optional<Map<String, String>> refs)
{
    private static final int INSTANCE_SIZE = instanceSize(Snapshot.class);

    public Snapshot
    {
        refs = requireNonNull(refs, "refs is null").map(ImmutableMap::copyOf);
    }

    public static Snapshot latest(long refreshGeneration)
    {
        return new Snapshot(refreshGeneration, Optional.empty());
    }

    public List<Ref> getRefs(Repository repository)
            throws IOException
    {
        if (refs.isEmpty()) {
            return repository.getRefDatabase().getRefs();
        }
        return refs.orElseThrow().entrySet().stream()
                .map(entry -> (Ref) new ObjectIdRef.Unpeeled(Ref.Storage.NETWORK, entry.getKey(), ObjectId.fromString(entry.getValue())))
                .collect(toImmutableList());
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(refs, map -> estimatedSizeOf(map, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf));
    }
}
