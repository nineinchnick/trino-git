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
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;

/**
 * Pins the refs of every repository read in a transaction to the commits they pointed to when it was first read,
 * on the coordinator, so all scans in a query read the same commits, even if workers fetch in the meantime.
 */
public class PinnedSnapshots
{
    private final GitRepositoryManager repositoryManager;
    private final RefreshGenerations refreshGenerations;
    private final ConcurrentMap<GitTransactionHandle, ConcurrentMap<String, Snapshot>> snapshots = new ConcurrentHashMap<>();

    @Inject
    public PinnedSnapshots(GitRepositoryManager repositoryManager, RefreshGenerations refreshGenerations)
    {
        this.repositoryManager = requireNonNull(repositoryManager, "repositoryManager is null");
        this.refreshGenerations = requireNonNull(refreshGenerations, "refreshGenerations is null");
    }

    public Snapshot get(GitTransactionHandle transaction, String schemaName)
    {
        return snapshots.computeIfAbsent(transaction, _ -> new ConcurrentHashMap<>())
                .computeIfAbsent(schemaName, this::pin);
    }

    public void release(GitTransactionHandle transaction)
    {
        snapshots.remove(transaction);
    }

    private Snapshot pin(String schemaName)
    {
        long refreshGeneration = refreshGenerations.get(schemaName);
        Repository repository = repositoryManager.getRepository(schemaName, Snapshot.latest(refreshGeneration)).getRepository();
        ImmutableMap.Builder<String, String> refs = ImmutableMap.builder();
        try {
            for (Ref ref : repository.getRefDatabase().getRefs()) {
                // symbolic refs to unborn branches, like HEAD in an empty repository, don't point to any object
                if (ref.getObjectId() != null) {
                    refs.put(ref.getName(), ref.getObjectId().getName());
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to read refs", e);
        }
        return new Snapshot(refreshGeneration, Optional.of(refs.buildOrThrow()));
    }
}
