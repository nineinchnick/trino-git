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
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.TupleDomain;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static pl.net.was.trino.git.CommitWalks.walkCommits;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;
import static pl.net.was.trino.git.GitMetadata.getIds;

public class GitSplitManager
        implements ConnectorSplitManager
{
    private final PinnedSnapshots pinnedSnapshots;
    private final GitRepositoryManager repositoryManager;
    private final int commitsPerSplit;

    @Inject
    public GitSplitManager(PinnedSnapshots pinnedSnapshots, GitRepositoryManager repositoryManager, GitConfig config)
    {
        this.pinnedSnapshots = requireNonNull(pinnedSnapshots, "pinnedSnapshots is null");
        this.repositoryManager = requireNonNull(repositoryManager, "repositoryManager is null");
        this.commitsPerSplit = config.getCommitsPerSplit();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        GitTableHandle table = (GitTableHandle) connectorTableHandle;
        Snapshot snapshot = pinnedSnapshots.get((GitTransactionHandle) transaction, table.schemaName());
        return new GitDynamicFilteringSplitSource(dynamicFilterSnapshot -> getSplitSource(table, snapshot, dynamicFilterSnapshot));
    }

    private ConnectorSplitSource getSplitSource(
            GitTableHandle table,
            Snapshot snapshot,
            DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        GitClient.Table gitTable = GitClient.Table.fromName(table.tableName());
        if (gitTable.getIdColumn().isEmpty()) {
            return new FixedSplitSource(List.of(new GitSplit(table.toSchemaTableName(), Optional.empty(), snapshot)));
        }

        TupleDomain<GitColumnHandle> constraint = table.constraint()
                .intersect(dynamicFilterSnapshot.currentPredicate().simplify(100).transformKeys(GitColumnHandle.class::cast));
        Optional<List<String>> ids = getIds(constraint, gitTable.getIdColumn().orElseThrow());
        if (!gitTable.isSplitByCommits()) {
            return new FixedSplitSource(List.of(new GitSplit(table.toSchemaTableName(), ids, snapshot)));
        }
        if (ids.isPresent()) {
            return new LazySplitSource(splits(table, snapshot, ids.orElseThrow().iterator()), () -> {});
        }
        // the coordinator has a clone with the pinned commits, since it read the refs from it
        Repository repository = repositoryManager.getRepository(table.schemaName(), snapshot).getRepository();
        RevWalk revWalk = new RevWalk(repository);
        try {
            Iterator<String> allCommitIds = Iterators.transform(walkCommits(revWalk, snapshot.getRefs(repository), Optional.empty()), RevCommit::getName);
            return new LazySplitSource(splits(table, snapshot, allCommitIds), revWalk::close);
        }
        catch (IOException e) {
            revWalk.close();
            throw new TrinoException(GIT_READ_ERROR, "Failed to read refs", e);
        }
    }

    private Iterator<GitSplit> splits(GitTableHandle table, Snapshot snapshot, Iterator<String> commitIds)
    {
        return Iterators.transform(
                Iterators.partition(commitIds, commitsPerSplit),
                chunk -> new GitSplit(table.toSchemaTableName(), Optional.of(chunk), snapshot));
    }

    /**
     * Creates splits only when they are requested, so all commit ids of a large repository are never in memory at once.
     */
    private static class LazySplitSource
            implements ConnectorSplitSource
    {
        private final Iterator<GitSplit> splits;
        private final Runnable onClose;

        private LazySplitSource(Iterator<GitSplit> splits, Runnable onClose)
        {
            this.splits = requireNonNull(splits, "splits is null");
            this.onClose = requireNonNull(onClose, "onClose is null");
        }

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
        {
            ImmutableList.Builder<ConnectorSplit> batch = ImmutableList.builder();
            for (int i = 0; i < maxSize && splits.hasNext(); i++) {
                batch.add(splits.next());
            }
            return completedFuture(batch.build());
        }

        @Override
        public void close()
        {
            onClose.run();
        }

        @Override
        public boolean isFinished()
        {
            return !splits.hasNext();
        }
    }

    private static class GitDynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private static final long DYNAMIC_FILTERING_WAIT_TIMEOUT_MILLIS = 20_000;

        private final Function<DynamicFilterSnapshot, ConnectorSplitSource> splitSourceFactory;
        private ConnectorSplitSource delegateSplitSource;

        private GitDynamicFilteringSplitSource(Function<DynamicFilterSnapshot, ConnectorSplitSource> splitSourceFactory)
        {
            this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        }

        @Override
        public long getRequestedDynamicFilterWaitTimeoutMillis()
        {
            return DYNAMIC_FILTERING_WAIT_TIMEOUT_MILLIS;
        }

        @Override
        public synchronized CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
        {
            if (delegateSplitSource == null) {
                delegateSplitSource = splitSourceFactory.apply(dynamicFilterSnapshot);
            }
            return delegateSplitSource.getNextBatch(maxSize, dynamicFilterSnapshot);
        }

        @Override
        public synchronized void close()
        {
            if (delegateSplitSource != null) {
                delegateSplitSource.close();
            }
        }

        @Override
        public synchronized boolean isFinished()
        {
            return delegateSplitSource != null && delegateSplitSource.isFinished();
        }
    }
}
