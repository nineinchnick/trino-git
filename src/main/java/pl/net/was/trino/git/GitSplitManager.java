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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static pl.net.was.trino.git.GitMetadata.getCommitIds;

public class GitSplitManager
        implements ConnectorSplitManager
{
    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        return new GitDynamicFilteringSplitSource((GitTableHandle) connectorTableHandle);
    }

    private static ConnectorSplitSource getSplitSource(
            GitTableHandle table,
            DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        TupleDomain<GitColumnHandle> constraint = table.constraint()
                .intersect(dynamicFilterSnapshot.currentPredicate().simplify(100).transformKeys(GitColumnHandle.class::cast));
        Optional<List<String>> commitIds = GitClient.Table.valueOf(table.tableName()).getCommitIdColumn()
                .flatMap(commitIdColumn -> getCommitIds(constraint, commitIdColumn));

        List<GitSplit> splits = List.of(new GitSplit(table.tableName(), commitIds));

        return new FixedSplitSource(splits);
    }

    private static class GitDynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private static final long DYNAMIC_FILTERING_WAIT_TIMEOUT_MILLIS = 20_000;

        private final GitTableHandle table;
        private ConnectorSplitSource delegateSplitSource;

        private GitDynamicFilteringSplitSource(GitTableHandle table)
        {
            this.table = requireNonNull(table, "table is null");
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
                delegateSplitSource = getSplitSource(table, dynamicFilterSnapshot);
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
