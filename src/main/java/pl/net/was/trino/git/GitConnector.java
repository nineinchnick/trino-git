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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class GitConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final GitMetadata metadata;
    private final GitSplitManager splitManager;
    private final GitRecordSetProvider recordSetProvider;
    private final Set<Procedure> procedures;
    private final PinnedSnapshots pinnedSnapshots;

    @Inject
    public GitConnector(
            LifeCycleManager lifeCycleManager,
            GitMetadata metadata,
            GitSplitManager splitManager,
            GitRecordSetProvider recordSetProvider,
            Set<Procedure> procedures,
            PinnedSnapshots pinnedSnapshots)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
        this.pinnedSnapshots = requireNonNull(pinnedSnapshots, "pinnedSnapshots is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return new GitTransactionHandle(randomUUID());
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        pinnedSnapshots.release((GitTransactionHandle) transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        pinnedSnapshots.release((GitTransactionHandle) transactionHandle);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
