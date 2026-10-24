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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class GitMetadata
        implements ConnectorMetadata
{
    private static final Predicate<String> OBJECT_ID = Pattern.compile("[0-9a-f]{40}|[0-9a-f]{64}").asMatchPredicate();

    private final GitClient gitClient;
    private final CatalogName catalogName;

    @Inject
    public GitMetadata(CatalogName catalogName, GitClient gitClient)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.gitClient = requireNonNull(gitClient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return List.copyOf(gitClient.getSchemaNames());
    }

    @Override
    public GitTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        GitTable table = gitClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new GitTableHandle(tableName.getSchemaName(), tableName.getTableName(), TupleDomain.all());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(((GitTableHandle) table).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        if (optionalSchemaName.isPresent() && !gitClient.getSchemaNames().contains(optionalSchemaName.orElseThrow())) {
            throw new SchemaNotFoundException(optionalSchemaName.orElseThrow());
        }
        Set<String> schemaNames = optionalSchemaName.map(Set::of)
                .orElseGet(() -> Set.copyOf(gitClient.getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : gitClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        GitTableHandle gitTableHandle = (GitTableHandle) tableHandle;

        GitTable table = gitClient.getTable(gitTableHandle.schemaName(), gitTableHandle.tableName());
        if (table == null) {
            throw new TableNotFoundException(gitTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new GitColumnHandle(column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new HashMap<>();
        for (SchemaTableName tableName : listTables(session, schemaName)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                relationColumns.put(tableName, RelationColumnsMetadata.forTable(tableName, tableMetadata.getColumns()));
            }
        }
        for (SchemaTableName viewName : listViews(session, schemaName)) {
            getView(session, viewName).ifPresent(view -> relationColumns.put(viewName, RelationColumnsMetadata.forView(viewName, view.getColumns())));
        }
        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        GitTable table = gitClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((GitColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        if (optionalSchemaName.isPresent() && !gitClient.getSchemaNames().contains(optionalSchemaName.orElseThrow())) {
            throw new SchemaNotFoundException(optionalSchemaName.orElseThrow());
        }
        Set<String> schemaNames = optionalSchemaName.map(Set::of)
                .orElseGet(() -> Set.copyOf(gitClient.getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : gitClient.viewColumns.keySet()) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.ofNullable(gitClient.getView(catalogName.toString(), viewName.getSchemaName(), viewName.getTableName()));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        GitTableHandle gitTableHandle = (GitTableHandle) tableHandle;
        String tableName = gitTableHandle.tableName();

        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);
        TableStatistics.Builder builder = TableStatistics.builder();
        switch (tableName) {
            case "commits" -> {
                builder.setRowCount(Estimate.of(1));
                builder.setColumnStatistics(columns.get("object_id"), ColumnStatistics.builder()
                        .setNullsFraction(Estimate.zero())
                        .setDistinctValuesCount(Estimate.of(1))
                        .setDataSize(Estimate.of(1000))
                        .build());
            }
            case "trees" -> {
                builder.setRowCount(Estimate.of(1000000));
                builder.setColumnStatistics(columns.get("commit_id"), ColumnStatistics.builder()
                        .setNullsFraction(Estimate.zero())
                        .setDistinctValuesCount(Estimate.of(1000000))
                        .setDataSize(Estimate.of(1000000000))
                        .build());
            }
        }
        return builder.build();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        GitTableHandle handle = (GitTableHandle) table;
        Optional<String> commitIdColumn = GitClient.Table.valueOf(handle.tableName()).getCommitIdColumn();
        if (commitIdColumn.isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<GitColumnHandle> summary = constraint.getSummary().transformKeys(GitColumnHandle.class::cast);
        TupleDomain<GitColumnHandle> enforced = summary.filter((column, domain) -> isEnforceable(column, domain, commitIdColumn.orElseThrow()));
        TupleDomain<GitColumnHandle> newConstraint = handle.constraint().intersect(enforced);
        if (newConstraint.equals(handle.constraint())) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> unenforced = summary
                .filter((column, domain) -> !isEnforceable(column, domain, commitIdColumn.orElseThrow()))
                .transformKeys(ColumnHandle.class::cast);
        return Optional.of(new ConstraintApplicationResult<>(
                new GitTableHandle(handle.schemaName(), handle.tableName(), newConstraint),
                unenforced,
                constraint.getExpression(),
                true));
    }

    public static Optional<List<String>> getCommitIds(TupleDomain<GitColumnHandle> constraint, String commitIdColumn)
    {
        if (constraint.isNone()) {
            return Optional.of(List.of());
        }
        Optional<Domain> domain = constraint.getDomains().orElseThrow().entrySet().stream()
                .filter(entry -> entry.getKey().columnName().equals(commitIdColumn))
                .map(Map.Entry::getValue)
                .findFirst();
        if (domain.isEmpty() || !domain.orElseThrow().isNullableDiscreteSet()) {
            return Optional.empty();
        }
        // commit ids are never null, and always are lowercase, full length object ids,
        // so other values can never match and can be ignored
        return Optional.of(domain.orElseThrow().getNullableDiscreteSet().getNonNullValues().stream()
                .map(value -> ((Slice) value).toStringUtf8())
                .filter(OBJECT_ID)
                .sorted()
                .collect(toImmutableList()));
    }

    private static boolean isEnforceable(GitColumnHandle column, Domain domain, String commitIdColumn)
    {
        return column.columnName().equals(commitIdColumn) && domain.isNullableDiscreteSet();
    }
}
