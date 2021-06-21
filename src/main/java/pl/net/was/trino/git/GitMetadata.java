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
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.DiscreteValues;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class GitMetadata
        implements ConnectorMetadata
{
    private final GitClient gitClient;
    private String catalogName;

    @Inject
    public GitMetadata(GitClient gitClient)
    {
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
    public GitTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        GitTable table = gitClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new GitTableHandle(tableName.getSchemaName(), tableName.getTableName(), Optional.empty(), OptionalLong.empty());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(((GitTableHandle) table).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        if (optionalSchemaName.isPresent() && !gitClient.getSchemaNames().contains(optionalSchemaName.get())) {
            throw new SchemaNotFoundException(optionalSchemaName.get());
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

        GitTable table = gitClient.getTable(gitTableHandle.getSchemaName(), gitTableHandle.getTableName());
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
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
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

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return List.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((GitColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        if (optionalSchemaName.isPresent() && !gitClient.getSchemaNames().contains(optionalSchemaName.get())) {
            throw new SchemaNotFoundException(optionalSchemaName.get());
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
        return Optional.ofNullable(gitClient.getView(catalogName, viewName.getSchemaName(), viewName.getTableName()));
    }

    public void setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        GitTableHandle gitTableHandle = (GitTableHandle) tableHandle;
        String tableName = gitTableHandle.getTableName();

        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);
        TableStatistics.Builder builder = TableStatistics.builder();
        switch (tableName) {
            case "commits":
                builder.setRowCount(Estimate.of(1));
                builder.setColumnStatistics(columns.get("object_id"), ColumnStatistics.builder()
                        .setNullsFraction(Estimate.zero())
                        .setDistinctValuesCount(Estimate.of(1))
                        .setDataSize(Estimate.of(1000))
                        .build());
                break;
            case "trees":
                builder.setRowCount(Estimate.of(1000000));
                builder.setColumnStatistics(columns.get("commit_id"), ColumnStatistics.builder()
                        .setNullsFraction(Estimate.zero())
                        .setDistinctValuesCount(Estimate.of(1000000))
                        .setDataSize(Estimate.of(1000000000))
                        .build());
                break;
        }
        return builder.build();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        GitTableHandle handle = (GitTableHandle) table;

        Optional<List<String>> oldCommits = handle.getCommitIds();

        Optional<List<String>> commitIds = Optional.empty();
        TupleDomain<ColumnHandle> unenforcedConstraint = constraint.getSummary();

        Map<String, ColumnHandle> columns = getColumnHandles(session, handle);

        if (handle.getTableName().equals(GitClient.Table.trees.name())) {
            commitIds = getCommitIds(constraint.getSummary());
            unenforcedConstraint = constraint.getSummary().filter(
                    (columnHandle, domain) -> !columnHandle.equals(columns.get("commit_id")));
        }
        else if (handle.getTableName().equals(GitClient.Table.commits.name())) {
            // TODO merge both conditions, mapping table name to column (FK?)
            commitIds = getCommitIds(constraint.getSummary());
            unenforcedConstraint = constraint.getSummary().filter(
                    (columnHandle, domain) -> !columnHandle.equals(columns.get("object_id")));
        }

        if ((oldCommits.isEmpty() && commitIds.isEmpty()) ||
                (oldCommits.isPresent() && commitIds.isPresent() &&
                        oldCommits.get().size() == commitIds.get().size() && oldCommits.get().containsAll(commitIds.get()))) {
            return Optional.empty();
        }
        if (oldCommits.isEmpty()) {
            oldCommits = commitIds;
        }
        else if (commitIds.isPresent()) {
            oldCommits.get().addAll(commitIds.get());
        }

        return Optional.of(new ConstraintApplicationResult<>(
                new GitTableHandle(
                        handle.getSchemaName(),
                        handle.getTableName(),
                        oldCommits,
                        handle.getLimit()),
                unenforcedConstraint,
                true));
    }

    public static Optional<List<String>> getCommitIds(TupleDomain<ColumnHandle> constraintSummary)
    {
        if (constraintSummary.isNone() || constraintSummary.isAll()) {
            return Optional.empty();
        }

        for (Map.Entry<ColumnHandle, Domain> entry : constraintSummary.getDomains().get().entrySet()) {
            System.out.println(entry.getKey() + "/" + entry.getValue());
            GitColumnHandle column = ((GitColumnHandle) entry.getKey());
            // TODO this is ambiguous and should be passed as a param, since every call to getCommitIds has a table handle in scope
            if (!column.getColumnName().equals("commit_id") && !column.getColumnName().equals("object_id")) {
                continue;
            }
            Domain domain = entry.getValue();
            verify(!domain.isNone(), "Domain is none");
            if (domain.isAll()) {
                continue;
            }
            if (domain.isOnlyNull()) {
                return Optional.of(List.of());
            }
            if ((!domain.getValues().isNone() && domain.isNullAllowed()) || (domain.getValues().isAll() && !domain.isNullAllowed())) {
                continue;
            }
            if (domain.isSingleValue()) {
                String value = ((Slice) domain.getSingleValue()).toStringUtf8();
                return Optional.of(List.of(value));
            }
            ValueSet valueSet = domain.getValues();
            if (valueSet instanceof EquatableValueSet) {
                DiscreteValues discreteValues = valueSet.getDiscreteValues();
                return Optional.of(discreteValues.getValues().stream().map(value -> ((Slice) value).toStringUtf8()).collect(Collectors.toList()));
            }
            if (valueSet instanceof SortedRangeSet) {
                Ranges ranges = ((SortedRangeSet) valueSet).getRanges();
                List<Range> rangeList = ranges.getOrderedRanges();
                if (rangeList.stream().allMatch(Range::isSingleValue)) {
                    List<String> values = rangeList.stream()
                            .map(range -> ((Slice) range.getSingleValue()).toStringUtf8())
                            .collect(toImmutableList());
                    return Optional.of(values);
                }
                // ignore unbounded ranges
                return Optional.empty();
            }
            throw new IllegalStateException("Unexpected domain: " + domain);
        }
        return Optional.empty();
    }
}
