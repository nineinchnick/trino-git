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
import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class GitClient
{
    private final Set<String> schemaNames;

    public enum Table
    {
        BRANCHES(new BranchesTable(), Optional.empty(), false, Optional.empty()),
        COMMITS(new CommitsTable(), Optional.of("object_id"), true, Optional.empty()),
        DIFF_STATS(new DiffStatsTable(), Optional.of("commit_id"), true, Optional.of("path_name")),
        IDENTS(new IdentsTable(), Optional.empty(), false, Optional.empty()),
        TAGS(new TagsTable(), Optional.empty(), false, Optional.empty()),
        TREES(new TreesTable(), Optional.of("commit_id"), true, Optional.of("path_name")),
        OBJECTS(new ObjectsTable(), Optional.of("object_id"), false, Optional.empty());

        private final GitTableReader<?> reader;
        private final Optional<String> idColumn;
        private final boolean splitByCommits;
        private final Optional<String> pathColumn;

        Table(GitTableReader<?> reader, Optional<String> idColumn, boolean splitByCommits, Optional<String> pathColumn)
        {
            this.reader = reader;
            this.idColumn = idColumn;
            this.splitByCommits = splitByCommits;
            this.pathColumn = pathColumn;
        }

        public GitTableReader<?> getReader()
        {
            return reader;
        }

        /**
         * Column with the id of the commit or object each row was read from, used to read only the ones matching a filter.
         */
        public Optional<String> getIdColumn()
        {
            return idColumn;
        }

        /**
         * Whether the table reads every commit separately, so its scans can be divided into splits by commits.
         */
        public boolean isSplitByCommits()
        {
            return splitByCommits;
        }

        /**
         * Column with the path of a file, used to skip other files, but not enforced, because directories or renames
         * of matching files can be read too.
         */
        public Optional<String> getPathColumn()
        {
            return pathColumn;
        }

        public String getName()
        {
            return name().toLowerCase(ENGLISH);
        }

        public static Table fromName(String name)
        {
            return valueOf(name.toUpperCase(ENGLISH));
        }
    }

    public enum View
    {
        COMMIT_STATS;

        public String getName()
        {
            return name().toLowerCase(ENGLISH);
        }
    }

    private final Map<String, List<GitColumn>> columns = Stream.of(Table.values())
            .collect(toImmutableMap(Table::getName, table -> table.getReader().getColumns().stream().map(TableColumn::column).collect(toImmutableList())));

    private final Map<String, List<ConnectorViewDefinition.ViewColumn>> viewColumns = Map.of(
            View.COMMIT_STATS.getName(), List.of(
                    new ConnectorViewDefinition.ViewColumn("object_id", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("author_name", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("author_email", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("committer_name", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("committer_email", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("message", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("parents", new ArrayType(VarcharType.VARCHAR).getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("tree_id", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("commit_time", TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("author_time", TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS.getTypeId(), Optional.empty()),

                    new ConnectorViewDefinition.ViewColumn("added_lines", BigintType.BIGINT.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("deleted_lines", BigintType.BIGINT.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("changed_files", BigintType.BIGINT.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("similarity_score", DoubleType.DOUBLE.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("change_types", new ArrayType(VarcharType.VARCHAR).getTypeId(), Optional.empty())));

    @Inject
    public GitClient(GitConfig config)
    {
        this.schemaNames = ImmutableSet.copyOf(config.getAllRepositories().keySet());
    }

    public Set<String> getSchemaNames()
    {
        return schemaNames;
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");

        return columns.keySet();
    }

    public GitTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");

        List<GitColumn> selected = columns.get(tableName);
        if (selected == null) {
            return null;
        }
        return new GitTable(tableName, selected);
    }

    public Set<String> getViewNames()
    {
        return viewColumns.keySet();
    }

    public ConnectorViewDefinition getView(String catalog, String schema, String viewName)
    {
        if (!viewColumns.containsKey(viewName)) {
            return null;
        }
        String query;
        try {
            query = Resources.toString(Resources.getResource(getClass(), "/sql/%s.sql".formatted(viewName)), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new ConnectorViewDefinition(
                query,
                Optional.of(catalog),
                Optional.of(schema),
                viewColumns.get(viewName),
                Optional.empty(),
                Optional.empty(),
                true,
                List.of());
    }
}
