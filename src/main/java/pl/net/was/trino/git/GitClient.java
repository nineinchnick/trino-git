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

import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class GitClient
{
    public static enum Table {
        branches,
        commits,
        diff_stats,
        tags,
        trees,
        objects,
    }

    public static enum TreesColumns {
        commit_id,
    }

    public static enum View {
        idents,
        commit_stats,
    }

    private final Map<String, List<GitColumn>> columns = Map.of(
            Table.branches.name(), List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("name", VarcharType.VARCHAR),
                    new GitColumn("is_merged", BooleanType.BOOLEAN)),
            Table.commits.name(), List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("author_name", VarcharType.VARCHAR),
                    new GitColumn("author_email", VarcharType.VARCHAR),
                    new GitColumn("committer_name", VarcharType.VARCHAR),
                    new GitColumn("committer_email", VarcharType.VARCHAR),
                    new GitColumn("message", VarcharType.VARCHAR),
                    new GitColumn("parents", new ArrayType(VarcharType.VARCHAR)),
                    new GitColumn("tree_id", VarcharType.VARCHAR),
                    new GitColumn("commit_time", TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS)),
            Table.diff_stats.name(), List.of(
                    new GitColumn("commit_id", VarcharType.VARCHAR),
                    new GitColumn("old_commit_id", VarcharType.VARCHAR),
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("path_name", VarcharType.VARCHAR),
                    new GitColumn("old_path_name", VarcharType.VARCHAR),
                    new GitColumn("change_type", VarcharType.VARCHAR),
                    new GitColumn("similarity_score", IntegerType.INTEGER),
                    new GitColumn("added_lines", IntegerType.INTEGER),
                    new GitColumn("deleted_lines", IntegerType.INTEGER)),
            Table.tags.name(), List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("name", VarcharType.VARCHAR)),
            Table.trees.name(), List.of(
                    new GitColumn(TreesColumns.commit_id.name(), VarcharType.VARCHAR),
                    new GitColumn("object_type", VarcharType.VARCHAR),
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("file_name", VarcharType.VARCHAR),
                    new GitColumn("path_name", VarcharType.VARCHAR),
                    new GitColumn("attributes", VarcharType.VARCHAR),
                    new GitColumn("depth", IntegerType.INTEGER)),
            Table.objects.name(), List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("contents", VarbinaryType.VARBINARY)));

    Map<String, List<ConnectorViewDefinition.ViewColumn>> viewColumns = Map.of(
            View.idents.name(),
            List.of(
                    new ConnectorViewDefinition.ViewColumn("email", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("name", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("extra_emails", new ArrayType(VarcharType.VARCHAR).getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("extra_names", new ArrayType(VarcharType.VARCHAR).getTypeId(), Optional.empty())),
            View.commit_stats.name(),
            List.of(
                    new ConnectorViewDefinition.ViewColumn("object_id", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("author_name", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("author_email", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("committer_name", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("committer_email", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("message", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("parents", new ArrayType(VarcharType.VARCHAR).getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("tree_id", VarcharType.VARCHAR.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("commit_time", TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS.getTypeId(), Optional.empty()),

                    new ConnectorViewDefinition.ViewColumn("added_lines", BigintType.BIGINT.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("deleted_lines", BigintType.BIGINT.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("changed_files", BigintType.BIGINT.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("similarity_score", DoubleType.DOUBLE.getTypeId(), Optional.empty()),
                    new ConnectorViewDefinition.ViewColumn("change_types", new ArrayType(VarcharType.VARCHAR).getTypeId(), Optional.empty())));

    @Inject
    public GitClient(GitConfig config)
    {
        requireNonNull(config, "config is null");
    }

    public Set<String> getSchemaNames()
    {
        return Stream.of("default").collect(Collectors.toSet());
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

    public ConnectorViewDefinition getView(String catalog, String schema, String viewName)
    {
        if (!viewColumns.containsKey(viewName)) {
            return null;
        }
        String query;
        try {
            query = Resources.toString(Resources.getResource(getClass(), format("/sql/%s.sql", viewName)), UTF_8);
        }
        catch (IOException e) {
            return null;
        }
        return new ConnectorViewDefinition(
                query,
                Optional.of(catalog),
                Optional.of(schema),
                viewColumns.get(viewName),
                Optional.empty(),
                Optional.empty(),
                true);
    }
}
