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
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class GitClient
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private final Set<String> schemaNames;

    public enum Table
    {
        BRANCHES(Optional.empty(), false, Optional.empty()),
        COMMITS(Optional.of("object_id"), true, Optional.empty()),
        DIFF_STATS(Optional.of("commit_id"), true, Optional.of("path_name")),
        IDENTS(Optional.empty(), false, Optional.empty()),
        TAGS(Optional.empty(), false, Optional.empty()),
        TREES(Optional.of("commit_id"), true, Optional.of("path_name")),
        OBJECTS(Optional.of("object_id"), false, Optional.empty());

        private final Optional<String> idColumn;
        private final boolean splitByCommits;
        private final Optional<String> pathColumn;

        Table(Optional<String> idColumn, boolean splitByCommits, Optional<String> pathColumn)
        {
            this.idColumn = idColumn;
            this.splitByCommits = splitByCommits;
            this.pathColumn = pathColumn;
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

    private final Map<String, List<GitColumn>> columns = Map.of(
            Table.BRANCHES.getName(), List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR, "Id of the commit the branch points to"),
                    new GitColumn("name", VarcharType.VARCHAR, "Full ref name, like refs/heads/main"),
                    new GitColumn("is_merged", BooleanType.BOOLEAN, "Whether the branch is merged into the HEAD commit")),
            Table.COMMITS.getName(), List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR, "Commit id"),
                    new GitColumn("author_name", VarcharType.VARCHAR, "Author name"),
                    new GitColumn("author_email", VarcharType.VARCHAR, "Author email"),
                    new GitColumn("committer_name", VarcharType.VARCHAR, "Committer name"),
                    new GitColumn("committer_email", VarcharType.VARCHAR, "Committer email"),
                    new GitColumn("message", VarcharType.VARCHAR, "Full commit message"),
                    new GitColumn("parents", new ArrayType(VarcharType.VARCHAR), "Ids of the parent commits"),
                    new GitColumn("tree_id", VarcharType.VARCHAR, "Id of the root tree"),
                    new GitColumn("commit_time", TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS, "Time when the commit was made, in the committer's time zone"),
                    new GitColumn("author_time", TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS, "Time when the change was originally made, in the author's time zone, which differs from commit_time after a rebase or a cherry-pick")),
            Table.IDENTS.getName(), List.of(
                    new GitColumn("email", VarcharType.VARCHAR, "Main email"),
                    new GitColumn("name", VarcharType.VARCHAR, "Main name"),
                    new GitColumn("extra_emails", new ArrayType(VarcharType.VARCHAR), "Other emails used by the same person"),
                    new GitColumn("extra_names", new ArrayType(VarcharType.VARCHAR), "Other names used by the same person")),
            Table.DIFF_STATS.getName(), List.of(
                    new GitColumn("commit_id", VarcharType.VARCHAR, "Commit id"),
                    new GitColumn("old_commit_id", VarcharType.VARCHAR, "Id of the first parent commit, NULL for a root commit"),
                    new GitColumn("object_id", VarcharType.VARCHAR, "Id of the new file contents, NULL for a deleted file"),
                    new GitColumn("old_object_id", VarcharType.VARCHAR, "Id of the old file contents, NULL for an added file"),
                    new GitColumn("path_name", VarcharType.VARCHAR, "New file path, NULL for a deleted file"),
                    new GitColumn("old_path_name", VarcharType.VARCHAR, "Old file path, NULL for an added file"),
                    new GitColumn("change_type", VarcharType.VARCHAR, "One of added, modified, deleted, renamed, copied"),
                    new GitColumn("similarity_score", IntegerType.INTEGER, "Similarity of the old and new file for renames and copies, 0 to 100"),
                    new GitColumn("added_lines", IntegerType.INTEGER, "Number of added lines, NULL for binary files"),
                    new GitColumn("deleted_lines", IntegerType.INTEGER, "Number of deleted lines, NULL for binary files")),
            Table.TAGS.getName(), List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR, "Id of the object the tag points to: a tag object or a commit"),
                    new GitColumn("commit_id", VarcharType.VARCHAR, "Id of the tagged commit, NULL if the tag points to a tree or a blob"),
                    new GitColumn("name", VarcharType.VARCHAR, "Full ref name, like refs/tags/v1.0"),
                    new GitColumn("tag_time", TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS, "Time of an annotated tag, NULL for lightweight tags and tags without one")),
            Table.TREES.getName(), List.of(
                    new GitColumn("commit_id", VarcharType.VARCHAR, "Commit id"),
                    new GitColumn("file_mode", VarcharType.VARCHAR, "One of file, executable, symlink, submodule, directory"),
                    new GitColumn("mode", VarcharType.VARCHAR, "Raw file mode in octal, like 100644, the same as in git ls-tree"),
                    new GitColumn("object_id", VarcharType.VARCHAR, "Id of the file contents, the directory tree, or the submodule commit"),
                    new GitColumn("file_name", VarcharType.VARCHAR, "File name"),
                    new GitColumn("path_name", VarcharType.VARCHAR, "Full file path"),
                    new GitColumn("attributes", new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR, TYPE_OPERATORS), "Git attributes of the file from .gitattributes, true or false for attributes that are set or unset"),
                    new GitColumn("depth", IntegerType.INTEGER, "Number of parent directories of the file")),
            Table.OBJECTS.getName(), List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR, "Blob id"),
                    new GitColumn("contents", VarbinaryType.VARBINARY, "File contents, NULL for blobs too large to load, over 50 MB by default")));

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
