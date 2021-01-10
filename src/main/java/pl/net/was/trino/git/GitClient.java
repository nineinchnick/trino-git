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

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class GitClient
{
    private final Map<String, List<GitColumn>> columns = Map.of(
            "branches", List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("name", VarcharType.VARCHAR),
                    new GitColumn("is_merged", BooleanType.BOOLEAN)),
            "commits", List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("author_name", VarcharType.VARCHAR),
                    new GitColumn("author_email", VarcharType.VARCHAR),
                    new GitColumn("committer_name", VarcharType.VARCHAR),
                    new GitColumn("committer_email", VarcharType.VARCHAR),
                    new GitColumn("message", VarcharType.VARCHAR),
                    new GitColumn("parents", new ArrayType(VarcharType.VARCHAR)),
                    new GitColumn("tree_id", VarcharType.VARCHAR),
                    new GitColumn("commit_time", TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS)),
            "diff_stats", List.of(
                    new GitColumn("commit_id", VarcharType.VARCHAR),
                    new GitColumn("old_commit_id", VarcharType.VARCHAR),
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("path_name", VarcharType.VARCHAR),
                    new GitColumn("old_path_name", VarcharType.VARCHAR),
                    new GitColumn("change_type", VarcharType.VARCHAR),
                    new GitColumn("similarity_score", IntegerType.INTEGER),
                    new GitColumn("added_lines", IntegerType.INTEGER),
                    new GitColumn("deleted_lines", IntegerType.INTEGER)),
            "tags", List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("name", VarcharType.VARCHAR)),
            "trees", List.of(
                    new GitColumn("commit_id", VarcharType.VARCHAR),
                    new GitColumn("object_type", VarcharType.VARCHAR),
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("file_name", VarcharType.VARCHAR),
                    new GitColumn("path_name", VarcharType.VARCHAR),
                    new GitColumn("attributes", VarcharType.VARCHAR),
                    new GitColumn("depth", IntegerType.INTEGER)),
            "objects", List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("contents", VarbinaryType.VARBINARY)));
    /*
    TODO implement this:
     */

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
}