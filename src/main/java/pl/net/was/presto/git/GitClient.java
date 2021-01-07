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
package pl.net.was.presto.git;

import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.VarcharType;

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
                    new GitColumn("name", VarcharType.VARCHAR)),
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
                    new GitColumn("depth", IntegerType.INTEGER)));
    /*
    TODO implement this:
            "changes", List.of(
                    new GitColumn("commit_id", VarcharType.VARCHAR),
                    new GitColumn("file_name", VarcharType.VARCHAR),
                    new GitColumn("path_name", VarcharType.VARCHAR),
                    new GitColumn("insertions", IntegerType.INTEGER),
                    new GitColumn("deletions", IntegerType.INTEGER))
            "objects", List.of(
                    new GitColumn("object_id", VarcharType.VARCHAR),
                    new GitColumn("contents", VarbinaryType.VARBINARY))
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
