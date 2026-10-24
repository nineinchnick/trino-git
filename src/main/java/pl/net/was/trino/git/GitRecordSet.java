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
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import org.eclipse.jgit.api.Git;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GitRecordSet
        implements RecordSet
{
    private final List<GitColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final String tableName;
    private final Git repo;
    private final Optional<List<String>> commitIds;

    public GitRecordSet(GitSplit split, GitTableHandle table, List<GitColumnHandle> columnHandles, Git repo)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (GitColumnHandle column : columnHandles) {
            types.add(column.columnType());
        }
        this.columnTypes = types.build();
        this.tableName = split.tableName();
        this.commitIds = split.commitIds();

        this.repo = requireNonNull(repo, "repo is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        Map<String, RecordCursorProvider> map = Map.of(
                "branches", (columnHandles, repo, _) -> new BranchesRecordCursor(columnHandles, repo),
                "commits", CommitsRecordCursor::new,
                "diff_stats", DiffStatsRecordCursor::new,
                "idents", (columnHandles, repo, _) -> new IdentsRecordCursor(columnHandles, repo),
                "objects", (columnHandles, repo, _) -> new ObjectsRecordCursor(columnHandles, repo),
                "tags", (columnHandles, repo, _) -> new TagsRecordCursor(columnHandles, repo),
                "trees", TreesRecordCursor::new);
        RecordCursorProvider recordCursorProvider = map.get(tableName);
        if (recordCursorProvider == null) {
            return null;
        }
        return recordCursorProvider.create(columnHandles, repo, commitIds);
    }
}
