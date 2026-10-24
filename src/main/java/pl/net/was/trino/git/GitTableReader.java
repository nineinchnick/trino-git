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

import com.google.common.collect.Maps;
import io.trino.spi.connector.RecordCursor;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Ref;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Reads the rows of one table, and defines its columns.
 */
public interface GitTableReader<T>
{
    List<TableColumn<T>> getColumns();

    /**
     * Reads the rows reachable from the refs, or only for the selected commits or objects, if any.
     * Expensive values are read only when their column is read, and only until moving to the next row.
     */
    RowIterator<T> read(Git repo, List<Ref> refs, Optional<List<String>> ids, Optional<List<String>> paths);

    default RecordCursor createCursor(List<GitColumnHandle> columnHandles, Git repo, List<Ref> refs, Optional<List<String>> ids, Optional<List<String>> paths)
    {
        Map<String, TableColumn<T>> columnsByName = Maps.uniqueIndex(getColumns(), column -> column.column().name());
        List<TableColumn<T>> selected = columnHandles.stream()
                .map(handle -> {
                    TableColumn<T> column = columnsByName.get(handle.columnName());
                    checkArgument(column != null, "Unknown column: %s", handle.columnName());
                    return column;
                })
                .collect(toImmutableList());
        return new GitRecordCursor<>(
                read(repo, refs, ids, paths),
                selected.stream().map(column -> column.column().type()).collect(toImmutableList()),
                selected.stream().map(TableColumn::getter).collect(toImmutableList()));
    }
}
