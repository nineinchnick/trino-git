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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Ref;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;

public class GitRecordSet
        implements RecordSet
{
    private final List<GitColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final String tableName;
    private final Git repo;
    private final List<Ref> refs;
    private final Optional<List<String>> commitIds;
    private final Optional<List<String>> paths;

    public GitRecordSet(GitSplit split, GitTableHandle table, List<GitColumnHandle> columnHandles, Git repo)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (GitColumnHandle column : columnHandles) {
            types.add(column.columnType());
        }
        this.columnTypes = types.build();
        this.tableName = split.table().getTableName();
        this.commitIds = split.commitIds();
        this.paths = GitClient.Table.fromName(tableName).getPathColumn()
                .flatMap(pathColumn -> GitMetadata.getValues(table.constraint(), pathColumn));
        this.repo = requireNonNull(repo, "repo is null");
        try {
            this.refs = split.snapshot().getRefs(repo.getRepository());
        }
        catch (IOException e) {
            throw new TrinoException(GIT_READ_ERROR, "Failed to read refs", e);
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return GitClient.Table.fromName(tableName).getReader().createCursor(columnHandles, repo, refs, commitIds, paths);
    }
}
