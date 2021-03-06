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
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
    private Git repo;
    private final Optional<List<String>> commitIds;

    public GitRecordSet(GitSplit split, GitTableHandle table, List<GitColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (GitColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.tableName = split.getTableName();
        Optional<List<String>> splitCommits = split.getCommitIds();
        if (splitCommits.isEmpty()) {
            splitCommits = table.getCommitIds();
        }
        else if (table.getCommitIds().isPresent()) {
            splitCommits.get().addAll(table.getCommitIds().get());
        }
        this.commitIds = splitCommits;

        String url = split.getUri().toString();
        File localPath;
        try {
            localPath = ensureDir(url);
        }
        catch (IOException ignored) {
            return;
        }
        if (!localPath.exists()) {
            try {
                repo = Git.cloneRepository()
                        .setURI(url)
                        .setDirectory(localPath)
                        .call();
            }
            catch (GitAPIException ignored) {
                // pass
            }
        }
        else {
            try {
                repo = new Git(new FileRepositoryBuilder()
                        .setGitDir(new File(localPath, ".git"))
                        .build());
                repo.fetch().setCheckFetchedObjects(true).call();
            }
            catch (GitAPIException | IOException ignored) {
                // pass
            }
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
        Map<String, Class<?>> map = Map.of(
                "branches", BranchesRecordCursor.class,
                "commits", CommitsRecordCursor.class,
                "diff_stats", DiffStatsRecordCursor.class,
                "objects", ObjectsRecordCursor.class,
                "tags", TagsRecordCursor.class,
                "trees", TreesRecordCursor.class);
        Class<?> clazz = map.get(tableName);
        if (clazz == null) {
            return null;
        }
        Constructor<?> ctr;
        try {
            ctr = clazz.getConstructor(List.class, Git.class, Optional.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException("Missing cursor constructor", e);
        }
        try {
            return (RecordCursor) ctr.newInstance(columnHandles, repo, commitIds);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Unknown exception", e);
        }
    }

    public static File ensureDir(String prefix)
            throws IOException
    {
        String tmpDirStr = System.getProperty("java.io.tmpdir");
        if (tmpDirStr == null) {
            throw new IOException(
                    "System property 'java.io.tmpdir' does not specify a tmp dir");
        }

        File tmpDir = new File(tmpDirStr);
        if (!tmpDir.exists()) {
            boolean created = tmpDir.mkdirs();
            if (!created) {
                throw new IOException("Unable to create tmp dir " + tmpDir);
            }
        }

        return new File(tmpDir, Integer.toHexString(prefix.hashCode()));
    }
}
