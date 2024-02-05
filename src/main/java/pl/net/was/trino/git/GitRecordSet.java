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
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.RemoteConfig;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
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

        this.repo = getRepo(split.getUri());
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
                "branches", (columnHandles, repo, commitIds) -> new BranchesRecordCursor(columnHandles, repo),
                "commits", CommitsRecordCursor::new,
                "diff_stats", DiffStatsRecordCursor::new,
                "objects", (columnHandles, repo, commitIds) -> new ObjectsRecordCursor(columnHandles, repo),
                "tags", (columnHandles, repo, commitIds) -> new TagsRecordCursor(columnHandles, repo),
                "trees", TreesRecordCursor::new);
        RecordCursorProvider recordCursorProvider = map.get(tableName);
        if (recordCursorProvider == null) {
            return null;
        }
        return recordCursorProvider.create(columnHandles, repo, commitIds);
    }

    private Git getRepo(URI uri)
    {
        String url = uri.toString();
        File localPath;
        try {
            localPath = ensureDir(url);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (!localPath.exists()) {
            CloneCommand repo = Git.cloneRepository()
                    .setURI(url)
                    .setDirectory(localPath);
            if (uri.getUserInfo() != null && !uri.getUserInfo().isEmpty()) {
                String[] parts = uri.getUserInfo().split(":", 2);
                UsernamePasswordCredentialsProvider credentials = new UsernamePasswordCredentialsProvider(parts[0], parts.length > 1 ? parts[1] : "");
                repo.setCredentialsProvider(credentials);
            }
            try {
                return repo.call();
            }
            catch (GitAPIException e) {
                throw new RuntimeException(e);
            }
        }
        Repository fileRepo;
        try {
            fileRepo = new FileRepositoryBuilder()
                    .setGitDir(new File(localPath, ".git"))
                    .build();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        Git repo = new Git(fileRepo);
        try {
            List<RemoteConfig> remotes = repo.remoteList().call();
            if (!remotes.isEmpty()) {
                repo.fetch().setCheckFetchedObjects(true).call();
            }
        }
        catch (GitAPIException e) {
            throw new RuntimeException(e);
        }
        return repo;
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
