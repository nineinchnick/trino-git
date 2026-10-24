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

import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.MergeResult;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestGitDiffStats
{
    private static final List<GitColumnHandle> COLUMNS = List.of(
            new GitColumnHandle("commit_id", VARCHAR, 0),
            new GitColumnHandle("old_commit_id", VARCHAR, 1),
            new GitColumnHandle("path_name", VARCHAR, 2),
            new GitColumnHandle("change_type", VARCHAR, 3),
            new GitColumnHandle("added_lines", BIGINT, 4));

    private GitRepositoryManager repositoryManager;
    private RevCommit root;
    private RevCommit feature;
    private RevCommit main;
    private RevCommit merge;

    @BeforeAll
    public void setUp(@TempDir Path tempDir)
            throws Exception
    {
        Path source = tempDir.resolve("source");
        try (Git git = Git.init().setDirectory(source.toFile()).setInitialBranch("main").call()) {
            root = commit(git, source, "root.txt", 1);
            git.checkout().setCreateBranch(true).setName("feature").call();
            feature = commit(git, source, "feature.txt", 2);
            git.checkout().setName("main").call();
            main = commit(git, source, "main.txt", 3);
            MergeResult result = git.merge()
                    .include(git.getRepository().exactRef("refs/heads/feature"))
                    .setCommit(false)
                    .call();
            assertThat(result.getMergeStatus().isSuccessful()).isTrue();
            merge = git.commit().setMessage("merge").setAuthor(ident(4)).setCommitter(ident(4)).setSign(false).call();
        }
        repositoryManager = new GitRepositoryManager(new GitConfig()
                .setUrl(source.toUri())
                .setCacheDirectory(tempDir.resolve("cache").toFile()));
    }

    @Test
    public void testComparesCommitsWithFirstParent()
    {
        assertThat(read(Optional.empty())).containsExactlyInAnyOrder(
                row(root, null, "root.txt", "added", 1),
                row(feature, root, "feature.txt", "added", 1),
                row(main, root, "main.txt", "added", 1),
                row(merge, main, "feature.txt", "added", 1));
    }

    @Test
    public void testReadsSelectedCommits()
    {
        assertThat(read(Optional.of(List.of(main.getName())))).containsExactly(
                row(main, root, "main.txt", "added", 1));
        assertThat(read(Optional.of(List.of(root.getName(), merge.getName())))).containsExactlyInAnyOrder(
                row(root, null, "root.txt", "added", 1),
                row(merge, main, "feature.txt", "added", 1));
    }

    @Test
    public void testCopies(@TempDir Path tempDir)
            throws Exception
    {
        Path source = tempDir.resolve("source");
        String contents = "a file long enough to be compared for similarity\n".repeat(10);
        RevCommit copied;
        RevCommit moved;
        try (Git git = Git.init().setDirectory(source.toFile()).setInitialBranch("main").call()) {
            Files.writeString(source.resolve("original.txt"), contents);
            git.add().addFilepattern(".").call();
            git.commit().setMessage("original").setAuthor(ident(1)).setCommitter(ident(1)).setSign(false).call();
            // copying a file that is kept is an added file
            Files.writeString(source.resolve("copy.txt"), contents);
            git.add().addFilepattern(".").call();
            copied = git.commit().setMessage("copy").setAuthor(ident(2)).setCommitter(ident(2)).setSign(false).call();
            // moving a file to more than one path is a rename and copies
            Files.delete(source.resolve("original.txt"));
            Files.writeString(source.resolve("first.txt"), contents);
            Files.writeString(source.resolve("second.txt"), contents);
            git.add().addFilepattern(".").call();
            git.rm().addFilepattern("original.txt").call();
            moved = git.commit().setMessage("move").setAuthor(ident(3)).setCommitter(ident(3)).setSign(false).call();
        }
        GitRepositoryManager manager = new GitRepositoryManager(new GitConfig()
                .setUrl(source.toUri())
                .setCacheDirectory(tempDir.resolve("cache").toFile()));
        try (RecordCursor cursor = new DiffStatsTable().createCursor(
                List.of(new GitColumnHandle("commit_id", VARCHAR, 0), new GitColumnHandle("path_name", VARCHAR, 1), new GitColumnHandle("change_type", VARCHAR, 2)),
                manager.getRepository("default", Snapshot.latest(0)),
                manager.getRepository("default", Snapshot.latest(0)).getRepository().getRefDatabase().getRefs(),
                Optional.of(List.of(copied.getName(), moved.getName())),
                Optional.empty())) {
            List<List<Object>> rows = new ArrayList<>();
            while (cursor.advanceNextPosition()) {
                rows.add(List.of(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8(), cursor.getSlice(2).toStringUtf8()));
            }
            assertThat(rows).containsExactlyInAnyOrder(
                    List.of(copied.getName(), "copy.txt", "added"),
                    List.of(moved.getName(), "first.txt", "renamed"),
                    List.of(moved.getName(), "second.txt", "copied"));
        }
        manager.close();
    }

    private List<List<Object>> read(Optional<List<String>> commitIds)
    {
        GitRecordSet recordSet = new GitRecordSet(
                new GitSplit(new SchemaTableName("default", "diff_stats"), commitIds, Snapshot.latest(0)),
                new GitTableHandle("default", "diff_stats", TupleDomain.all()),
                COLUMNS,
                repositoryManager.getRepository("default", Snapshot.latest(0)));
        List<List<Object>> rows = new ArrayList<>();
        try (RecordCursor cursor = recordSet.cursor()) {
            while (cursor.advanceNextPosition()) {
                List<Object> row = new ArrayList<>();
                row.add(cursor.getSlice(0).toStringUtf8());
                row.add(cursor.isNull(1) ? null : cursor.getSlice(1).toStringUtf8());
                row.add(cursor.getSlice(2).toStringUtf8());
                row.add(cursor.getSlice(3).toStringUtf8());
                row.add(cursor.getLong(4));
                rows.add(row);
            }
        }
        return rows;
    }

    private static List<Object> row(RevCommit commit, RevCommit parent, String path, String changeType, long addedLines)
    {
        List<Object> row = new ArrayList<>();
        row.add(commit.getName());
        row.add(parent == null ? null : parent.getName());
        row.add(path);
        row.add(changeType);
        row.add(addedLines);
        return row;
    }

    private static RevCommit commit(Git git, Path workTree, String file, int time)
            throws Exception
    {
        Files.writeString(workTree.resolve(file), file + "\n");
        git.add().addFilepattern(file).call();
        return git.commit().setMessage(file).setAuthor(ident(time)).setCommitter(ident(time)).setSign(false).call();
    }

    private static PersonIdent ident(int time)
    {
        return new PersonIdent("test", "test@example.com", Instant.ofEpochSecond(1_600_000_000L + time), UTC);
    }
}
