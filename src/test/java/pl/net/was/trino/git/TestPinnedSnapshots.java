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

import io.airlift.units.Duration;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPinnedSnapshots
{
    @Test
    public void testPinsRefsPerTransaction(@TempDir Path tempDir)
            throws Exception
    {
        Path source = tempDir.resolve("source");
        try (Git git = Git.init().setDirectory(source.toFile()).setInitialBranch("main").call()) {
            RevCommit first = git.commit().setMessage("first").setAllowEmpty(true).setSign(false).call();
            GitRepositoryManager repositoryManager = new GitRepositoryManager(new GitConfig()
                    .setUrl(source.toUri())
                    .setCacheDirectory(tempDir.resolve("coordinator").toFile())
                    .setFetchInterval(new Duration(1, HOURS)));
            RefreshGenerations refreshGenerations = new RefreshGenerations();
            PinnedSnapshots pinnedSnapshots = new PinnedSnapshots(repositoryManager, refreshGenerations);

            GitTransactionHandle firstTransaction = new GitTransactionHandle(randomUUID());
            Snapshot firstSnapshot = pinnedSnapshots.get(firstTransaction, "default");
            assertThat(firstSnapshot.refs().orElseThrow())
                    .containsEntry("refs/heads/main", first.getName())
                    .containsEntry("HEAD", first.getName());

            RevCommit second = git.commit().setMessage("second").setAllowEmpty(true).setSign(false).call();
            refreshGenerations.refresh("default");
            // scans in the same transaction keep reading the same commits
            assertThat(pinnedSnapshots.get(firstTransaction, "default")).isEqualTo(firstSnapshot);

            Snapshot secondSnapshot = pinnedSnapshots.get(new GitTransactionHandle(randomUUID()), "default");
            assertThat(secondSnapshot.refs().orElseThrow()).containsEntry("refs/heads/main", second.getName());
            assertThat(secondSnapshot.refreshGeneration()).isGreaterThan(firstSnapshot.refreshGeneration());

            pinnedSnapshots.release(firstTransaction);
            assertThat(pinnedSnapshots.get(firstTransaction, "default")).isNotEqualTo(firstSnapshot);
            repositoryManager.close();
        }
    }

    @Test
    public void testFetchesMissingPinnedCommits(@TempDir Path tempDir)
            throws Exception
    {
        Path source = tempDir.resolve("source");
        try (Git git = Git.init().setDirectory(source.toFile()).setInitialBranch("main").call()) {
            git.commit().setMessage("first").setAllowEmpty(true).setSign(false).call();
            GitRepositoryManager worker = new GitRepositoryManager(new GitConfig()
                    .setUrl(source.toUri())
                    .setCacheDirectory(tempDir.resolve("worker").toFile())
                    .setFetchInterval(new Duration(1, HOURS)));
            worker.getRepository("default", Snapshot.latest(0)).close();

            // the coordinator pinned a commit this worker hasn't fetched yet
            RevCommit second = git.commit().setMessage("second").setAllowEmpty(true).setSign(false).call();
            Snapshot snapshot = new Snapshot(0, Optional.of(Map.of("refs/heads/main", second.getName())));
            Repository repository = worker.getRepository("default", snapshot).getRepository();
            assertThat(repository.getObjectDatabase().has(second)).isTrue();
            assertThat(repository.resolve("refs/heads/main")).isEqualTo(second);
            worker.close();
        }
    }
}
