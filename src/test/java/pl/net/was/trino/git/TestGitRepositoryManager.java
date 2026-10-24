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

import com.google.common.collect.Streams;
import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.trino.git.GitErrorCode.GIT_CLONE_ERROR;
import static pl.net.was.trino.git.GitErrorCode.GIT_OPEN_ERROR;

public class TestGitRepositoryManager
{
    @Test
    public void testCloneMissingRepository()
    {
        GitRepositoryManager repositoryManager = new GitRepositoryManager(new GitConfig().setUrl(URI.create("file:///nonexistent/trino-git/repo")));
        assertTrinoExceptionThrownBy(repositoryManager::getRepository)
                .hasErrorCode(GIT_CLONE_ERROR)
                .hasMessage("Failed to clone the repository");
    }

    @Test
    public void testCredentialsAreNotStored()
            throws Exception
    {
        URI url = TestGitClient.setupRepo(URI.create("credentials-source.invalid"));
        deleteClone(url);

        GitRepositoryManager repositoryManager = new GitRepositoryManager(new GitConfig()
                .setUrl(url)
                .setUsername("user")
                .setPassword("secret-password"));
        try (Git repo = repositoryManager.getRepository()) {
            Path config = repo.getRepository().getDirectory().toPath().resolve("config");
            assertThat(Files.readString(config))
                    .contains(url.toString())
                    .doesNotContain("secret-password");
        }
    }

    @Test
    public void testCacheDirectoryAccessibleByOthers(@TempDir Path tempDir)
            throws IOException
    {
        Path cacheDirectory = Files.createDirectory(tempDir.resolve("cache"), PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---")));
        GitRepositoryManager repositoryManager = new GitRepositoryManager(new GitConfig()
                .setUrl(URI.create("file:///nonexistent/trino-git/repo"))
                .setCacheDirectory(cacheDirectory.toFile()));
        assertTrinoExceptionThrownBy(repositoryManager::getRepository)
                .hasErrorCode(GIT_OPEN_ERROR)
                .hasMessageContaining("must be accessible only by its owner");
    }

    @Test
    public void testCacheDirectoryCreatedForOwnerOnly(@TempDir Path tempDir)
            throws IOException
    {
        Path cacheDirectory = tempDir.resolve("cache");
        GitRepositoryManager.getLocalPath(cacheDirectory, URI.create("https://example.com/repo.git"));
        assertThat(Files.getPosixFilePermissions(cacheDirectory)).isEqualTo(PosixFilePermissions.fromString("rwx------"));
    }

    @Test
    public void testConcurrentClones()
            throws Exception
    {
        URI url = TestGitClient.setupRepo(URI.create("concurrent-source.invalid"));
        deleteClone(url);

        int threads = 16;
        ExecutorService executor = newFixedThreadPool(threads);
        try {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Future<Long>> commitCounts = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                commitCounts.add(executor.submit(() -> {
                    // separate instances, like separate catalogs using the same repository
                    GitRepositoryManager repositoryManager = new GitRepositoryManager(new GitConfig().setUrl(url));
                    barrier.await();
                    try (Git repo = repositoryManager.getRepository()) {
                        return Streams.stream(repo.log().all().call()).count();
                    }
                }));
            }
            for (Future<Long> commitCount : commitCounts) {
                assertThat(commitCount.get()).isEqualTo(2);
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testReplacesIncompleteClone()
            throws Exception
    {
        URI url = TestGitClient.setupRepo(URI.create("incomplete-source.invalid"));
        deleteClone(url);
        Path clone = GitRepositoryManager.getLocalPath(new GitConfig().getCacheDirectory().toPath(), url);
        Files.createDirectories(clone.resolve(".git"));

        try (Git repo = new GitRepositoryManager(new GitConfig().setUrl(url)).getRepository()) {
            assertThat(repo.log().all().call()).hasSize(2);
        }
    }

    @Test
    public void testReplacesCloneOfAnotherRepository()
            throws Exception
    {
        URI url = TestGitClient.setupRepo(URI.create("replaced-source.invalid"));
        deleteClone(url);
        Path clone = GitRepositoryManager.getLocalPath(new GitConfig().getCacheDirectory().toPath(), url);
        Git.init().setDirectory(clone.toFile()).call().close();

        try (Git repo = new GitRepositoryManager(new GitConfig().setUrl(url)).getRepository()) {
            assertThat(repo.log().all().call()).hasSize(2);
        }
    }

    private static void deleteClone(URI url)
            throws IOException
    {
        Path clone = GitRepositoryManager.getLocalPath(new GitConfig().getCacheDirectory().toPath(), url);
        if (Files.exists(clone)) {
            deleteRecursively(clone, ALLOW_INSECURE);
        }
    }
}
