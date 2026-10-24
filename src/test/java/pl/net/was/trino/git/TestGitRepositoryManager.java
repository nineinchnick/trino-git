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
import com.sun.net.httpserver.HttpServer;
import io.airlift.units.Duration;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jgit.lib.Constants.HEAD;
import static pl.net.was.trino.git.GitErrorCode.GIT_AUTHENTICATION_ERROR;
import static pl.net.was.trino.git.GitErrorCode.GIT_OPEN_ERROR;
import static pl.net.was.trino.git.GitErrorCode.GIT_REPOSITORY_NOT_FOUND;

public class TestGitRepositoryManager
{
    @Test
    public void testCloneMissingRepository()
    {
        GitRepositoryManager repositoryManager = new GitRepositoryManager(new GitConfig().setUrl(URI.create("file:///nonexistent/trino-git/repo")));
        assertTrinoExceptionThrownBy(() -> repositoryManager.getRepository("default", Snapshot.latest(0)))
                .hasErrorCode(GIT_REPOSITORY_NOT_FOUND)
                .hasMessage("Failed to clone file:///nonexistent/trino-git/repo: repository not found");
    }

    @Test
    public void testFetchRemovedRepository(@TempDir Path tempDir)
            throws Exception
    {
        Path source = tempDir.resolve("source");
        Git.init().setDirectory(source.toFile()).call().close();
        URI url = source.toUri();
        GitRepositoryManager repositoryManager = new GitRepositoryManager(new GitConfig()
                .setUrl(url)
                .setFetchInterval(new Duration(0, SECONDS))
                .setCacheDirectory(tempDir.resolve("cache").toFile()));
        repositoryManager.getRepository("default", Snapshot.latest(0)).close();

        deleteRecursively(source, ALLOW_INSECURE);
        assertTrinoExceptionThrownBy(() -> repositoryManager.getRepository("default", Snapshot.latest(0)))
                .hasErrorCode(GIT_REPOSITORY_NOT_FOUND)
                .hasMessage("Failed to fetch %s: repository not found".formatted(url));
    }

    @Test
    public void testAuthenticationFailure(@TempDir Path tempDir)
            throws Exception
    {
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            exchange.getResponseHeaders().add("WWW-Authenticate", "Basic realm=\"git\"");
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
        });
        server.start();
        try {
            URI url = URI.create("http://localhost:%d/repo.git".formatted(server.getAddress().getPort()));
            for (GitConfig config : List.of(
                    new GitConfig(),
                    new GitConfig().setUsername("user").setPassword("wrong"))) {
                GitRepositoryManager repositoryManager = new GitRepositoryManager(config
                        .setUrl(url)
                        .setCacheDirectory(tempDir.resolve("cache").toFile()));
                assertTrinoExceptionThrownBy(() -> repositoryManager.getRepository("default", Snapshot.latest(0)))
                        .hasErrorCode(GIT_AUTHENTICATION_ERROR)
                        .hasMessage("Failed to clone %s: authentication failed, check git.username and git.password".formatted(url));
            }
        }
        finally {
            server.stop(0);
        }
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
        try (Git repo = repositoryManager.getRepository("default", Snapshot.latest(0))) {
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
        assertTrinoExceptionThrownBy(() -> repositoryManager.getRepository("default", Snapshot.latest(0)))
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
                    try (Git repo = repositoryManager.getRepository("default", Snapshot.latest(0))) {
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

        try (Git repo = new GitRepositoryManager(new GitConfig().setUrl(url)).getRepository("default", Snapshot.latest(0))) {
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

        try (Git repo = new GitRepositoryManager(new GitConfig().setUrl(url)).getRepository("default", Snapshot.latest(0))) {
            assertThat(repo.log().all().call()).hasSize(2);
        }
    }

    @Test
    public void testFollowsRemoteBranches()
            throws Exception
    {
        URI url = TestGitClient.setupRepo(URI.create("following-source.invalid"));
        deleteClone(url);
        GitRepositoryManager repositoryManager = new GitRepositoryManager(new GitConfig().setUrl(url).setFetchInterval(new Duration(0, SECONDS)));
        repositoryManager.getRepository("default", Snapshot.latest(0)).close();

        try (Git source = Git.open(Path.of(url).toFile())) {
            RevCommit commit = source.commit().setMessage("New commit").setAllowEmpty(true).setSign(false).call();
            source.branchCreate().setName("feature").call();
            source.getRepository().updateRef(HEAD).link("refs/heads/feature");

            try (Git repo = repositoryManager.getRepository("default", Snapshot.latest(0))) {
                assertThat(repo.getRepository().exactRef("refs/heads/master").getObjectId()).isEqualTo(commit.getId());
                assertThat(repo.getRepository().exactRef("refs/heads/feature").getObjectId()).isEqualTo(commit.getId());
                assertThat(repo.getRepository().exactRef(HEAD).getTarget().getName()).isEqualTo("refs/heads/feature");
                assertThat(repo.getRepository().getRefDatabase().getRefsByPrefix("refs/remotes/")).isEmpty();
            }

            source.getRepository().updateRef(HEAD).link("refs/heads/master");
            source.branchDelete().setBranchNames("feature").setForce(true).call();
            try (Git repo = repositoryManager.getRepository("default", Snapshot.latest(0))) {
                assertThat(repo.getRepository().exactRef("refs/heads/feature")).isNull();
                assertThat(repo.getRepository().exactRef(HEAD).getTarget().getName()).isEqualTo("refs/heads/master");
            }
        }
    }

    @Test
    public void testFetchInterval(@TempDir Path tempDir)
            throws Exception
    {
        Path source = tempDir.resolve("source");
        try (Git git = Git.init().setDirectory(source.toFile()).setInitialBranch("main").call()) {
            RevCommit first = git.commit().setMessage("first").setAllowEmpty(true).setSign(false).call();
            GitConfig config = new GitConfig()
                    .setUrl(source.toUri())
                    .setCacheDirectory(tempDir.resolve("cache").toFile());
            GitRepositoryManager cachingManager = new GitRepositoryManager(config.setFetchInterval(new Duration(1, HOURS)));
            Repository repository = cachingManager.getRepository("default", Snapshot.latest(0)).getRepository();
            assertThat(repository.resolve("refs/heads/main")).isEqualTo(first);

            RevCommit second = git.commit().setMessage("second").setAllowEmpty(true).setSign(false).call();
            // the open repository is reused, and not fetched again
            assertThat(cachingManager.getRepository("default", Snapshot.latest(0)).getRepository()).isSameAs(repository);
            assertThat(repository.resolve("refs/heads/main")).isEqualTo(first);

            GitRepositoryManager fetchingManager = new GitRepositoryManager(config.setFetchInterval(new Duration(0, SECONDS)));
            assertThat(fetchingManager.getRepository("default", Snapshot.latest(0)).getRepository().resolve("refs/heads/main")).isEqualTo(second);
            cachingManager.close();
            fetchingManager.close();
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
