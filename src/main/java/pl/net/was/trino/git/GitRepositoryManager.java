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

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import jakarta.annotation.PreDestroy;
import org.eclipse.jgit.api.FetchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.NoRemoteRepositoryException;
import org.eclipse.jgit.errors.TransportException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.FetchResult;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.RemoteConfig;
import org.eclipse.jgit.transport.TagOpt;
import org.eclipse.jgit.transport.TrackingRefUpdate;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.base.Throwables.getRootCause;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.eclipse.jgit.lib.ConfigConstants.CONFIG_FETCH_SECTION;
import static org.eclipse.jgit.lib.ConfigConstants.CONFIG_KEY_AUTOGC;
import static org.eclipse.jgit.lib.Constants.HEAD;
import static pl.net.was.trino.git.GitErrorCode.GIT_AUTHENTICATION_ERROR;
import static pl.net.was.trino.git.GitErrorCode.GIT_CLONE_ERROR;
import static pl.net.was.trino.git.GitErrorCode.GIT_FETCH_ERROR;
import static pl.net.was.trino.git.GitErrorCode.GIT_OPEN_ERROR;
import static pl.net.was.trino.git.GitErrorCode.GIT_REPOSITORY_NOT_FOUND;

public class GitRepositoryManager
{
    private static final Set<PosixFilePermission> OWNER_ONLY = PosixFilePermissions.fromString("rwx------");
    private static final Set<RefUpdate.Result> FAILED_REF_UPDATES = EnumSet.of(
            RefUpdate.Result.LOCK_FAILURE,
            RefUpdate.Result.IO_FAILURE,
            RefUpdate.Result.REJECTED_MISSING_OBJECT,
            RefUpdate.Result.REJECTED_OTHER_REASON);
    private static final ConcurrentMap<Path, ReentrantLock> LOCKS = new ConcurrentHashMap<>();
    private static final String REMOTE = "origin";
    // messages of TransportException from JGitText, which is not a public API
    private static final List<String> AUTHENTICATION_ERRORS = List.of(
            "not authorized",
            "authentication not supported",
            "Authentication is required but no CredentialsProvider has been registered");
    private static final List<RefSpec> REF_SPECS = List.of(
            new RefSpec("+refs/heads/*:refs/heads/*"),
            new RefSpec("+refs/tags/*:refs/tags/*"));

    private final Map<String, URI> repositories;
    private final Optional<CredentialsProvider> credentialsProvider;
    private final Path cacheDirectory;
    private final long fetchIntervalNanos;
    private final int timeoutSeconds;
    private final ConcurrentMap<String, OpenRepository> openRepositories = new ConcurrentHashMap<>();

    @Inject
    public GitRepositoryManager(GitConfig config)
    {
        this.repositories = ImmutableMap.copyOf(config.getAllRepositories());
        this.cacheDirectory = config.getCacheDirectory().toPath();
        this.credentialsProvider = config.getUsername()
                .map(username -> new UsernamePasswordCredentialsProvider(username, config.getPassword().orElse("")));
        this.fetchIntervalNanos = config.getFetchInterval().roundTo(NANOSECONDS);
        this.timeoutSeconds = toIntExact(config.getTimeout().roundTo(SECONDS));
    }

    @PreDestroy
    public void close()
    {
        openRepositories.values().forEach(openRepository -> openRepository.repository().close());
        openRepositories.clear();
    }

    /**
     * Returns the repository, cloning it first if necessary, and fetching new commits if the last fetch was more
     * than git.fetch-interval ago. The repository is shared, so closing the returned object doesn't close it.
     */
    public Git getRepository(String schemaName)
    {
        URI url = repositories.get(schemaName);
        checkArgument(url != null, "Unknown repository: %s", schemaName);
        OpenRepository cached = openRepositories.get(schemaName);
        if (cached != null && System.nanoTime() - cached.fetchedNanos() < fetchIntervalNanos) {
            return Git.wrap(cached.repository());
        }
        Path localPath;
        try {
            localPath = getLocalPath(cacheDirectory, url);
        }
        catch (IOException e) {
            throw new TrinoException(GIT_OPEN_ERROR, "Failed to create the directory for the repository: " + e.getMessage(), e);
        }
        // the file lock is held by the whole JVM, so threads must also be synchronized
        ReentrantLock lock = LOCKS.computeIfAbsent(localPath, _ -> new ReentrantLock());
        lock.lock();
        try (FileChannel channel = FileChannel.open(localPath.resolveSibling(localPath.getFileName() + ".lock"), CREATE, WRITE);
                FileLock _ = channel.lock()) {
            OpenRepository openRepository = openRepositories.get(schemaName);
            long fetchedNanos = System.nanoTime();
            if (openRepository != null) {
                // another thread could have fetched it while this one was waiting for the lock
                if (fetchedNanos - openRepository.fetchedNanos() >= fetchIntervalNanos) {
                    fetchExisting(Git.wrap(openRepository.repository()), url);
                    openRepository = new OpenRepository(openRepository.repository(), fetchedNanos);
                }
            }
            else {
                openRepository = new OpenRepository(openOrClone(localPath, url).getRepository(), fetchedNanos);
            }
            openRepositories.put(schemaName, openRepository);
            return Git.wrap(openRepository.repository());
        }
        catch (IOException e) {
            throw new TrinoException(GIT_OPEN_ERROR, "Failed to lock the repository", e);
        }
        finally {
            lock.unlock();
        }
    }

    private Git openOrClone(Path localPath, URI url)
            throws IOException
    {
        if (Files.exists(localPath)) {
            Optional<Git> existing = openExisting(localPath, url);
            if (existing.isPresent()) {
                Git repo = existing.orElseThrow();
                try {
                    fetchExisting(repo, url);
                }
                catch (RuntimeException e) {
                    repo.getRepository().close();
                    throw e;
                }
                return repo;
            }
            // a clone that was interrupted, or made from another URL
            deleteRecursively(localPath, ALLOW_INSECURE);
        }
        Path temporaryPath = localPath.resolveSibling(localPath.getFileName() + ".tmp-" + randomUUID());
        try {
            // fetch only branches and tags, a mirror would also include refs like GitHub's refs/pull/*
            try (Git repo = Git.init().setBare(true).setDirectory(temporaryPath.toFile()).call()) {
                StoredConfig config = repo.getRepository().getConfig();
                RemoteConfig remote = new RemoteConfig(config, REMOTE);
                remote.addURI(new URIish(url.toString()));
                REF_SPECS.forEach(remote::addFetchRefSpec);
                remote.setTagOpt(TagOpt.NO_TAGS);
                remote.update(config);
                // JGit runs gc in the background after a fetch, which would race with moving the clone and with other fetches
                config.setBoolean(CONFIG_FETCH_SECTION, null, CONFIG_KEY_AUTOGC, false);
                config.save();
                fetch(repo);
            }
            Files.move(temporaryPath, localPath, ATOMIC_MOVE);
        }
        catch (GitAPIException | URISyntaxException e) {
            throw repositoryError(url, GIT_CLONE_ERROR, "clone", e);
        }
        finally {
            if (Files.exists(temporaryPath)) {
                deleteRecursively(temporaryPath, ALLOW_INSECURE);
            }
        }
        return openExisting(localPath, url)
                .orElseThrow(() -> new TrinoException(GIT_OPEN_ERROR, "Failed to open the cloned repository"));
    }

    private static TrinoException repositoryError(URI url, GitErrorCode errorCode, String action, Exception e)
    {
        // JGit exceptions are often wrapped multiple times, and only the innermost one says what is wrong
        List<Throwable> causes = getCausalChain(e);
        if (causes.stream().anyMatch(NoRemoteRepositoryException.class::isInstance)) {
            return new TrinoException(GIT_REPOSITORY_NOT_FOUND, "Failed to %s %s: repository not found".formatted(action, url), e);
        }
        if (causes.stream().anyMatch(GitRepositoryManager::isAuthenticationError)) {
            return new TrinoException(
                    GIT_AUTHENTICATION_ERROR,
                    "Failed to %s %s: authentication failed, check git.username and git.password".formatted(action, url),
                    e);
        }
        Throwable rootCause = getRootCause(e);
        String reason = rootCause instanceof UnknownHostException ? "unknown host " + rootCause.getMessage() : rootCause.getMessage();
        return new TrinoException(errorCode, "Failed to %s %s: %s".formatted(action, url, reason), e);
    }

    private static boolean isAuthenticationError(Throwable throwable)
    {
        String message = throwable.getMessage();
        return throwable instanceof TransportException
                && message != null
                && AUTHENTICATION_ERRORS.stream().anyMatch(message::contains);
    }

    private void fetchExisting(Git repo, URI url)
    {
        try {
            fetch(repo);
        }
        catch (GitAPIException e) {
            throw repositoryError(url, GIT_FETCH_ERROR, "fetch", e);
        }
    }

    private Optional<Git> openExisting(Path localPath, URI url)
    {
        Repository repository;
        try {
            repository = new FileRepositoryBuilder()
                    .setGitDir(localPath.toFile())
                    .setMustExist(true)
                    .build();
        }
        catch (IOException _) {
            return Optional.empty();
        }
        if (!url.toString().equals(repository.getConfig().getString("remote", REMOTE, "url"))) {
            repository.close();
            return Optional.empty();
        }
        return Optional.of(new Git(repository));
    }

    private void fetch(Git repo)
            throws GitAPIException
    {
        FetchCommand fetch = repo.fetch()
                .setTimeout(timeoutSeconds)
                .setRemote(REMOTE)
                .setRefSpecs(REF_SPECS)
                .setTagOpt(TagOpt.NO_TAGS)
                .setRemoveDeletedRefs(true)
                .setCheckFetchedObjects(true);
        credentialsProvider.ifPresent(fetch::setCredentialsProvider);
        FetchResult result = fetch.call();
        for (TrackingRefUpdate update : result.getTrackingRefUpdates()) {
            if (FAILED_REF_UPDATES.contains(update.getResult())) {
                throw new TrinoException(GIT_FETCH_ERROR, "Failed to update %s: %s".formatted(update.getLocalName(), update.getResult()));
            }
        }
        updateHead(repo, result);
    }

    private static void updateHead(Git repo, FetchResult result)
    {
        Ref remoteHead = result.getAdvertisedRef(HEAD);
        if (remoteHead == null || !remoteHead.isSymbolic()) {
            return;
        }
        try {
            Ref localHead = repo.getRepository().exactRef(HEAD);
            String target = remoteHead.getTarget().getName();
            if (localHead != null && localHead.isSymbolic() && localHead.getTarget().getName().equals(target)) {
                return;
            }
            RefUpdate.Result update = repo.getRepository().updateRef(HEAD).link(target);
            if (FAILED_REF_UPDATES.contains(update)) {
                throw new TrinoException(GIT_FETCH_ERROR, "Failed to update HEAD: " + update);
            }
        }
        catch (IOException e) {
            throw new TrinoException(GIT_FETCH_ERROR, "Failed to update HEAD", e);
        }
    }

    public static Path getLocalPath(Path cacheDirectory, URI url)
            throws IOException
    {
        createCacheDirectory(cacheDirectory);
        return cacheDirectory.resolve(Hashing.sha256().hashString(url.toString(), UTF_8).toString());
    }

    private static void createCacheDirectory(Path cacheDirectory)
            throws IOException
    {
        boolean isPosix = cacheDirectory.getFileSystem().supportedFileAttributeViews().contains("posix");
        if (!Files.exists(cacheDirectory)) {
            if (isPosix) {
                Files.createDirectories(cacheDirectory, PosixFilePermissions.asFileAttribute(OWNER_ONLY));
            }
            else {
                Files.createDirectories(cacheDirectory);
            }
            return;
        }
        UserPrincipal currentUser = cacheDirectory.getFileSystem().getUserPrincipalLookupService().lookupPrincipalByName(System.getProperty("user.name"));
        if (!Files.getOwner(cacheDirectory).equals(currentUser)) {
            throw new IOException("Cache directory %s is not owned by the current user".formatted(cacheDirectory));
        }
        if (isPosix && !OWNER_ONLY.containsAll(Files.getPosixFilePermissions(cacheDirectory))) {
            throw new IOException("Cache directory %s must be accessible only by its owner".formatted(cacheDirectory));
        }
    }

    private record OpenRepository(Repository repository, long fetchedNanos) {}
}
