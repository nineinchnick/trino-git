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

import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.FetchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.FetchResult;
import org.eclipse.jgit.transport.TrackingRefUpdate;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static pl.net.was.trino.git.GitErrorCode.GIT_CLONE_ERROR;
import static pl.net.was.trino.git.GitErrorCode.GIT_FETCH_ERROR;
import static pl.net.was.trino.git.GitErrorCode.GIT_OPEN_ERROR;

public class GitRepositoryManager
{
    private static final Set<PosixFilePermission> OWNER_ONLY = PosixFilePermissions.fromString("rwx------");
    private static final Set<RefUpdate.Result> FAILED_REF_UPDATES = EnumSet.of(
            RefUpdate.Result.LOCK_FAILURE,
            RefUpdate.Result.IO_FAILURE,
            RefUpdate.Result.REJECTED_MISSING_OBJECT,
            RefUpdate.Result.REJECTED_OTHER_REASON);
    private static final ConcurrentMap<Path, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

    private final URI url;
    private final Optional<CredentialsProvider> credentialsProvider;
    private final Path cacheDirectory;

    @Inject
    public GitRepositoryManager(GitConfig config)
    {
        this.url = requireNonNull(config.getUrl(), "url is null");
        this.cacheDirectory = config.getCacheDirectory().toPath();
        this.credentialsProvider = config.getUsername()
                .map(username -> new UsernamePasswordCredentialsProvider(username, config.getPassword().orElse("")));
    }

    public Git getRepository()
    {
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
            return openOrClone(localPath);
        }
        catch (IOException e) {
            throw new TrinoException(GIT_OPEN_ERROR, "Failed to lock the repository", e);
        }
        finally {
            lock.unlock();
        }
    }

    private Git openOrClone(Path localPath)
            throws IOException
    {
        if (Files.exists(localPath)) {
            Optional<Git> existing = openExisting(localPath);
            if (existing.isPresent()) {
                fetch(existing.orElseThrow());
                return existing.orElseThrow();
            }
            // a clone that was interrupted, or made from another URL
            deleteRecursively(localPath, ALLOW_INSECURE);
        }
        Path temporaryPath = localPath.resolveSibling(localPath.getFileName() + ".tmp-" + randomUUID());
        try {
            CloneCommand clone = Git.cloneRepository()
                    .setURI(url.toString())
                    .setDirectory(temporaryPath.toFile());
            credentialsProvider.ifPresent(clone::setCredentialsProvider);
            clone.call().close();
            Files.move(temporaryPath, localPath, ATOMIC_MOVE);
        }
        catch (GitAPIException e) {
            throw new TrinoException(GIT_CLONE_ERROR, "Failed to clone the repository", e);
        }
        finally {
            if (Files.exists(temporaryPath)) {
                deleteRecursively(temporaryPath, ALLOW_INSECURE);
            }
        }
        return openExisting(localPath)
                .orElseThrow(() -> new TrinoException(GIT_OPEN_ERROR, "Failed to open the cloned repository"));
    }

    private Optional<Git> openExisting(Path localPath)
    {
        Repository repository;
        try {
            repository = new FileRepositoryBuilder()
                    .setGitDir(localPath.resolve(".git").toFile())
                    .setMustExist(true)
                    .build();
        }
        catch (IOException _) {
            return Optional.empty();
        }
        if (!url.toString().equals(repository.getConfig().getString("remote", "origin", "url"))) {
            repository.close();
            return Optional.empty();
        }
        return Optional.of(new Git(repository));
    }

    private void fetch(Git repo)
    {
        FetchResult result;
        try {
            FetchCommand fetch = repo.fetch().setCheckFetchedObjects(true);
            credentialsProvider.ifPresent(fetch::setCredentialsProvider);
            result = fetch.call();
        }
        catch (GitAPIException e) {
            repo.close();
            throw new TrinoException(GIT_FETCH_ERROR, "Failed to fetch the repository", e);
        }
        for (TrackingRefUpdate update : result.getTrackingRefUpdates()) {
            if (FAILED_REF_UPDATES.contains(update.getResult())) {
                repo.close();
                throw new TrinoException(GIT_FETCH_ERROR, "Failed to update %s: %s".formatted(update.getLocalName(), update.getResult()));
            }
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
}
