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
import org.eclipse.jgit.api.FetchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
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
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.EnumSet;
import java.util.List;
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
import static org.eclipse.jgit.lib.Constants.HEAD;
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
    private static final String REMOTE = "origin";
    private static final List<RefSpec> REF_SPECS = List.of(
            new RefSpec("+refs/heads/*:refs/heads/*"),
            new RefSpec("+refs/tags/*:refs/tags/*"));

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
                Git repo = existing.orElseThrow();
                try {
                    fetch(repo);
                }
                catch (GitAPIException e) {
                    repo.close();
                    throw new TrinoException(GIT_FETCH_ERROR, "Failed to fetch the repository", e);
                }
                catch (RuntimeException e) {
                    repo.close();
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
                config.save();
                fetch(repo);
            }
            Files.move(temporaryPath, localPath, ATOMIC_MOVE);
        }
        catch (GitAPIException | URISyntaxException e) {
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
}
