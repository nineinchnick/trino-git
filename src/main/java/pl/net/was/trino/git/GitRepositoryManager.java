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
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.RemoteConfig;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static pl.net.was.trino.git.GitErrorCode.GIT_CLONE_ERROR;
import static pl.net.was.trino.git.GitErrorCode.GIT_FETCH_ERROR;
import static pl.net.was.trino.git.GitErrorCode.GIT_OPEN_ERROR;

public class GitRepositoryManager
{
    private static final Set<PosixFilePermission> OWNER_ONLY = PosixFilePermissions.fromString("rwx------");

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
        if (!Files.exists(localPath)) {
            CloneCommand repo = Git.cloneRepository()
                    .setURI(url.toString())
                    .setDirectory(localPath.toFile());
            credentialsProvider.ifPresent(repo::setCredentialsProvider);
            try {
                return repo.call();
            }
            catch (GitAPIException e) {
                throw new TrinoException(GIT_CLONE_ERROR, "Failed to clone the repository", e);
            }
        }
        Repository fileRepo;
        try {
            fileRepo = new FileRepositoryBuilder()
                    .setGitDir(localPath.resolve(".git").toFile())
                    .build();
        }
        catch (IOException e) {
            throw new TrinoException(GIT_OPEN_ERROR, "Failed to open the repository", e);
        }
        Git repo = new Git(fileRepo);
        try {
            List<RemoteConfig> remotes = repo.remoteList().call();
            if (!remotes.isEmpty()) {
                FetchCommand fetch = repo.fetch().setCheckFetchedObjects(true);
                credentialsProvider.ifPresent(fetch::setCredentialsProvider);
                fetch.call();
            }
        }
        catch (GitAPIException e) {
            throw new TrinoException(GIT_FETCH_ERROR, "Failed to fetch the repository", e);
        }
        return repo;
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
