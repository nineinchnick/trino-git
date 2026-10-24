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
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.RemoteConfig;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class GitRepositoryManager
{
    public Git getRepository(URI uri)
    {
        String url = uri.toString();
        Path localPath;
        try {
            localPath = ensureDir(url);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (!Files.exists(localPath)) {
            CloneCommand repo = Git.cloneRepository()
                    .setURI(url)
                    .setDirectory(localPath.toFile());
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
                    .setGitDir(localPath.resolve(".git").toFile())
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

    public static Path ensureDir(String prefix)
            throws IOException
    {
        String tmpDirStr = System.getProperty("java.io.tmpdir");
        if (tmpDirStr == null) {
            throw new IOException(
                    "System property 'java.io.tmpdir' does not specify a tmp dir");
        }

        Path tmpDir = Files.createDirectories(Path.of(tmpDirStr));
        return tmpDir.resolve(Hashing.sha256().hashString(prefix, UTF_8).toString());
    }
}
