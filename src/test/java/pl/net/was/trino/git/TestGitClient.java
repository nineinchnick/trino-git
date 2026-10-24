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

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.ConfigInvalidException;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.util.SystemReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Set;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGitClient
{
    @Test
    public void testMetadata()
    {
        GitClient client = new GitClient(new GitConfig());
        assertThat(client.getSchemaNames()).isEqualTo(Set.of("default"));
    }

    public static void setupRepo(URI uri)
            throws IOException, GitAPIException
    {
        // make sure the global Git config is not being used
        try {
            SystemReader.getInstance().getUserConfig().clear();
        }
        catch (ConfigInvalidException e) {
            // ignore
        }

        // ensure the repo dir exists, remove and recreate if necessary
        Path localPath;
        try {
            localPath = GitRecordSet.ensureDir(uri.toString());
        }
        catch (IOException ignored) {
            return;
        }
        if (Files.exists(localPath)) {
            deleteRecursively(localPath, ALLOW_INSECURE);
        }

        Repository repository = FileRepositoryBuilder.create(localPath.resolve(".git").toFile());
        repository.create();

        // create a new file
        Path myFile = Files.createFile(repository.getWorkTree().toPath().resolve("testfile"));

        PersonIdent author = new PersonIdent("test", "test@invalid.com", Instant.ofEpochSecond(1580897313L), UTC);
        // commit the new file
        Git git = new Git(repository);
        git.add().addFilepattern(".").call();
        git.commit()
                .setMessage("Commit all changes including additions")
                .setAuthor(author)
                .setCommitter(author)
                .call();

        Files.writeString(myFile, "Hello, world!");
        Files.setLastModifiedTime(myFile, FileTime.fromMillis(1580897600000L));

        // Stage all changed files, omitting new files, and commit with one command
        git.commit()
                .setAll(true)
                .setMessage("Commit changes to all files")
                .setAuthor(author)
                .setCommitter(author)
                .call();

        git.tag()
                .setName("tag_for_testing")
                .setTagger(author)
                .call();

        git.tag()
                .setName("unannotated_tag_for_testing")
                .setAnnotated(false)
                .call();

        // ensure all loose objects are packed
        git.gc().call();
    }
}
