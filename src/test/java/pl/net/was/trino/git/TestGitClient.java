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
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
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
        // ensure the repo dir exists, remove and recreate if necessary
        File localPath;
        try {
            localPath = GitRecordSet.ensureDir(uri.toString());
        }
        catch (IOException ignored) {
            return;
        }
        if (localPath.exists()) {
            deleteRecursively(localPath.toPath(), ALLOW_INSECURE);
        }

        Repository repository = FileRepositoryBuilder.create(new File(localPath, ".git"));
        repository.create();

        // create a new file
        File myFile = new File(repository.getDirectory().getParent(), "testfile");
        if (!myFile.createNewFile()) {
            throw new IOException("Could not create file " + myFile);
        }

        PersonIdent author = new PersonIdent("test", "test@invalid.com", new Date(1580897313000L), TimeZone.getTimeZone("UTC"));
        // commit the new file
        Git git = new Git(repository);
        git.add().addFilepattern(".").call();
        git.commit()
                .setMessage("Commit all changes including additions")
                .setAuthor(author)
                .setCommitter(author)
                .call();

        try (PrintWriter writer = new PrintWriter(myFile)) {
            writer.append("Hello, world!");
        }
        if (!myFile.setLastModified(1580897600000L)) {
            throw new IOException("Could not set last modified on file " + myFile);
        }

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

        // ensure all loose objects are packed
        git.gc().call();
    }
}
