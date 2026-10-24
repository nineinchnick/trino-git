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
import org.eclipse.jgit.api.MergeCommand.FastForwardMode;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.ConfigInvalidException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.util.SystemReader;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.Objects.requireNonNull;

/**
 * A git repository with a small, deterministic history, used as the source repository of the connector.
 * <pre>
 *   gh-pages  O                      (orphan, empty tree)
 *   main      A - B - C - D - M - E  (M merges feature, E is empty)
 *   feature        \- F -/
 * </pre>
 * Tags: {@code v0.1} is a lightweight tag on A, {@code v1.0} is an annotated tag on M.
 * The repository is never garbage collected, so all its objects stay loose.
 */
public final class TestingGitRepository
        implements AutoCloseable
{
    public static final String MAIN_BRANCH = "main";
    public static final String FEATURE_BRANCH = "feature";
    public static final String ORPHAN_BRANCH = "gh-pages";
    public static final String LIGHTWEIGHT_TAG = "v0.1";
    public static final String ANNOTATED_TAG = "v1.0";

    public static final ZoneOffset ZONE_OFFSET = ZoneOffset.ofHoursMinutes(5, 30);
    // commits are an hour apart, so the order of a walk sorted by commit time is known
    public static final Instant START_TIME = Instant.parse("2024-01-01T10:00:00Z");
    // authors are earlier than committers, so the author time can't be mistaken for the commit time
    public static final Duration AUTHOR_TIME_SHIFT = Duration.ofMinutes(30);

    public static final PersonIdent ALICE = new PersonIdent("Alice", "alice@example.com", START_TIME, ZONE_OFFSET);
    // the same email as ALICE with other names, which the idents view should merge into one identity
    public static final PersonIdent ALICE_FULL_NAME = new PersonIdent("Alice Example", "alice@example.com", START_TIME, ZONE_OFFSET);
    public static final PersonIdent ALICE_LOWERCASE = new PersonIdent("alice", "alice@example.com", START_TIME, ZONE_OFFSET);
    public static final PersonIdent CAROL = new PersonIdent("Carol", "carol@example.com", START_TIME, ZONE_OFFSET);
    public static final PersonIdent COMMITTER = new PersonIdent("Bob Builder", "bob@example.com", START_TIME, ZONE_OFFSET);

    public static final String README = "README.md";
    public static final String GUIDE = "docs/guide.md";
    public static final String RENAMED_OLD = "old-name.txt";
    public static final String RENAMED_NEW = "new-name.txt";
    public static final String DELETED = "to-delete.txt";
    public static final String FEATURE_FILE = "feature.txt";
    public static final String BINARY = "image.bin";

    private final Path directory;
    private final Git git;
    private int commitCount;

    private final RevCommit initialCommit;
    private final RevCommit updateCommit;
    private final RevCommit featureCommit;
    private final RevCommit renameCommit;
    private final RevCommit deleteCommit;
    private final RevCommit mergeCommit;
    private final RevCommit emptyCommit;
    private final RevCommit orphanCommit;
    private final ObjectId annotatedTagId;

    public TestingGitRepository()
            throws IOException, GitAPIException
    {
        isolateFromUserConfig();
        directory = Files.createTempDirectory("trino-git-");
        git = Git.init()
                .setDirectory(directory.toFile())
                .setInitialBranch(MAIN_BRANCH)
                .call();

        writeFile(README, "# Test repository\n");
        writeFile(GUIDE, "# Guide\n\nRead the README first.\n");
        writeFile(RENAMED_OLD, "This file is renamed later.\nIts contents don't change,\nso git detects the rename.\n");
        writeFile(DELETED, "This file is deleted later.\n");
        initialCommit = commitAll(ALICE, "Initial commit");

        writeFile(README, "# Test repository\n\nIt has a known history.\nIt is used by the tests.\n");
        updateCommit = commitAll(ALICE_FULL_NAME, "Describe the repository");

        git.checkout().setCreateBranch(true).setName(FEATURE_BRANCH).call();
        writeFile(FEATURE_FILE, "A feature\n");
        featureCommit = commitAll(CAROL, "Add a feature");
        git.checkout().setName(MAIN_BRANCH).call();

        Files.move(directory.resolve(RENAMED_OLD), directory.resolve(RENAMED_NEW));
        Files.write(directory.resolve(BINARY), new byte[] {(byte) 0x89, 'P', 'N', 'G', 0, 0, 0, 0, (byte) 0xFF, (byte) 0xFE, 0, 1});
        renameCommit = commitAll(ALICE_LOWERCASE, "Rename a file and add a binary file");

        Files.delete(directory.resolve(DELETED));
        deleteCommit = commitAll(ALICE, "Delete a file");

        git.merge()
                .include(git.getRepository().findRef(FEATURE_BRANCH))
                .setFastForward(FastForwardMode.NO_FF)
                .setCommit(false)
                .call();
        mergeCommit = commitAll(ALICE, "Merge branch 'feature'");

        annotatedTagId = git.tag()
                .setName(ANNOTATED_TAG)
                .setMessage("Release 1.0")
                .setObjectId(mergeCommit)
                .setTagger(new PersonIdent(COMMITTER, nextTime(), ZONE_OFFSET))
                .setSigned(false)
                .call()
                .getObjectId();
        git.tag()
                .setName(LIGHTWEIGHT_TAG)
                .setObjectId(initialCommit)
                .setAnnotated(false)
                .setSigned(false)
                .call();

        emptyCommit = addEmptyCommit("Empty commit");

        git.checkout().setOrphan(true).setName(ORPHAN_BRANCH).call();
        git.rm()
                .addFilepattern(README)
                .addFilepattern(GUIDE)
                .addFilepattern(RENAMED_NEW)
                .addFilepattern(BINARY)
                .addFilepattern(FEATURE_FILE)
                .call();
        orphanCommit = commit(CAROL, "Start an orphan branch with an empty tree");
        git.checkout().setName(MAIN_BRANCH).call();
    }

    public URI getUri()
    {
        return directory.toUri();
    }

    public RevCommit getInitialCommit()
    {
        return initialCommit;
    }

    public RevCommit getUpdateCommit()
    {
        return updateCommit;
    }

    public RevCommit getFeatureCommit()
    {
        return featureCommit;
    }

    public RevCommit getRenameCommit()
    {
        return renameCommit;
    }

    public RevCommit getDeleteCommit()
    {
        return deleteCommit;
    }

    public RevCommit getMergeCommit()
    {
        return mergeCommit;
    }

    public RevCommit getEmptyCommit()
    {
        return emptyCommit;
    }

    public RevCommit getOrphanCommit()
    {
        return orphanCommit;
    }

    public ObjectId getAnnotatedTagId()
    {
        return annotatedTagId;
    }

    public ObjectId getBlobId(RevCommit commit, String path)
            throws IOException
    {
        try (TreeWalk treeWalk = TreeWalk.forPath(git.getRepository(), path, commit.getTree())) {
            requireNonNull(treeWalk, () -> "Path not found: " + path);
            return treeWalk.getObjectId(0);
        }
    }

    public RevCommit addEmptyCommit(String message)
            throws GitAPIException
    {
        return commit(ALICE, message);
    }

    public void deleteBranch(String name)
            throws GitAPIException
    {
        git.branchDelete().setBranchNames(name).setForce(true).call();
    }

    @Override
    public void close()
            throws IOException
    {
        git.close();
        deleteRecursively(directory, ALLOW_INSECURE);
    }

    private RevCommit commitAll(PersonIdent author, String message)
            throws GitAPIException
    {
        git.add().addFilepattern(".").call();
        git.add().addFilepattern(".").setUpdate(true).call();
        return commit(author, message);
    }

    private RevCommit commit(PersonIdent author, String message)
            throws GitAPIException
    {
        Instant time = nextTime();
        return git.commit()
                .setMessage(message)
                .setAuthor(new PersonIdent(author, time.minus(AUTHOR_TIME_SHIFT), ZONE_OFFSET))
                .setCommitter(new PersonIdent(COMMITTER, time, ZONE_OFFSET))
                .setAllowEmpty(true)
                .setSign(false)
                .call();
    }

    private Instant nextTime()
    {
        return START_TIME.plus(Duration.ofHours(commitCount++));
    }

    private void writeFile(String path, String contents)
            throws IOException
    {
        Path file = directory.resolve(path);
        Files.createDirectories(file.getParent());
        Files.writeString(file, contents);
    }

    private static void isolateFromUserConfig()
            throws IOException
    {
        try {
            SystemReader.getInstance().getUserConfig().clear();
            SystemReader.getInstance().getSystemConfig().clear();
        }
        catch (ConfigInvalidException e) {
            throw new IOException(e);
        }
    }
}
