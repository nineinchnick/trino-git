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

import com.google.common.collect.ImmutableList;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jgit.lib.Constants.OBJ_BLOB;

public class TestCommitWalks
{
    @Test
    public void testSkipsRefsNotPointingToCommits(@TempDir Path tempDir)
            throws Exception
    {
        try (Git git = Git.init().setDirectory(tempDir.toFile()).setInitialBranch("main").call()) {
            RevCommit first = git.commit().setMessage("first").setAllowEmpty(true).setSign(false).call();
            ObjectId blob;
            try (ObjectInserter inserter = git.getRepository().newObjectInserter()) {
                blob = inserter.insert(OBJ_BLOB, "key".getBytes(UTF_8));
                inserter.flush();
            }
            // refs are read in order, so these come before the branch with the second commit
            createRef(git, "refs/heads/a-blob", blob);
            createRef(git, "refs/heads/b-tree", first.getTree());
            git.getRepository().updateRef("refs/heads/c-unborn").link("refs/heads/does-not-exist");
            git.checkout().setOrphan(true).setName("z-branch").call();
            RevCommit second = git.commit().setMessage("second").setAllowEmpty(true).setSign(false).call();
            git.checkout().setName("main").call();

            try (RevWalk revWalk = new RevWalk(git.getRepository())) {
                assertThat(ImmutableList.copyOf(CommitWalks.walkCommits(git.getRepository(), revWalk, Optional.empty())))
                        .containsExactlyInAnyOrder(first, second);
            }
        }
    }

    private static void createRef(Git git, String name, ObjectId target)
            throws Exception
    {
        RefUpdate update = git.getRepository().updateRef(name);
        update.setNewObjectId(target);
        assertThat(update.update()).isEqualTo(RefUpdate.Result.NEW);
    }
}
