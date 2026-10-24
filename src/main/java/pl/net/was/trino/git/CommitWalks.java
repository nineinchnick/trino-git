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

import io.trino.spi.TrinoException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevWalk;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static pl.net.was.trino.git.GitErrorCode.GIT_READ_ERROR;

final class CommitWalks
{
    private CommitWalks() {}

    /**
     * Returns the selected commits, or all commits reachable from any ref, if none are selected.
     */
    static Iterator<RevCommit> walkCommits(Repository repository, RevWalk revWalk, Optional<List<String>> commitIds)
    {
        if (commitIds.isEmpty()) {
            try {
                for (Ref ref : repository.getRefDatabase().getRefs()) {
                    markStart(revWalk, ref);
                }
            }
            catch (IOException e) {
                throw new TrinoException(GIT_READ_ERROR, "Failed to read refs", e);
            }
            return revWalk.iterator();
        }
        return commitIds.orElseThrow().stream().map(id -> {
            try {
                RevCommit commit = revWalk.parseCommit(ObjectId.fromString(id));
                // annotated tags are peeled to the commit they point to, which has a different id
                return commit.getId().getName().equals(id) ? commit : null;
            }
            catch (IOException ignored) {
                // ignore invalid commits
                return null;
            }
        }).filter(Objects::nonNull).iterator();
    }

    private static void markStart(RevWalk revWalk, Ref ref)
            throws IOException
    {
        // symbolic refs to unborn branches, like HEAD in an empty repository, don't point to any object
        if (ref.getObjectId() == null) {
            return;
        }
        // refs can point to annotated tags, and these, like any ref, to trees or blobs
        RevObject object = revWalk.peel(revWalk.parseAny(ref.getObjectId()));
        if (object instanceof RevCommit commit) {
            revWalk.markStart(commit);
        }
    }
}
