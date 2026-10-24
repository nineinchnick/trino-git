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

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestGitSplitManager
{
    private TestingGitRepository repository;
    private GitRepositoryManager repositoryManager;
    private GitSplitManager splitManager;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        repository = new TestingGitRepository();
        GitConfig config = new GitConfig()
                .setUrl(repository.getUri())
                .setCommitsPerSplit(3);
        repositoryManager = new GitRepositoryManager(config);
        splitManager = new GitSplitManager(new PinnedSnapshots(repositoryManager, new RefreshGenerations()), repositoryManager, config);
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        repositoryManager.close();
        repository.close();
    }

    @Test
    public void testSplitsCommitsIntoChunks()
    {
        List<GitSplit> splits = getSplits(new GitTableHandle("default", "commits", TupleDomain.all()));
        // the fixture has 8 commits
        assertThat(splits).hasSize(3);
        assertThat(splits).extracting(split -> split.commitIds().orElseThrow().size()).containsExactly(3, 3, 2);
        assertThat(splits.stream().flatMap(split -> split.commitIds().orElseThrow().stream()).distinct()).hasSize(8);
        // all splits read the same snapshot
        assertThat(splits.stream().map(GitSplit::snapshot).distinct()).hasSize(1);
    }

    @Test
    public void testSplitsFilteredCommits()
    {
        GitColumnHandle objectId = new GitColumnHandle("object_id", VARCHAR, 0);
        TupleDomain<GitColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(objectId, multipleValues(VARCHAR, List.of(
                utf8Slice(repository.getInitialCommit().getName()),
                utf8Slice(repository.getMergeCommit().getName())))));
        List<GitSplit> splits = getSplits(new GitTableHandle("default", "commits", constraint));
        assertThat(splits).hasSize(1);
        assertThat(splits.getFirst().commitIds().orElseThrow()).hasSize(2);
    }

    @Test
    public void testSingleSplitForOtherTables()
    {
        List<GitSplit> splits = getSplits(new GitTableHandle("default", "branches", TupleDomain.all()));
        assertThat(splits).hasSize(1);
        assertThat(splits.getFirst().commitIds()).isEmpty();
    }

    private List<GitSplit> getSplits(GitTableHandle table)
    {
        List<GitSplit> splits = new ArrayList<>();
        try (ConnectorSplitSource splitSource = splitManager.getSplits(new GitTransactionHandle(randomUUID()), SESSION, table, Set.of(), Constraint.alwaysTrue())) {
            while (!splitSource.isFinished()) {
                for (ConnectorSplit split : splitSource.getNextBatch(2, DynamicFilterSnapshot.EMPTY).join()) {
                    splits.add((GitSplit) split);
                }
            }
        }
        return splits;
    }
}
