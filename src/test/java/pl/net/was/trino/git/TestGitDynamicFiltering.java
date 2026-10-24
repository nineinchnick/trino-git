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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGitDynamicFiltering
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        URI url = TestGitClient.setupRepo(URI.create("dynamic-filtering.invalid"));
        return GitQueryRunner.createGitQueryRunner(Map.of(), Map.of("git.url", url.toString()));
    }

    @Test
    public void testDynamicFilterOnTrees()
    {
        // commits is the build side, so its object ids are used to read only the matching trees
        assertInputPositions(
                """
                SELECT t.path_name
                FROM git.default.trees t
                JOIN git.default.commits c ON t.commit_id = c.object_id
                WHERE c.message = 'Commit changes to all files'
                """,
                // 2 commits and 1 of 2 trees
                3);
    }

    @Test
    public void testNoDynamicFilterWhenTreesIsTheBuildSide()
    {
        // without table statistics, joins are not reordered, so the right side is the build side
        assertInputPositions(
                """
                SELECT t.path_name
                FROM git.default.commits c
                JOIN git.default.trees t ON t.commit_id = c.object_id
                WHERE c.message = 'Commit changes to all files'
                """,
                // 2 commits and 2 trees
                4);
    }

    private void assertInputPositions(String sql, long expectedPositions)
    {
        assertQueryStats(
                getSession(),
                sql,
                queryStats -> assertThat(queryStats.getPhysicalInputPositions()).isEqualTo(expectedPositions),
                result -> assertThat(result.getRowCount()).isEqualTo(1));
    }
}
