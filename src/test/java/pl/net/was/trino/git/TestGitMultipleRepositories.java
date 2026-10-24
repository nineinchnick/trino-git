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

import static org.assertj.core.api.Assertions.assertThat;

public class TestGitMultipleRepositories
        extends AbstractTestQueryFramework
{
    private TestingGitRepository fixture;
    private URI other;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        fixture = closeAfterClass(new TestingGitRepository());
        other = TestGitClient.setupRepo(URI.create("multiple-repositories.invalid"));
        return GitQueryRunner.builder(fixture.getUri())
                .addConnectorProperty("git.repositories", "other=" + other)
                .build();
    }

    @Test
    public void testSchemaPerRepository()
    {
        assertThat(query("SHOW SCHEMAS FROM git"))
                .skippingTypesCheck()
                .matches("VALUES 'default', 'information_schema', 'other'");
        assertThat(query("SELECT count(*) FROM git.default.commits"))
                .matches("VALUES BIGINT '8'");
        assertThat(query("SELECT count(*) FROM git.other.commits"))
                .matches("VALUES BIGINT '2'");
        // commits of different repositories can be compared in one query
        assertThat(query("SELECT count(*) FROM git.default.commits d JOIN git.other.commits o ON d.object_id = o.object_id"))
                .matches("VALUES BIGINT '0'");
    }
}
