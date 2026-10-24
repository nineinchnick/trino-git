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

import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static pl.net.was.trino.git.GitErrorCode.GIT_CLONE_ERROR;

public class TestGitRepositoryManager
{
    @Test
    public void testCloneMissingRepository()
    {
        GitRepositoryManager repositoryManager = new GitRepositoryManager();
        assertTrinoExceptionThrownBy(() -> repositoryManager.getRepository(URI.create("file:///nonexistent/trino-git/repo")))
                .hasErrorCode(GIT_CLONE_ERROR)
                .hasMessage("Failed to clone the repository");
    }
}
