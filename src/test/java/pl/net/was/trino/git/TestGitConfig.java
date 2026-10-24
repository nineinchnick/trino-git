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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGitConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GitConfig.class)
                .setUrl(null)
                .setUsername(null)
                .setPassword(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("git.url", "https://example.com/repo.git")
                .put("git.username", "user")
                .put("git.password", "secret")
                .buildOrThrow();

        GitConfig expected = new GitConfig()
                .setUrl(URI.create("https://example.com/repo.git"))
                .setUsername("user")
                .setPassword("secret");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testLegacyPropertyMappings()
    {
        assertDeprecatedEquivalence(
                GitConfig.class,
                Map.of("git.url", "https://example.com/repo.git"),
                Map.of("metadata-uri", "https://example.com/repo.git"));
    }
}
