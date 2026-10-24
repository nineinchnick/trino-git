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
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

public class TestGitConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GitConfig.class)
                .setUrl(null)
                .setUsername(null)
                .setPassword(null)
                .setCacheDirectory(Path.of(System.getProperty("java.io.tmpdir"), "trino-git").toFile()));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("git.url", "https://example.com/repo.git")
                .put("git.username", "user")
                .put("git.password", "secret")
                .put("git.cache-directory", "/var/cache/trino-git")
                .buildOrThrow();

        GitConfig expected = new GitConfig()
                .setUrl(URI.create("https://example.com/repo.git"))
                .setUsername("user")
                .setPassword("secret")
                .setCacheDirectory(Path.of("/var/cache/trino-git").toFile());

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

    @Test
    public void testCredentialsInUrl()
    {
        assertFailsValidation(
                new GitConfig().setUrl(URI.create("https://user:secret@example.com/repo.git")),
                "urlWithoutCredentials",
                "git.url must not contain credentials, use git.username and git.password instead",
                AssertTrue.class);
        assertFailsValidation(
                new GitConfig().setUrl(URI.create("https://token@example.com/repo.git")),
                "urlWithoutCredentials",
                "git.url must not contain credentials, use git.username and git.password instead",
                AssertTrue.class);
        assertValidates(new GitConfig().setUrl(URI.create("ssh://git@example.com/repo.git")));
        assertValidates(new GitConfig().setUrl(URI.create("https://example.com/repo.git")));
    }
}
