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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

public class GitConfig
{
    private URI url;
    private String username;
    private String password;

    @NotNull
    public URI getUrl()
    {
        return url;
    }

    @Config("git.url")
    @LegacyConfig("metadata-uri")
    @ConfigDescription("URL of the git repository")
    public GitConfig setUrl(URI url)
    {
        this.url = url;
        return this;
    }

    public Optional<String> getUsername()
    {
        return Optional.ofNullable(username);
    }

    @Config("git.username")
    @ConfigDescription("Username used to authenticate to the git repository")
    public GitConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("git.password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password or access token used to authenticate to the git repository")
    public GitConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }
}
