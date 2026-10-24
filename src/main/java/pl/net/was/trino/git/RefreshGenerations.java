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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks when users asked to refresh repositories, on the coordinator, so the generation can be sent to workers in splits.
 */
public class RefreshGenerations
{
    private final ConcurrentMap<String, Long> generations = new ConcurrentHashMap<>();

    public long get(String schemaName)
    {
        return generations.getOrDefault(schemaName, 0L);
    }

    public void refresh(String schemaName)
    {
        // generations start at the current time, so they keep growing after the coordinator restarts
        generations.merge(schemaName, System.currentTimeMillis(), (previous, now) -> Math.max(previous + 1, now));
    }
}
