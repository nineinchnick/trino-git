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

import java.util.Iterator;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Rows of a table, holding resources like JGit walks until closed.
 */
public interface RowIterator<T>
        extends Iterator<T>, AutoCloseable
{
    @Override
    void close();

    static <S, T> RowIterator<T> of(Iterator<S> source, Function<S, T> mapper, Runnable onClose)
    {
        requireNonNull(source, "source is null");
        requireNonNull(mapper, "mapper is null");
        requireNonNull(onClose, "onClose is null");
        return new RowIterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return source.hasNext();
            }

            @Override
            public T next()
            {
                return mapper.apply(source.next());
            }

            @Override
            public void close()
            {
                onClose.run();
            }
        };
    }
}
