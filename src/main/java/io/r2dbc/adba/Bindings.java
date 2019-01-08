/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.r2dbc.adba;

import io.r2dbc.spi.Statement;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Holds one or more parameter bindings for a {@link Statement}.
 */
class Bindings {

    private final List<Binding> bindings = new ArrayList<>();
    private @Nullable
    Binding current;

    @Override
    public String toString() {
        return "Bindings{" + "bindings=" + this.bindings + ", current=" + this.current + '}';
    }

    void finish() {
        this.current = null;
    }

    Binding getCurrent() {
        if (this.current == null) {
            this.current = new Binding();
            this.bindings.add(this.current);
        }

        return this.current;
    }

    Stream<Binding> stream() {
        return this.bindings.stream();
    }
}
