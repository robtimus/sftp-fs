/*
 * SFTPMessagesTest.java
 * Copyright 2017 Rob Spoor
 *
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

package com.github.robtimus.filesystems.sftp;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.file.CopyOption;
import java.nio.file.OpenOption;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings({ "nls", "javadoc" })
public class SFTPMessagesTest {

    private static final Map<Class<?>, Object> INSTANCES;

    static {
        Map<Class<?>, Object> map = new HashMap<>();

        map.put(boolean.class, true);
        map.put(Boolean.class, true);

        map.put(char.class, 'A');
        map.put(Character.class, 'A');

        map.put(byte.class, (byte) 13);
        map.put(Byte.class, (byte) 13);
        map.put(short.class, (short) 13);
        map.put(Short.class, (short) 13);
        map.put(int.class, 13);
        map.put(Integer.class, 13);
        map.put(long.class, 13L);
        map.put(Long.class, 13L);
        map.put(float.class, 13F);
        map.put(Float.class, 13F);
        map.put(double.class, 13D);
        map.put(Double.class, 13D);

        map.put(String.class, "foobar");
        map.put(Class.class, Object.class);
        map.put(Object.class, "foobar");

        map.put(Collection.class, Collections.emptyList());
        map.put(List.class, Collections.emptyList());
        map.put(Set.class, Collections.emptySet());
        map.put(Map.class, Collections.emptyMap());

        map.put(OpenOption.class, StandardOpenOption.READ);
        map.put(OpenOption[].class, new OpenOption[0]);

        map.put(CopyOption.class, StandardCopyOption.REPLACE_EXISTING);
        map.put(CopyOption[].class, new CopyOption[0]);

        map.put(URI.class, URI.create("https://www.github.com/"));

        INSTANCES = Collections.unmodifiableMap(map);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void testMethodCall(@SuppressWarnings("unused") String testName, Method method, Object target) {
        Object obj = Modifier.isStatic(method.getModifiers()) ? null : target;
        Object[] args = getArguments(method);
        assertDoesNotThrow(() -> method.invoke(obj, args));
    }

    static Stream<Arguments> testMethodCall() {
        List<Arguments> parameters = new ArrayList<>();
        collectParameters(parameters, SFTPMessages.class, null, "SFTPMessages");
        return parameters.stream();
    }

    private static void collectParameters(List<Arguments> parameters, Class<?> cls, Object instance, String path) {
        for (Method method : cls.getMethods()) {
            if (method.getDeclaringClass() != Object.class) {
                String methodPath = path + "." + method.getName();
                parameters.add(arguments(methodPath, method, instance));

                Class<?> returnType = method.getReturnType();
                if (returnType.getDeclaringClass() == SFTPMessages.class) {
                    // a method that returns another messages providing object - recurse to that object if possible
                    // if not, then this test will fail
                    Object obj = Modifier.isStatic(method.getModifiers()) ? null : instance;
                    try {
                        Object[] arguments = getArguments(method);
                        Object returnValue = method.invoke(obj, arguments);
                        collectParameters(parameters, returnType, returnValue, methodPath);
                    } catch (@SuppressWarnings("unused") NullPointerException | ReflectiveOperationException e) {
                        // ignore the exception; the test for the method will fail
                    }
                }
            }
        }
    }

    private static Object[] getArguments(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] arguments = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            arguments[i] = Objects.requireNonNull(INSTANCES.get(parameterTypes[i]), "no instance defined for " + parameterTypes[i]);
        }
        return arguments;
    }
}
