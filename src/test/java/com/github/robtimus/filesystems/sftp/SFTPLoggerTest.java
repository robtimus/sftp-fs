/*
 * SFTPLoggerTest.java
 * Copyright 2019 Rob Spoor
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

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;

@SuppressWarnings({ "nls", "javadoc" })
public class SFTPLoggerTest {

    private static final Map<Class<?>, Object> INSTANCES;

    static {
        Map<Class<?>, Object> map = new HashMap<>();

        map.put(boolean.class, true);

        map.put(char.class, 'A');

        map.put(byte.class, (byte) 13);
        map.put(short.class, (short) 14);
        map.put(int.class, 15);
        map.put(long.class, 16L);

        map.put(String.class, "foobar");

        map.put(IOException.class, new IOException("dummy"));

        INSTANCES = Collections.unmodifiableMap(map);
    }

    private static Properties resourceBundle;

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void testMethodCall(@SuppressWarnings("unused") String testName, Method method) throws ReflectiveOperationException {
        Object[] args = getArguments(method);

        if ("creatingPool".equals(method.getName()) || "createdPool".equals(method.getName())) {
            testMethodCall(method, method.getName() + "WithPort", args, Arrays.copyOfRange(args, 1, args.length));
            args[2] = -1;
            Object[] formatArgs = new Object[args.length - 2];
            System.arraycopy(args, 1, formatArgs, 0, 1);
            System.arraycopy(args, 3, formatArgs, 1, args.length - 3);
            testMethodCall(method, method.getName() + "WithoutPort", args, formatArgs);
        } else if (args[args.length - 1] instanceof Exception) {
            testMethodCall(method, method.getName(), args, Arrays.copyOfRange(args, 1, args.length - 1));
        } else {
            testMethodCall(method, method.getName(), args, Arrays.copyOfRange(args, 1, args.length));
        }
    }

    private void testMethodCall(Method method, String resourceName, Object[] invokeArgs, Object[] formatArgs) throws ReflectiveOperationException {
        Logger logger = mock(Logger.class);
        doReturn(true).when(logger).isDebugEnabled();
        invokeArgs[0] = logger;
        method.invoke(null, invokeArgs);

        String pattern = resourceBundle.getProperty("log." + resourceName);
        String expected = String.format(pattern, formatArgs);
        if (invokeArgs[invokeArgs.length - 1] instanceof Exception) {
            verify(logger).debug(expected, (Exception) invokeArgs[invokeArgs.length - 1]);
        } else {
            verify(logger).debug(expected);
        }
    }

    @BeforeAll
    public static void loadResourceBundle() throws IOException {
        resourceBundle = new Properties();
        try (InputStream input = SFTPLogger.class.getResourceAsStream("fs.properties")) {
            resourceBundle.load(input);
        }
    }

    static Stream<Arguments> testMethodCall() {
        List<Arguments> parameters = new ArrayList<>();
        collectParameters(parameters);
        return parameters.stream();
    }

    private static void collectParameters(List<Arguments> parameters) {
        for (Method method : SFTPLogger.class.getMethods()) {
            if (method.getDeclaringClass() != Object.class) {
                Class<?>[] parameterTypes = method.getParameterTypes();
                if (parameterTypes.length > 0 && parameterTypes[0] == Logger.class) {
                    parameters.add(arguments(method.getName(), method));
                }
            }
        }
    }

    private static Object[] getArguments(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] arguments = new Object[parameterTypes.length];
        for (int i = 1; i < parameterTypes.length; i++) {
            arguments[i] = Objects.requireNonNull(INSTANCES.get(parameterTypes[i]), "no instance defined for " + parameterTypes[i]);
        }
        return arguments;
    }
}
