package com.wixpress.agent;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestsJavaAgent {
    private static final String EXTRA_DIRS = "extra.dirs";
    private static final String EXTRA_RUNTIME = "extra.runtime.dirs";

    public static void premain(String args, Instrumentation instrumentation) {
        createRequestedDirs();

        if (isJDK9OrAbove())
            addExtraRuntimeDirsToClasspath();
    }

    private static void createRequestedDirs() {
        createDirs(EXTRA_DIRS);

        if (isJDK9OrAbove())
            createDirs(EXTRA_RUNTIME);
    }

    private static void addExtraRuntimeDirsToClasspath() {
        try {
            addToClassPath(EXTRA_RUNTIME);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void createDirs(String propName) {
        getPathPropertyAsList(propName).stream()
                .map(Paths::get)
                .forEach(dir -> {
                    try {
                        Files.createDirectories(dir);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static void addToClassPath(String propName) throws Exception {
        addToClassPath(
                getPathPropertyAsList(propName).stream().map(Paths::get).collect(Collectors.toList())
        );
    }

    private static void addToClassPath(Iterable<Path> paths) throws Exception {
        // Gain access to the system classloader
        Field oldClassLoaderField = ClassLoader.class.getDeclaredField("scl");
        oldClassLoaderField.setAccessible(true);
        Object currentLoader = oldClassLoaderField.get(null);

        // Gain access to URLClassPath that manages all the URLs the SCL is aware of
        Field ucpField = currentLoader.getClass().getDeclaredField("ucp");
        ucpField.setAccessible(true);

        Object ucp = ucpField.get(currentLoader);

        URL[] currentUrls = getCurrentURLs(ucp);

        emptyUcp(ucp);

        // Push every extra dir to URLClassPath
        Method addURLMethod = ucp.getClass().getMethod("addURL", URL.class);
        addURLMethod.setAccessible(true);

        paths.forEach(p -> {
            try {
                addURLMethod.invoke(ucp, p.toUri().toURL());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Arrays.asList(currentUrls).forEach(u -> {
            try {
                addURLMethod.invoke(ucp, u);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static URL[] getCurrentURLs(Object ucp) throws Exception {
        Method getURLs = ucp.getClass().getMethod("getURLs");
        return (URL[]) getURLs.invoke(ucp);
    }

    private static void emptyUcp(Object ucp) {
        Arrays.asList("unopenedUrls", "loaders", "path", "lmap").forEach(f -> {
            try {
                Field field = ucp.getClass().getDeclaredField(f);
                field.setAccessible(true);

                Object fieldObj = field.get(ucp);

                if (fieldObj instanceof Collection) {
                    ((Collection) fieldObj).clear();
                } else if (fieldObj instanceof Map) {
                    ((Map) fieldObj).clear();
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

    }

    private static List<String> getPathPropertyAsList(String prop) {
        String[] values = getSystemProperty(prop).split(File.pathSeparator);
        return Arrays.asList(values);
    }

    private static String getSystemProperty(String prop) {
        return System.getProperty(prop, "");
    }

    private static boolean isJDK9OrAbove() {
        return !System.getProperty("java.version").startsWith("1.");
    }
}