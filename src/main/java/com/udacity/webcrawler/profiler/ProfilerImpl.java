package com.udacity.webcrawler.profiler;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Objects;

import static java.lang.reflect.Proxy.newProxyInstance;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

/**
 * Concrete implementation of the {@link Profiler}.
 */
final class ProfilerImpl implements Profiler {

  private final Clock clock;
  private final ProfilingState state = new ProfilingState();
  private final ZonedDateTime startTime;

  @Inject
  ProfilerImpl(Clock clock) {
    this.clock = Objects.requireNonNull(clock);
    this.startTime = ZonedDateTime.now(clock);
  }
  @Profiled
  Boolean isClassProfiled(Class<?> klass) throws   IOException ,IllegalArgumentException{
    Method[] methods = klass.getDeclaredMethods();
    if (methods.length == 0) {
      return false;
    }
    for (Method method: methods) {
      if (method.getAnnotation(Profiled.class) != null) {
        return true;
      }
    }
    return false;
  }
   @Override
  public <T> T wrap(Class<T> klass, T delegate)  throws IOException ,IllegalArgumentException{
    Objects.requireNonNull(klass);

    // TODO: Use a dynamic proxy (java.lang.reflect.Proxy) to "wrap" the delegate in a
    //       ProfilingMethodInterceptor and return a dynamic proxy from this method.
    //       See https://docs.oracle.com/javase/10/docs/api/java/lang/reflect/Proxy.html.

    if (!isClassProfiled(klass)) {
      throw new IllegalArgumentException("");
    }

    InvocationHandler handler = new ProfilingMethodInterceptor(clock, state, delegate);
    @SuppressWarnings("unchecked")
    T proxy = (T) newProxyInstance(
            klass.getClassLoader(),
            new Class[]{klass}, handler);

    return proxy;
  }



  @Override
  public void writeData(Path path) {
    // TODO: Write the ProfilingState data to the given file path. If a file already exists at that
    //       path, the new data should be appended to the existing file.

    Objects.requireNonNull(path);
    try (Writer writer = Files.newBufferedWriter(
            path, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      writeData(writer);
    } catch (IOException e) {
e.fillInStackTrace();    }
  }

  @Override
  public void writeData(Writer writer) throws IOException {
    writer.write("Run at " + RFC_1123_DATE_TIME.format(startTime));
    writer.write(System.lineSeparator());
   try{ state.write(writer);
    writer.write(System.lineSeparator());
  }catch (IOException E){
     E.fillInStackTrace();
   }

  }
}
