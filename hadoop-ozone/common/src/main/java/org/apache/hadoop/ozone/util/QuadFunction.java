package org.apache.hadoop.ozone.util;

import java.io.IOException;

/**
 * An interface representing a function which takes 3 parameters.
 */
public interface QuadFunction<T1, T2, T3, CALLER, RESULT,
    THROWABLE extends IOException > {
  RESULT apply(T1 val1, T2 val2, T3 val3, CALLER caller) throws IOException;
}
