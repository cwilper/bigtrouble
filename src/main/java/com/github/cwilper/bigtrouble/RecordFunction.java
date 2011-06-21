package com.github.cwilper.bigtrouble;

import java.util.Map;

/**
 * Callback interface to do something with a record.
 */
public interface RecordFunction {

    /**
     * Do something with a record.
     *
     * @param key the unique id of the record within its column family.
     * @param columns the name-value pairs that comprise the record.
     * @return true if iteration of additional records should continue,
     *         false otherwise.
     */
    boolean execute(String key,
                    Map<String, String> columns);
}
