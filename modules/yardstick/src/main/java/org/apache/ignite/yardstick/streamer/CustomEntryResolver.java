/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.yardstick.streamer;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.stream.StreamTransformer;

/** */
public class CustomEntryResolver extends StreamTransformer<CustomKey, CustomValue> {
    /** */
    @Override
    public Object process(MutableEntry<CustomKey, CustomValue> cachedEntry, Object... streamValues) throws EntryProcessorException {
        CustomValue mandateValFromStream = (CustomValue) streamValues[0];
        CustomValue cachedVal = cachedEntry.getValue();

        if (cachedVal == null || cachedVal.getTime().compareTo(mandateValFromStream.getTime()) < 0)
            cachedEntry.setValue(mandateValFromStream);

        return null;
    }
}
