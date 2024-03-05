package writables;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class Step2ArrayWritable extends ArrayWritable {
    public Step2ArrayWritable() {
        super(Step2Writable.class);
    }

    public Step2ArrayWritable(Step2Writable[] values) {
        super(Step2Writable.class, values);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (String string : toStrings()) {
            builder.append(string).append("\n\t");
        }
        return builder.toString();
    }
}
