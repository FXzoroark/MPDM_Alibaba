package writables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class Step2Writable implements Writable, Comparable<Step2Writable> {

    private int startingTime;
    private int nbCores;
    private int duree;

    public Step2Writable() {
        set(0, 0, 0);
    }

    public Step2Writable(int startingTime, int nbCores, int duree) {
        set(startingTime, nbCores, duree);
    }

    public void set(int startingTime, int nbCores, int duree) {
        this.startingTime = startingTime;
        this.nbCores = nbCores;
        this.duree = duree;
    }

    public int getStartingTime() {
        return startingTime;
    }

    public int getNbCores() {
        return nbCores;
    }

    public int getDuree() {
        return duree;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(startingTime);
        out.writeInt(nbCores);
        out.writeInt(duree);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        startingTime = in.readInt();
        nbCores = in.readInt();
        duree = in.readInt();
    }

    @Override
    public String toString() {
        return startingTime + "\t" + nbCores + "\t" + duree;
    }

    @Override
    public int compareTo(Step2Writable other) {
        return Integer.compare(other.nbCores, this.nbCores);
    }
}
