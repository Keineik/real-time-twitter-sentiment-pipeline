package hcmus.group02.Spout;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TwitterFileListenerSpout extends BaseRichSpout {
    private SpoutOutputCollector outputCollector;
    private List<String> tweets;

    // Define the field names for all tuples emitted by the spout
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));
    }

    // Gets called when Storm prepares the spout to be run
    @Override
    public void open(Map configMap, TopologyContext context, SpoutOutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        tweets = IOUtils.readLines(
                Objects.requireNonNull(ClassLoader.getSystemResourceAsStream("./data/hashtag_joebiden.csv")),
                Charset.defaultCharset().name());
        tweets.remove(0);
    }

    // Called by Storm when it's ready to read the next tuple for the spout
    @Override
    public void nextTuple() {
        for (String record : tweets) {
            outputCollector.emit(new Values(record));
        }
    }
}
