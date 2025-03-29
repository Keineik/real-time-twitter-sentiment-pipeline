package hcmus.group02.Spout;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TwitterFileListeningSpout extends BaseRichSpout {
    private final String filepath;
    private SpoutOutputCollector collector;
    private List<String> tweets;

    public TwitterFileListeningSpout(String filepath) {
        this.filepath = filepath;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));
    }

    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        tweets = IOUtils.readLines(
                Objects.requireNonNull(ClassLoader.getSystemResourceAsStream(filepath)),
                Charset.defaultCharset().name());
        tweets.remove(0);
    }

    @Override
    public void nextTuple() {
        for (String record : tweets) {
            Utils.sleep(10);
            collector.emit(new Values(record));
        }
    }
}
