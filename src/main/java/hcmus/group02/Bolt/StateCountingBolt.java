package hcmus.group02.Bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class StateCountingBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String,Integer> counts;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("state", "tweet_cnt"));
    }

    public void prepare(Map<String, Object> config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        String state = tuple.getStringByField("state");
        counts.put(state, counts.getOrDefault(state, 0) + 1);
        collector.emit(new Values(state, counts.get(state)));
        printCounts();
        collector.ack(tuple);
    }

    private void printCounts() {
        for (String state : counts.keySet()) {
            System.out.printf("%s has count of %s%n", state, counts.get(state));
        }
    }
}
