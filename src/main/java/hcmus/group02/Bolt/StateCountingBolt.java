package hcmus.group02.Bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class StateCountingBolt extends BaseRichBolt {
    private Map<String,Integer> counts;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // This bolt does not emit anything and therefore does not declare any output fields.
    }

    public void prepare(Map<String, Object> config, TopologyContext context, OutputCollector collector) {
        this.counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple input) {
        String state = input.getStringByField("state");
        counts.put(state, counts.getOrDefault(state, 0) + 1);
        printCounts();
    }

    private void printCounts() {
        for (String state : counts.keySet()) {
            System.out.printf("%s has count of %s%n", state, counts.get(state));
        }
    }
}
