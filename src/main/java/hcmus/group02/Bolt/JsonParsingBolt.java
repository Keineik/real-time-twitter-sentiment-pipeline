package hcmus.group02.Bolt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JsonParsingBolt extends BaseRichBolt {
    private static final Logger LOGGER = Logger.getLogger(JsonParsingBolt.class.getName());
    private OutputCollector collector;
    private final List<String> fields;

    public JsonParsingBolt(String fields) {
        this.fields = Arrays.asList(fields.split(","));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(fields));
    }

    public void prepare(Map<String, Object> config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        try {
            // Get the json record
            String record = tuple.getStringByField("record");

            // Convert the record to an instance of Map
            ObjectMapper mapper = new ObjectMapper();
            Map<String, String> map = mapper.readValue(record, new TypeReference<Map<String, String>>() {});

            // Build the tuple by getting each field from the map
            List<String> outputValues = new ArrayList<>();
            for (String field : fields) {
                outputValues.add(map.get(field) != null ? map.get(field) : null);
            }

            // Emit the parsed result to stream
            collector.emit(new Values(outputValues.toArray()));
            collector.ack(tuple);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error in JsonParsingBolt", e);
            this.collector.fail(tuple);
        }
    }
}
