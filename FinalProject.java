import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.Map.Entry;
import java.util.stream.StreamSupport;

public class FinalProject {

    public static class ProbabilityMapper extends Mapper<Object, Text, Text, Text>{

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] x = line.split(",");
        
        if (!x[0].equals("Year")) {
            String ontime = "0";
            String arrivalDelay = x[14];
            
            if (!arrivalDelay.equals("NA")) {
                int d = Integer.parseInt(arrivalDelay);
                if (d <= 15) {
                    ontime = "1";
                }
                String carrier = x[8];
                context.write(new Text(carrier), new Text(ontime));
            }
        }
    }
}

public class ProbabilityReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Double> carrierAverages = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Integer> counts = new ArrayList<>();
        values.forEach(value -> counts.add(Integer.parseInt(value.toString())));

        double total = counts.stream().mapToInt(Integer::intValue).sum();
        double average = total / counts.size();

        carrierAverages.put(key.toString(), average);
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        List<Map.Entry<String, Double>> sortedAverages = carrierAverages.entrySet()
            .stream()
            .sorted(Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()))
            .collect(Collectors.toList());

        writeEntries(context, sortedAverages.subList(0, Math.min(sortedAverages.size(), 3)), "highest");
        writeEntries(context, sortedAverages.subList(Math.max(0, sortedAverages.size() - 3), sortedAverages.size()), "lowest");

        if (sortedAverages.isEmpty()) {
            context.write(new Text("N/A"), new Text(""));
        }
    }

private void writeEntries(Context context, List<Map.Entry<String, Double>> entries, String category) throws IOException, InterruptedException {
    context.write(new Text(category), new Text(""));
    for (Map.Entry<String, Double> entry : entries) {
        context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
    }
}

}

public class TaxiMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] x = line.split(",");

        String year = x[0];
        String origin = x[16];
        String destination = x[17];
        String i = x[19];
        String o = x[20];

        if (!year.equals("Year") && !origin.equals("NA") && !destination.equals("NA") && !i.equals("NA") && !o.equals("NA")) {
            int taxiInValue = Integer.parseInt(i);
            int taxiOutValue = Integer.parseInt(o);

            context.write(new Text(origin), new IntWritable(taxiOutValue));
            context.write(new Text(destination), new IntWritable(taxiInValue));
        }
    }
}




public class TaxiReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Integer> taxiTimes = new ArrayList<>();

        for (Text value : values) {
            taxiTimes.add(Integer.parseInt(value.toString()));
        }

        double avg = taxiTimes.stream().mapToInt(Integer::intValue).average().orElse(0.0);

        if (avg == 0.0) {
            context.write(key, new Text("Exception: Zero Average"));
        } else {
            context.write(key, new Text(String.valueOf(avg)));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, List<Integer>> taxiTimesMap = new HashMap<>();

        for (Entry<Text, List<Integer>> entry : taxiTimesMap.entrySet()) {
            double avg = entry.getValue().stream().mapToInt(Integer::intValue).average().orElse(0.0);
            context.write(entry.getKey(), new Text(String.valueOf(avg)));
        }
    }
}




public class CancellationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final IntWritable ONE = new IntWritable(1);
    private Text cancellationReason = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        if (!line.startsWith("Year")) {  
            String[] x = line.split(",");
            
            String cancellationIndicator = x[21];
            String cancellationReasonCode = x[22].trim();
            
            if ("1".equals(cancellationIndicator) && !cancellationReasonCode.equals("NA") && !cancellationReasonCode.isEmpty()) {
                cancellationReason.set(cancellationReasonCode);
                context.write(cancellationReason, ONE);
            }
        }
    }
}
public class CancellationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = StreamSupport.stream(values.spliterator(), false)
                .mapToInt(IntWritable::get)
                .sum();
        context.write(key, new IntWritable(count));
    }

}}
