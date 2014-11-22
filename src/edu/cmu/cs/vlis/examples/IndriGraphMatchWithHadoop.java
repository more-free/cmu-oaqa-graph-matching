package edu.cmu.cs.vlis.examples;

import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import edu.cmu.cs.vlis.adapter.IndriAdapter;
import edu.cmu.cs.vlis.adapter.IndriGraphEdge;
import edu.cmu.cs.vlis.adapter.IndriGraphEdgeFactory;
import edu.cmu.cs.vlis.adapter.IndriGraphNode;
import edu.cmu.cs.vlis.adapter.IndriGraphNodeFactory;
import edu.cmu.cs.vlis.matcher.ArbitraryGraphMatcher;
import edu.cmu.cs.vlis.matcher.GraphMatcher;
import edu.cmu.cs.vlis.matching.InexactMatching;
import edu.cmu.cs.vlis.matching.Matching;
import edu.uci.ics.jung.graph.Graph;

/**
 * This class shows an simple example of matching indri graph with Hadoop (the code
 * is based on Hadoop 1.1.2).
 * The pattern graph is a single graph, while target graphs might be huge and
 * stored in large files. This class runs a Hadoop job to match the pattern graph
 * against large target files.
 * 
 * Rewrite serializeIndriGraph(Graph) method below to get different serialization 
 * of matched result (in HDFS).  
 * 
 * Reset <matching> in main() to apply different matching algorithms.
 * 
 * Rewrite equals() method in SimpleNode class to apply different matching metrics.
 * 
 * @see IndriGraphMatch
 * 
 * @author ke xu (morefree7@gmail.com)
 *
 */
public class IndriGraphMatchWithHadoop {
	// pattern will be a single graph. set in main()
	private static Graph<IndriGraphNode, IndriGraphEdge> pattern;  
    private static IndriGraphNodeFactory nodeFactory = new SimpleNodeFactory();
    private static IndriGraphEdgeFactory edgeFactory = new SimpleEdgeFactory();
    private static IndriAdapter adapter = new IndriAdapter(nodeFactory, edgeFactory);
    
    // matching algorithm, set in main()
    private static Matching<IndriGraphNode, IndriGraphEdge> matching; 
    private static GraphMatcher<IndriGraphNode, IndriGraphEdge, List<Graph<IndriGraphNode, IndriGraphEdge>>>
    					matcher = new ArbitraryGraphMatcher<IndriGraphNode, IndriGraphEdge>(matching);
	
	private static class SimpleNode extends IndriGraphNode {
		public boolean equals(Object obj) {
			if (!(obj instanceof IndriGraphNode))
				return false;
			IndriGraphNode node = (IndriGraphNode) obj;

			return equals(this, node);
		}

		// change here for different flavors of comparison
		private boolean equals(IndriGraphNode fir, IndriGraphNode sec) {
			return fir.text.contains(sec.text) || sec.text.contains(fir.text);
			// return fir.tagID == sec.tagID;
		}

		// change here for different visualization
		public String toString() {
			return this.text;
			// return this.tagID + "";
		}
	}

	private static class SimpleEdge extends IndriGraphEdge {
		public SimpleEdge(String name) {
			this.edgeName = name;
		}

		public String toString() {
			return edgeName;
		}
	}

	private static class SimpleNodeFactory extends IndriGraphNodeFactory {
		public IndriGraphNode createEmptyNode() {
			return new SimpleNode();
		}
	}

	private static class SimpleEdgeFactory extends IndriGraphEdgeFactory {
		public IndriGraphEdge createEdge(IndriGraphNode src, IndriGraphNode dst) {
			if (src == null || dst == null)
				return null;
			return new SimpleEdge(src.tagID + "->" + dst.tagID);
		}
	}
 
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
      private IntWritable docID = new IntWritable(0);
    	
      public void map(LongWritable key,
                      Text value,
                      OutputCollector<IntWritable, Text> output,
                      Reporter reporter)
              throws IOException {
        String line = value.toString();
        int pos = line.indexOf('\t');
        
        docID.set(Integer.parseInt(line.substring(0, pos)));
        output.collect(docID, value);
      }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
      private Text text = new Text();
      
      public void reduce(IntWritable key,
                         Iterator<Text> values,
                         OutputCollector<IntWritable, Text> output,
                         Reporter reporter) throws IOException {
        List<String> lines = new ArrayList<String>();
        while(values.hasNext()) 
        	lines.add(values.next().toString());
        
        Graph<IndriGraphNode, IndriGraphEdge> target = adapter.convert(lines).get(0);
        List<Graph<IndriGraphNode, IndriGraphEdge>> matched = matcher.match(pattern, target).getMatchResult();
        for(Graph<IndriGraphNode, IndriGraphEdge> g : matched) {
        	text.set(serializeIndriGraph(g));
        	output.collect(key, text);
        }
      }
    }
    
    // change this method for different kind of storage
    private static String serializeIndriGraph(Graph<IndriGraphNode, IndriGraphEdge> graph) {
    	StringBuilder s = new StringBuilder();
    	for(IndriGraphNode node : graph.getVertices()) {
    		s.append(node.tagID + " ");
    	}
    	
    	String res = s.toString();
    	return res.substring(0, res.length() - 1);
    }

    public static void main(String[] args) throws Exception {
      if(args.length != 3) {
      	  System.out.println("parameters : target-file-dir(HDFS)  result-file-dir(HDFS)  patternFile(local)");
      	  return;
      }
      
      String patternFileLocation = args[2];
      pattern = adapter.convert(patternFileLocation).get(0);
      matching = new InexactMatching<IndriGraphNode, IndriGraphEdge>();
      
      // run a hadoop job
      JobConf conf = new JobConf(IndriGraphMatchWithHadoop.class);
      conf.setJobName("indri graph match with hadoop");
      
      conf.setMapOutputKeyClass(IntWritable.class);
      conf.setMapOutputValueClass(Text.class);
      
      conf.setOutputKeyClass(IntWritable.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(Map.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
      
      FileInputFormat.setInputPaths(conf, new Path(args[0])); // target files
      FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
      
      JobClient.runJob(conf);
    }
}
