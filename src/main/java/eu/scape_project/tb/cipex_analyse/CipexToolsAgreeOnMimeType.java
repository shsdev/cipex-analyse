/*
 * Copyright 2012 The SCAPE Project Consortium.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * under the License.
 */
package eu.scape_project.tb.cipex_analyse;

import eu.scape_project.tb.cipex_analyse.cli.CliConfig;
import eu.scape_project.tb.cipex_analyse.cli.Options;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * CipexToolsAgreeOnMimeType
 */
public class CipexToolsAgreeOnMimeType {

    private static CliConfig config;
    
    private static final String VALUESEPARATOR = "~";

    /**
     * Reducer class.
     */
    public static class CipexToolsAgreeOnMimeTypeReducer
            extends Reducer<Text, Text, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            LongWritable one = new LongWritable(1L);
            String droidMime = null;
            String unixfileMime = null;
            String tikaMime = null;
            for (Text val : values) {
                String valStr = val.toString();
                if (valStr.startsWith("droid~")) {
                    droidMime = valStr.substring(6); // only mime type result
                } else if (valStr.startsWith("unixfile~")) {
                    unixfileMime = valStr.substring(9); // only mime type result
                } else if (valStr.startsWith("tika~")) {
                    tikaMime = valStr.substring(5); // only mime type result
                }
            }
            if(droidMime != null && unixfileMime != null) {
                if(droidMime.equals(unixfileMime)) {
                    context.write(new Text("droid-unixfile"),one);
                }
            }
            if(droidMime != null && tikaMime != null) {
                if(droidMime.equals(tikaMime)) {
                    context.write(new Text("droid-tika"),one);
                }
            }
            
            if(droidMime != null && tikaMime != null) {
                if(droidMime.equals(tikaMime)) {
                    context.write(new Text("unixfile-tika"),one);
                }
            }
        }
    }

    /**
     * Mapper class.
     */
    public static class CipexToolsAgreeOnMimeTypeMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException, FileNotFoundException {
            // /010.zip5149771439614046179/010/010000.txt	tika	mime	text/plain
            // /010.zip5149771439614046179/010/010000.txt	unixfile	mime	text/plain
            // /010.zip5149771439614046179/010/010000.txt	droid	puid	fmt/0
            // /010.zip5149771439614046179/010/010000.txt	droid	mime
            String line = value.toString();
            String[] cols = line.split("\t");
            // Each line has 4 columns
            if (cols.length == 4) {
                String pathkey = (String) cols[0];
                String tool = (String) cols[1];
                String type = (String) cols[2];
                String idres = (String) cols[3];
                Text outKey = new Text(pathkey);
                // consider only mime type identification results
                if(type.equals("mime")) {
                    Text outVal = new Text(tool+VALUESEPARATOR+idres);
                    // Key: 010.zip5149771439614046179/010/010000.txt , Value: droid~text/plain
                    // Key: 010.zip5149771439614046179/010/010000.txt , Value: unixfile~text/plain
                    // Key: 010.zip5149771439614046179/010/010000.txt , Value: tika~application/octet-stream
                    context.write(outKey, outVal);
                }
                
            }
        }
    }

    public CipexToolsAgreeOnMimeType() {
    }

    public static CliConfig getConfig() {
        return config;
    }

    /**
     * Main entry point.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Command line interface
        config = new CliConfig();
        CommandLineParser cmdParser = new PosixParser();
        GenericOptionsParser gop = new GenericOptionsParser(conf, args);
        CommandLine cmd = cmdParser.parse(Options.OPTIONS, gop.getRemainingArgs());
        if ((args.length == 0) || (cmd.hasOption(Options.HELP_OPT))) {
            Options.exit("Usage", 0);
        } else {
            Options.initOptions(cmd, config);
        }


        startHadoopJob(conf);

    }

    public static void startHadoopJob(Configuration conf) {
        try {
            Job job = new Job(conf, "cipex-analyse");

            // local debugging (pseudo-distributed)
            // job.getConfiguration().set("mapred.job.tracker", "local");
            // job.getConfiguration().set("fs.default.name", "file:///");

            job.setJarByClass(CipexToolsAgreeOnMimeType.class);

            job.setMapperClass(CipexToolsAgreeOnMimeType.CipexToolsAgreeOnMimeTypeMapper.class);
            job.setReducerClass(CipexToolsAgreeOnMimeType.CipexToolsAgreeOnMimeTypeReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            SequenceFileInputFormat.addInputPath(job, new Path(config.getDirStr()));
            String outpath = "output/" + System.currentTimeMillis();
            FileOutputFormat.setOutputPath(job, new Path(outpath));
            job.waitForCompletion(true);
            System.out.print(outpath);
            System.exit(0);
        } catch (Exception e) {
            System.out.println("I/O Error");
        }

    }
}
