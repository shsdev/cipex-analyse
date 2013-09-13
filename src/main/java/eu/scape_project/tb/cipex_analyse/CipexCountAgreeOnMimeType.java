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
public class CipexCountAgreeOnMimeType {

    private static CliConfig config;

    /**
     * Reducer class.
     */
    public static class CipexCountAgreeOnMimeTypeReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    /**
     * Mapper class.
     */
    public static class CipexCountAgreeOnMimeTypeMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException, FileNotFoundException {
            // unixfile-tika   1
            // droid-unixfile  1
            // droid-tika  1
            String[] keyVal = value.toString().split("\t");
            Text outKey = new Text(keyVal[0]);
            LongWritable outVal = new LongWritable(1L);
            context.write(outKey, outVal);
        }
    }

    public CipexCountAgreeOnMimeType() {
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

            job.setJarByClass(CipexCountAgreeOnMimeType.class);

            job.setMapperClass(CipexCountAgreeOnMimeType.CipexCountAgreeOnMimeTypeMapper.class);
            job.setReducerClass(CipexCountAgreeOnMimeType.CipexCountAgreeOnMimeTypeReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

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
