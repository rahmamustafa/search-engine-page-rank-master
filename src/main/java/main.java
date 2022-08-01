import java.io.*;
import java.net.URL;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

public class main {
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text textDocId = new Text();
        private Text keyyy = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] d=value.toString().split("::::::::::::;!",0);
            if(d.length<2)
                return;
            String DocId = d[0];
            String Doc = d[1];
            StringTokenizer itr = new StringTokenizer(Doc);

            while (itr.hasMoreTokens()) {
                keyyy.set(itr.nextToken()+"::::::::::::;!");
                context.write(keyyy, new Text(DocId));
            }
        }
    }
    public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            String text="";
            HashMap<String,Integer> map=new HashMap<String, Integer>();
            for (Text val : values) {
                text = val.toString();
                if(map.containsKey(text))
                    map.put(text,map.get(text)+1);
                else
                    map.put(text,1);

            }
            text="";
            for (String v: map.keySet()) {
                text+="("+v+","+map.get(v)+");";

            }
            result.set(text);
            context.write(key, result);

        }
    }

    public static class SearchWithWordsMapper extends Mapper<Object, Text, Text, Text>{


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf =context.getConfiguration();
            String word= conf.get("word");

            String[] words=word.split(" ");


            String[] document=value.toString().split("::::::::::::;!\\s+",0);
            String id = document[0].toLowerCase();
            if(document.length<2&&id.contains("[^A-Za-z[0-9] ]")){
                return;}
            id = id.toLowerCase();
            String doc = document[1];
            for(String w:words){
                if(id.equals(w)){
                    String[] pages=doc.split(";");
                    for (String sss :pages) {
                        if(sss.contains("(")){
                            int index = sss.indexOf("(");
                            int splitIndex = sss.indexOf(",");
                            context.write(new Text(sss.substring(index+1,splitIndex)),  new Text(words.length+":"+w));
                        }
                    }


                }
            }


        }
    }
    public static class SearchWithWordsReducer extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String ranknUnit ="";
            int wordslength=0;
            List<String> words = new ArrayList<String>();

            for (Text value: values) {
                String v=value.toString();
                if(v.contains(":")) {
                    String[] wordss=value.toString().split(":");
                    if(!words.contains(wordss[1])){

                        wordslength=Integer.parseInt(wordss[0]);
                        words.add(wordss[1]);
                    }

                }
                else{
                    ranknUnit=value.toString();
                }

            }
            if(!words.isEmpty()&&!ranknUnit.equals("")) {
                String result = words.stream()
                        .map(n -> String.valueOf(n))
                        .collect(Collectors.joining(" ","",""));
                double newRank=(Double.parseDouble(ranknUnit)*1/(Math.pow(wordslength,10))*Math.pow(words.size(),10));

                context.write(key, new Text(Double.parseDouble(ranknUnit)+";"+newRank+";"+result+";"+wordslength+";"+words.size()));
            }

        }
    }


    public static class RelationsMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fromTo = value.toString().split(":");
            if(fromTo.length<2) {
                return;
            }
            String from = fromTo[0];
            String[] toList = fromTo[1].split(",");
            if(toList.length==0) {
                return;
            }
            Double listvalue = 1.0/ toList.length;
            for (String to: toList) {
                context.write(new Text(from),  new Text(to+":"+(listvalue)));
            }
        }
    }
    public static class GetPreviousRankMapper
            extends Mapper<Object, Text, Text, Text>{


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] rank=value.toString().split("\\s+");
            if(rank.length!=2){

                return;}
            String p = rank[0];
            String pr = rank[1];

            context.write(new Text(p),new Text(pr));
        }

    }

    public static class MultiplicationReducer
            extends Reducer<Text,Text,Text,DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<String> transitionUnit = new ArrayList<String>();
            double prUnit = 0;
            for (Text value: values) {
                if(value.toString().contains(":")) {
                    transitionUnit.add(value.toString());

                }
                else
                    prUnit = Double.parseDouble(value.toString());


            }
            if(!transitionUnit.isEmpty())
                for (String unit: transitionUnit) {
                    String to=unit.split(":")[0];
                    context.write(new Text(to), new DoubleWritable(prUnit*Double.parseDouble(unit.split(":")[1])));
                }
            else
                return;
        }
    }


    public static class MultiplicationSummationMapper
            extends Mapper<Object, Text, Text, Text>{


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] rank=value.toString().split("\\s+");
            if(rank.length!=2){

                return;}
            String to = rank[0];
            String pr = rank[1];
            //
            context.write(new Text(to),new Text(pr));
        }

    }

    public static class MultiplicationSummationReducer
            extends Reducer<Text,Text,Text,DoubleWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Double sum = 0.0;
            for (Text value: values) {

                sum+=Double.parseDouble(value.toString());

            }

            context.write(key, new DoubleWritable((1-0.85)+0.85*sum));
        }
    }

    public static void main(String[] args) throws Exception {


        Configuration conf2 = new Configuration();
        FileSystem fs2 = FileSystem.get(conf2);
        System.out.println(fs2.getWorkingDirectory().toString()+"/");
        ///System.exit(1);
        String loc=args[0];
        String invertedIndex =loc+"input/invertedIndex2";
        String inputRelations=loc+"input/URLSRelation";
        String inputRank=loc+"input/rank2/rank";
        String multiplyRes= loc+"input/multiplication_result2/mr";

        String inputURLS=loc+"input/URLS/URLS";
        String relatedPages=loc+"related_page/";
        String outputRank=loc+"output_pages.html";
        String pagesContent=loc+"input/pageMini";
        int URLSFileSNumber=11;
        String word=args[1];
        String wordToSearch = word.replaceAll("[^A-Za-z[0-9] ]", " ").toLowerCase();
        //System.out.println(word);
        String iterations="2";
        String ranking="";
        if(args.length==3) {
            ranking=args[2];
        }
        else  if(args.length>3) {
            ranking = args[2];
            iterations=args[3];
        }




        ///pages ranking
        if(ranking.equals("ranking")||ranking.equals("all")||!fs2.exists(new Path(inputRank + "Final"))){
            for (int i=1;i<=Integer.parseInt(iterations);) {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf);
                job.setJarByClass(main.class);
                MultipleInputs.addInputPath(job, new Path(inputRelations), TextInputFormat.class, RelationsMapper.class);

                MultipleInputs.addInputPath(job, new Path(inputRank + i), TextInputFormat.class, GetPreviousRankMapper.class);

                job.setReducerClass(MultiplicationReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);



                FileSystem fs = FileSystem.get(conf);
                boolean exists = fs.exists(new Path(multiplyRes + i));
                if (exists) {
                    fs.delete(new Path(multiplyRes + i), true);
                }
                FileOutputFormat.setOutputPath(job, new Path(multiplyRes + i));
                job.waitForCompletion(true);


                conf = new Configuration();
                job = Job.getInstance(conf);
                job.setJarByClass(main.class);
                MultipleInputs.addInputPath(job, new Path(multiplyRes + i), TextInputFormat.class, MultiplicationSummationMapper.class);
                job.setReducerClass(MultiplicationSummationReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                i = i + 1;
                System.out.print(i);
                fs = FileSystem.get(conf);
                String outputLocation;
                if (i == Integer.parseInt(iterations) + 1)
                    outputLocation = inputRank + "Final";
                else
                    outputLocation = inputRank + i;
                exists = fs.exists(new Path(outputLocation));
                if (exists) {
                    fs.delete(new Path(outputLocation), true);
                }
                FileOutputFormat.setOutputPath(job, new Path(outputLocation));


                job.waitForCompletion(true);
            }}




        //inverted index
        if(ranking.equals("ir")||ranking.equals("all")||!fs2.exists(new Path(invertedIndex))){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(main.class);



            MultipleInputs.addInputPath(job, new Path(pagesContent), TextInputFormat.class, InvertedIndexMapper.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(conf);
            boolean exists = fs.exists(new Path(invertedIndex));
            if (exists) {
                fs.delete(new Path(invertedIndex), true);
            }
            FileOutputFormat.setOutputPath(job, new Path(invertedIndex));
            job.waitForCompletion(true);

        }

        Instant start = Instant.now();
        //Searching Word
        if(!ranking.equals("x")||!fs2.exists(new Path(relatedPages))) {
            Configuration conf = new Configuration();
            conf.set("word", wordToSearch);
            Job job = Job.getInstance(conf);
            job.setJarByClass(main.class);

            MultipleInputs.addInputPath(job, new Path(invertedIndex), TextInputFormat.class, SearchWithWordsMapper.class);
            MultipleInputs.addInputPath(job, new Path(inputRank + "Final"), TextInputFormat.class, GetPreviousRankMapper.class);

            job.setReducerClass(SearchWithWordsReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            FileSystem fs = FileSystem.get(conf);
            boolean exists = fs.exists(new Path(relatedPages));
            if (exists) {
                fs.delete(new Path(relatedPages), true);
            }
            FileOutputFormat.setOutputPath(job, new Path(relatedPages));
            job.waitForCompletion(true);

        }

        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        long duration =timeElapsed.toMillis();
        System.out.println("Time taken: "+duration  +" msec");

        getGreatestHDFS(relatedPages,inputURLS,outputRank,word,duration,URLSFileSNumber);

        Runtime rt = Runtime.getRuntime();
        Path path = new Path(outputRank);
        if(conf2.get("fs.default.name").equals("hdfs://localhost:54310")){
            System.out.println(fs2.getWorkingDirectory().toString()+"/"+path);
        }
        else{rt.exec("/usr/bin/firefox -new-tab file://"+fs2.getWorkingDirectory().toString()+"/"+path);}
        System.exit(1);
    }























    public static class Greatest implements Comparable<Greatest>{
        public Double rank;
        public int number;
        public String pageName;

        public Greatest(double v, int i,String l) {
            rank =v;
            number =i;
            pageName=l;
        }

        public Double getRank() {
            return rank;
        }

        public void setRank(Double rank) {
            this.rank = rank;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }

        public String getPageName() {
            return pageName;
        }

        public void setPageName(String pageName) {
            this.pageName = pageName;
        }

        @Override
        public int compareTo(Greatest greatestTemp) {
            double temp = greatestTemp.getRank();

            return (int) (temp*100000000 - this.getRank()*100000000);

        }
    }


    public static void getGreatestHDFS(String rankLocation,String URLSLocation,String output,String word,long duration,int URLSFileSNumber) {
        try {
            Configuration conf =new Configuration();
            FileSystem fs =FileSystem.get(conf);
            Path path = new Path(rankLocation+"part-r-00000");
            FSDataInputStream ris=fs.open(path);

            Scanner inputFile = new Scanner(ris);
            int sortedPagesNumber = 20;

            Greatest[] highestpages= new Greatest[sortedPagesNumber+1];
            for(int i=0;i<=sortedPagesNumber;i++)
                highestpages[i]=new Greatest(0.0,-5,"");

            while (inputFile.hasNext()) {

                String[] strs = inputFile.nextLine().split("\t");
                String strNum = strs[1];

                if (!strNum.equals("")) {
                    String[] s=strNum.split(";");
                    Double temp = Double.parseDouble(s[1]);
                    Greatest lastPage=highestpages[sortedPagesNumber];

                    if (temp > lastPage.getRank()) {
                        lastPage.setRank(temp);
                        lastPage.setNumber(Integer.parseInt(strs[0]));
                        lastPage.setPageName(s[2]);

                        Arrays.sort(highestpages);
                    }
                }
            }inputFile.close();
            DecimalFormat df = new DecimalFormat("0.0");
            Path file = new Path(output);
            if ( fs.exists( file )) { fs.delete( file, true ); }
            OutputStream os = fs.create( file,
                    new Progressable() {
                        public void progress() {
                            //out.println("...bytes written: [ "+bytesWritten+" ]");
                        } });
            BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
            br.write("<html>\n    " +
                    "<head>\n" +
                    "        <title>Blind Search Engine</title>\n" +
                    "    </head>\n" +
                    "    <body>\n" +
                    "<center><h1>Blind Search Engine</h1></center>"+
                    "<h2>You searched with \""+ word+"\"</h2>\n"+
                    "<h3>&nbsp;&nbsp;&nbsp;&nbsp;Result in :"+df.format(duration/1000.0)+" sec </h3>"+
                    "        <ul>");



            String wordToSearch=word.replaceAll("[^A-Za-z[0-9] ]", "").toLowerCase();
            for (int i=0;i<sortedPagesNumber;i++) {
                if(highestpages[i].number <=-5||highestpages[i].rank <1){
                    System.out.print("only "+i+" pages founded");
                    br.write(
                            "<p>only "+ i+" results</p>\n");
                    break;
                }

                String page = searchPageByIDHDFS(highestpages[i].number,URLSLocation,fs,URLSFileSNumber);
                String namePage=getPageName(page);
                if(namePage.equals(""))
                    namePage=highestpages[i].pageName;



                System.out.println((i+1) + ") page number:: " +highestpages[i].number + " link: " +page+"\nrank: "+ df.format(highestpages[i].rank) +"\n"+"Words: "+highestpages[i].pageName);

                br.write("            <li>\n" +
                        "                <a href=\""+page+"\">"+namePage+"</a>\n" +
                        "            <ul>\n" +
                        "            <li>\n" +
                        "                <a href=\""+page+"\">"+page+"</a>\n" +"            </li>"
                );

                String[] smallWords=highestpages[i].pageName.split(" ");

                String missingWords=wordToSearch;
                for(String w : smallWords)
                    missingWords= missingWords.replaceAll(w,"");
                if(!missingWords.trim().equals("")){
                    br.write("            <li>\n"+"                page not include: "+ missingWords+"            </li>");
                }
                br.write(
                        "            </ul>\n" +
                                "            </li>");
            }




            br.write("        </ul>\n" +
                    "    </body>\n" +
                    "</html>");
            br.close();




        } catch (IOException e) {
            System.out.println("Problem finding file");
        }

    }

    private static String getPageName(String page) {

        try{
            URL url=new URL(page);
            String hostname=url.getHost();

            int splitIndex = hostname.indexOf(".");
            int splitIndex2 =hostname.indexOf(".",splitIndex+1);



            if(splitIndex!=-1&&splitIndex2!=-1){

                return hostname.substring(splitIndex+1,splitIndex2);}
            else if(splitIndex!=-1&&splitIndex2==-1){

                return hostname.substring(0,splitIndex);
            }
            else
                return hostname;
        }catch(Exception e){
            System.out.println(e);
            return "";
        }
    }

    public static String searchPageByIDHDFS(int num, String URLSLocation, FileSystem fs,int URLSFileSNumber) throws IOException {



        for(int i=0;i<=URLSFileSNumber;i++) {
            Path path2 = new Path(URLSLocation + i + ".txt");

            FSDataInputStream uis=fs.open(path2);
            Scanner inputFile = new Scanner(uis);
            while (inputFile.hasNext()) {
                //String strNum = new Main().buildNumber(inputFile.nextLine());
                String[] strs = inputFile.nextLine().split(":",2);
                String strNum = strs[0];
                if(strNum.equals(String.valueOf(num))){
                    return strs[1];
                }
            }
        }
        return "not founded";
    }



}