package com.ov.spark.training;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Sample Spark application that counts the words in a text file
 */
public class WordCount
{
	

    public static void wordCountJava7( String filename )
    {
        System.setProperty("hadoop.home.dir", "C:/hadoop");
    	// Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the input data, which is a text file read from the command line
        JavaRDD<String> input = sc.textFile( filename );

        // Java 7 and earlier
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String s) {
                        return Arrays.asList(s.split(" "));
                    }
                } );

        // Java 7 and earlier: transform the collection of words into pairs (word and 1)
        JavaPairRDD<String, Integer> counts = words.mapToPair(
            new PairFunction<String, String, Integer>(){
                public Tuple2<String, Integer> call(String s){
                        return new Tuple2(s, 1);
                    }
            } );

        // Java 7 and earlier: count the words
        JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
            new Function2<Integer, Integer, Integer>(){
                public Integer call(Integer x, Integer y){ return x + y; }
            } );

        // Save the word count back out to a text file, causing evaluation.
        reducedCounts.saveAsTextFile( "output" );
    }

    public static void wordCountJava8( String filename )
    {
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the input data, which is a text file read from the command line
        JavaRDD<String> input = sc.textFile( filename );
        
        /**
         * Traitement LogProcessing 
         */
        
        //Parser la ligne suivante l'objet SessionCatcher
        JavaRDD<SessionCatcher> linesParsed = input.map(l -> parsingLigne(l));
        
    	// Mapper les lignes parsées par IP
        JavaPairRDD<String, SessionCatcher> linesPairsByIP = linesParsed.mapToPair(lp -> new Tuple2(lp.getIp(), lp));
        
        
        //Reducer les lignes par key+ reste de la ligne selon la fonction mergeLines
        JavaPairRDD<String, SessionCatcher> res = linesPairsByIP.reduceByKey((l1,l2) -> mergeLines(l1,l2));
        
      //Reducer les lignes par key+ reste de la ligne selon la fonction mergeLines
        JavaPairRDD<String, String> res2 = res.mapToPair(r -> new Tuple2<String ,String> (r._1 , r._2.getResultMessageAccess()));
        
        // Save the word count back out to a text file, causing evaluation.
        res2.saveAsTextFile( "outputLogProcessingV7" );

        /**
         * Fin traitement LogProcessing
         */
        
        // ###################################################################################""
        
        /**
         * Traitement word COUNT
         */
        // Java 8 with lambdas: split the input string into words
//        JavaRDD<String> ip =input.flatMap(s -> Arrays.asList( s.split(" ") ) );
//        
//        // Java 8 with lambdas: split the input string into words
//        JavaRDD<String> words = input.flatMap(s -> Arrays.asList( s.split(" ") ) );
//
//        // Java 8 with lambdas: transform the collection of words into pairs (word and 1) and then count them
//        JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
//        		                                   .reduceByKey((x, y) -> x + y);
//        
//        JavaPairRDD<Integer ,String> countsReversed = counts.mapToPair(z -> new Tuple2<Integer ,String>(z._2 , z._1))
//                .reduceByKey((x, y) ->maxValue(x,y));
//        /**
//         * Fin traitement word count
//         */
//   
//
//        // Save the word count back out to a text file, causing evaluation.
//        countsReversed.saveAsTextFile( "outputReversed" );
    }
    
    private static SessionCatcher mergeLines(SessionCatcher l1, SessionCatcher l2) {
	
     	if (l1.getDateAccess() != null && l2.getDateAccess()!= null )
    	{
     	// Calculer la durée+ initialisation des dates 	
    	 Long sTimeDiff= l2.getDateAccess().getTime()- l1.getDateAccess().getTime() ;
    	 if(sTimeDiff >= 0)
    	   {
    		l1.setDateFirstAccess(l1.getDateAccess());
    		l1.setDateLastAccess(l2.getDateAccess());
    		l1.setDateDureeAccess(sTimeDiff+ l1.getDateDureeAccess());
    	   }
    	 else
    	   {
    		l1.setDateFirstAccess(l2.getDateAccess());
    		l1.setDateLastAccess(l1.getDateAccess());
    		l1.setDateDureeAccess((-1)*sTimeDiff+ l1.getDateDureeAccess());
    	   }	
    	}
     // Surcharger le type d'accés
    	if(!l1.getTypeAccess().contains(l2.getTypeAccess()))
    	{
    		l1.setTypeAccess(l1.getTypeAccess()+","+l2.getTypeAccess());	
    	}
    	// Surcharger la liste des URL + nbr de pages 
    	l1.setListeURLVisite(l1.getPageAccess());
    	if(!l1.getListeURLVisite().contains(l2.getPageAccess()))
    	{
    		l1.setNbURLvisite(l1.getNbURLvisite()+1);
    		l1.setPageAccess(l1.getListeURLVisite()+ " ; "+l2.getPageAccess());
    	}
    	// Surcharger le message a afficher 
    	l1.setResultMessageAccess("**** Date conx: "+l1.getDateFirstAccess()+"***** Date deconx : "+l1.getDateLastAccess() 
    	                                +"***** Duree de conx : "+TimeUnit.MILLISECONDS.toSeconds(l1.getDateDureeAccess()) + "** secondes **"
    	                                +"**** Requetes envoyées : "+l1.getTypeAccess() +"***** NB de pages visités :"+l1.getNbURLvisite()
    	                                +"***** Liste de pages visités : "+l1.getListeURLVisite()); 
    	
    	return l1;
    	
	}

	public static SessionCatcher parsingLigne (String ligne) throws ParseException
    {
    	//initialize parametres 
    	SessionCatcher resultSessionCatcher= new SessionCatcher();
    	String words[] = null;
    	// Date formatter
    	DateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    	// surcharger la liste des mots
    	words = ligne.split(" ");
    	// Surcharger le resultSessionCatcher
    	resultSessionCatcher.setIp(words[0]);
    	resultSessionCatcher.setDateAccess(format.parse((words[3]).substring(1)));
    	resultSessionCatcher.setTypeAccess((words[5]).substring(1));
    	resultSessionCatcher.setPageAccess(words[6]);
 		return resultSessionCatcher;
    }
    
    public static  String maxValue(String x,String y)
    {
    	if (x.length()>=y.length())
    	{
    		return x;
    	}
    	return y;
    }

    public static void main( String[] args )
    {
    	if( args.length == 0 )
        {
            System.out.println( "Usage: WordCount <file>" );
            System.exit( 0 );
        }

        wordCountJava8( args[ 0 ] );
    }
}	