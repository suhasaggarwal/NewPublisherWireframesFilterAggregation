package com.publisherdata.Daos;

import com.publisherdata.model.PublisherReport;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javolution.util.FastMap;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CSVResultsExtractor;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

public class AggregationModule
{
  private static TransportClient client;
  private static SearchDao searchDao;
  private static AggregationModule INSTANCE;
  
  public static Map<String,String> citycodeMap;
  static {
      Map<String, String> cityMap = new HashMap<String,String>();
      String csvFile = "/root/citycode1.csv";
      BufferedReader br = null;
      String line = "";
      String cvsSplitBy = ",";
      String key = "";
      Map<String, String> cityMap1  = new HashMap<String,String>();
      try {

          br = new BufferedReader(new FileReader(csvFile));
         
          while ((line = br.readLine()) != null) {

             try{
          	// use comma as separator
              line = line.replace(",,",", , ");
          	//   System.out.println(line);
          	String[] geoDetails = line.split(cvsSplitBy);
              key = geoDetails[6];
              cityMap1.put(key,geoDetails[5]);
            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
             }
             catch(Exception e)
             {
          	   e.printStackTrace(); 
             }

          }


        
      
      }

      
      
      
catch(Exception e){
	
	e.printStackTrace();
} 

      
      citycodeMap = Collections.unmodifiableMap(cityMap1);  
  //    System.out.println(citycodeMap);
  }
  
  
  
  
  
  public static Map<String,String> oscodeMap;
  static {
      Map<String, String> osMap = new HashMap<String,String>();
      String csvFile = "/root/oscode1.csv";
      BufferedReader br = null;
      String line = "";
      String cvsSplitBy = ",";
      String key = "";
      Map<String, String> osMap1  = new HashMap<String,String>();
      try {

          br = new BufferedReader(new FileReader(csvFile));
         
          while ((line = br.readLine()) != null) {

             try{
          	// use comma as separator
              line = line.replace(",,",", , ");
          	//   System.out.println(line);
          	String[] osDetails = line.split(cvsSplitBy);
              key = osDetails[0];
              osMap1.put(key,osDetails[1]);
            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
             }
             catch(Exception e)
             {
          	   e.printStackTrace(); 
             }

          }


        
      
      }

      
      
      
catch(Exception e){
	
	e.printStackTrace();
} 

      
      oscodeMap = Collections.unmodifiableMap(osMap1);  
      System.out.println(oscodeMap);
  }
   
 
  
  public static Map<String,String> devicecodeMap;
  static {
      Map<String, String> deviceMap = new HashMap<String,String>();
      String csvFile = "/root/devicecode.csv";
      BufferedReader br = null;
      String line = "";
      String cvsSplitBy = ",";
      String key = "";
      Map<String, String> deviceMap1  = new HashMap<String,String>();
      try {

          br = new BufferedReader(new FileReader(csvFile));
         
          while ((line = br.readLine()) != null) {

             try{
          	// use comma as separator
              line = line.replace(",,",", , ");
          	 //  System.out.println(line);
          	String[] deviceDetails = line.split(cvsSplitBy);
              key = deviceDetails[0];
              deviceMap1.put(key,deviceDetails[1]+","+deviceDetails[4]);
            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
             }
             catch(Exception e)
             {
          	   e.printStackTrace(); 
             }

          }


        
      
      }

      
      
      
catch(Exception e){
	
	e.printStackTrace();
} 

      
      devicecodeMap = Collections.unmodifiableMap(deviceMap1);  
   //   System.out.println(deviceMap);
  }
  
  
  
  
  
  
  
  
  public static AggregationModule getInstance()
  {
    if (INSTANCE == null) {
      return new AggregationModule();
    }
    return INSTANCE;
  }
  
  public static void main(String[] args)
    throws Exception
  {
	  
	
	 //setUp();
     AggregationModule mod = new  AggregationModule();
    
     /*
     mod.countOS("2017-01-01","2017-01-31");
     mod.counttotalvisitorsChannelSectionDateHourlywise("2017-01-19","2017-01-19","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.counttotalvisitorsChannelSectionDateHourlyMinutewise("2017-01-16 13:00:01","2017-01-16 13:59:59","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.countNewUsersChannelSectionDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.countReturningUsersChannelSectionDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.countLoyalUsersChannelSectionDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.countNewUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_trending_moles_on_these_areas_signify_wealth_and_luck_read_to_know");
     mod.countReturningUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_trending_moles_on_these_areas_signify_wealth_and_luck_read_to_know");
     mod.countLoyalUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_trending_moles_on_these_areas_signify_wealth_and_luck_read_to_know");
     mod.getGenderChannelSection("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.gettimeofdayChannelSection("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     */
     
     /*
     mod.getChannelSectionArticleCount("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.getChannelSectionArticleList("2017-01-01","2017-01-31","Womanseraindia_indiagate","*");
     mod.getChannelArticleReferrerList("2017-01-01","2017-01-31","Womanseraindia_indiagate","amitabh");
     mod.getChannelArticleReferredPostsList("2017-01-01","2017-01-31","Womanseraindia_indiagate","adult");
     mod.countfingerprintChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","adult");
     mod.countfingerprintChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","adult");
     mod.counttotalvisitorsChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","adult");
     mod.counttotalvisitorsChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","adult");
     mod.countAudiencesegmentChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate", "http___womansera_com_trending_moles_on_these_areas_signify_wealth_and_luck_read_to_know");
  */   
     
  /*   
     mod.getChannelSectionArticleCount("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.getChannelSectionArticleList("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.getChannelSectionReferrerList("2017-01-01","2017-01-31","Womanseraindia_indiagate","entertainment");
     mod.getChannelSectionReferredPostsList("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.countfingerprintChannelSection("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.countfingerprintChannelSectionDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.counttotalvisitorsChannelSection("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
     mod.counttotalvisitorsChannelSectionDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http___womansera_com_entertainment");
 */
 
 //    mod.countAudiencesegmentChannelSection("2017-01-01","2017-01-31","Womanseraindia_indiagate", "http___womansera_com_trending_moles_on_these_areas_signify_wealth_and_luck_read_to_know");
     /*
    
     mod.countOSChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/trending/amitabh-bachchan-dead-pictures-going-viral");
     
     mod.countCityChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/trending/amitabh-bachchan-dead-pictures-going-viral");
     
     mod.countModelChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/trending/amitabh-bachchan-dead-pictures-going-viral");
    
 mod.countOSChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     
     mod.countCityChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     
     mod.countModelChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     
     mod.counttotalvisitorsChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     mod.counttotalvisitorsChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     
     mod.countfingerprintChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     mod.countfingerprintChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     
     mod.countLoyalUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     mod.countReturningUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     mod.countNewUsersChannelArticleDatewise("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
    
     mod.getAgegroupChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
     mod.getGenderChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
    */ 
//     mod.countAudiencesegmentChannelArticle("2017-01-01","2017-01-31","Womanseraindia_indiagate","http://womansera.com/wedding/video-shahid-miras-dance-sajh-dajh-ke-sangeet-ceremony");
    
     Map<String,String>filter = new HashMap<String,String>();
     filter.put("city","delhi,mumbai");
     filter.put("agegroup","35_44");
//     filter.put("incomelevel", "medium");
  //   filter.put("devicetype","tablet");
    // mod.getGenderChannelFilter("2017-01-01","2017-01-31","Womanseraindia_indiagate", filter);
    
     
     List<String> groupby = new ArrayList<String>();
     groupby.add("city");
     groupby.add("agegroup");
   //  groupby.add("incomelevel");
   //  mod.getGenderChannelGroupBy("2017-01-01","2017-01-31","Womanseraindia_indiagate", groupby);
     /*	 
	 final long startTime1 = System.currentTimeMillis();
	 AggregationModule mod = new AggregationModule();
	 mod.countAudienceSegment("2016-08-20","2016-12-02");
	 mod.countAudienceSegment("2016-08-20","2016-12-02");  
	 mod.countAudienceSegment("2016-08-20","2016-12-02");
	 mod.countAudienceSegment("2016-08-20","2016-12-02");  
	 mod.countAudienceSegment("2016-08-20","2016-12-02");
	 mod.countAudienceSegment("2016-08-20","2016-12-02");  
	 mod.countAudienceSegment("2016-08-20","2016-12-02");
	 final long endTime1 = System.currentTimeMillis();
	 
	 
	 System.out.println("Total code execution time: " + (endTime1 - startTime1) );
		
	  */
	  //  countfingerprintChannel("2016-08-20","2016-12-02", "Mumbai_T1_airport");
	  
	//  countAudiencesegmentChannel("2016-08-20","2016-12-02", "Mumbai_T1_airport");
	
	  
	//  Integer countv1 = 500000;
	  
	  /*
	  
	  if(countv1 >= 500000)
	    {
	    double total_length = countv1 - 0;
	    double subrange_length = total_length/30;	
	    
	    double current_start = 0;
	    for (int i = 0; i < 20; ++i) {
	      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
	      current_start += subrange_length;
	    }
	   }
  */
	  
	  /*
	  
	    Double countv1 = Double.parseDouble("90000");
	    
	    Double n = 0.0;
	    if(countv1 >= 250000)
	       n=50.0;
	    
	    if(countv1 >= 100000 && countv1 <= 250000 )
	       n=20.0;
	    
	    if(countv1 < 100000)
           n=50.0;	    
	   
	    Double total_length = countv1 - 0;
	    Double subrange_length = total_length/n;	
	    String startdate= "Startdate";
	    String enddate= "endDate";
	    Double current_start = 0.0;
	    for (int i = 0; i < n; ++i) {
	      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
	      Double startlimit = current_start;
	      Double finallimit = current_start + subrange_length;
	      Double index = startlimit +1;
	      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdatabeta1 where date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit "+index.intValue()+","+finallimit.intValue();  	
		  System.out.println(query);
	  //    Query.add(query);
	      current_start += subrange_length;
	    //  Query.add(query);
	     
	    }
	   */ 
	    
  
  }
  
  public void setUp()
    throws Exception
  {
    if (client == null)
    {
      client = new TransportClient();
      client.addTransportAddress(getTransportAddress());
      
      NodesInfoResponse nodeInfos = (NodesInfoResponse)client.admin().cluster().prepareNodesInfo(new String[0]).get();
      String clusterName = nodeInfos.getClusterName().value();
      //System.out.println(String.format("Found cluster... cluster name: %s", new Object[] { clusterName }));
      
      searchDao = new SearchDao(client);
    }
    //System.out.println("Finished the setup process...");
  }
  
  public static SearchDao getSearchDao()
  {
    return searchDao;
  }
  
  public List<PublisherReport> countBrandName(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,brandName FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by brandName", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if (lines.size() > 0) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if(data[0].trim().toLowerCase().contains("logitech")==false && data[0].trim().toLowerCase().contains("mozilla")==false && data[0].trim().toLowerCase().contains("web_browser")==false && data[0].trim().toLowerCase().contains("microsoft")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false){ 
        obj.setBrandname(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
       }
      }
    }
    //System.out.println(headers);
    //System.out.println(lines);
    
    return pubreport;
  }
  
  public List<PublisherReport> countBrowser(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,browser_name FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by browser_name", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
       
        obj.setBrowser(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
    }
    //System.out.println(headers);
    //System.out.println(lines);
    
    return pubreport;
  }
  
  public List<PublisherReport> countOS(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,system_os FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by system_os", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    System.out.println(headers);
    System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setOs(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countModel(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,modelName FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by modelName", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if(data[0].trim().toLowerCase().contains("logitech_revue")==false && data[0].trim().toLowerCase().contains("mozilla_firefox")==false && data[0].trim().toLowerCase().contains("apple_safari")==false && data[0].trim().toLowerCase().contains("generic_web")==false && data[0].trim().toLowerCase().contains("google_compute")==false && data[0].trim().toLowerCase().contains("microsoft_xbox")==false && data[0].trim().toLowerCase().contains("google_chromecast")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false && data[0].trim().toLowerCase().contains("laptop")==false){    
        obj.setMobile_device_model_name(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
        }
        
        }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countCity(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,city FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by city", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setCity(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countPinCode(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,postalcode FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by postalcode", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    //System.out.println(headers);
    //System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setPostalcode(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countLatLong(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,latitude_longitude FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by latitude_longitude", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    //System.out.println(headers);
    //System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        String[] dashcount = data[0].split("_");
        if ((dashcount.length == 3) && (data[0].charAt(data[0].length() - 1) != '_') && 
          (!dashcount[2].isEmpty()))
        {
          obj.setLatitude_longitude(data[0]);
          obj.setCount(data[1]);
          pubreport.add(obj);
        }
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countfingerprint(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where date between '" + 
      startdate + "'" + " and " + "'" + enddate + "'" + " group by date", new Object[] {"enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setDate(data[0]);
        obj.setReach(data[1]);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countAudienceSegment(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
	
	  PrintStream out = new PrintStream(new FileOutputStream(
				"audiencesegmentcount.txt"));
		System.setOut(out);
	  
	  List<PublisherReport> pubreport = new ArrayList(); 
	  
	  String querya1 = "Select Count(DISTINCT(cookie_id)) FROM enhanceduserdata where date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit 20000000";  
	  
	    //Divide count in different limits 
	
	  
	  List<String> Query = new ArrayList();
	  


	    System.out.println(querya1);
	    
	    final long startTime2 = System.currentTimeMillis();
		
	    
	    CSVResult csvResult1 = null;
		try {
			csvResult1 = AggregationModule.getCsvResult(false, querya1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    final long endTime2 = System.currentTimeMillis();
		
	    List<String> headers = csvResult1.getHeaders();
	    List<String> lines = csvResult1.getLines();
	    
	    
	    String count = lines.get(0);
	    Double countv1 = Double.parseDouble(count);
	    Double n = 0.0;
	    if(countv1 >= 250000)
	       n=10.0;
	    
	    if(countv1 >= 100000 && countv1 <= 250000 )
	       n=10.0;
	    
	    if(countv1 < 100000)
           n=10.0;	    
	   
	    
	    if(countv1 <= 100)
	    	n=1.0;
	    
	    if(countv1 == 0)
	    {
	    	
	    	return pubreport;
	    	
	    }
	    
	    Double total_length = countv1 - 0;
	    Double subrange_length = total_length/n;	
	    
	    Double current_start = 0.0;
	    for (int i = 0; i < n; ++i) {
	      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
	      Double startlimit = current_start;
	      Double finallimit = current_start + subrange_length;
	      Double index = startlimit +1;
	      if(countv1 == 1)
	    	  index=0.0;
	      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdata where date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' Order by cookie_id limit "+index.intValue()+","+finallimit.intValue();  	
		  System.out.println(query);
	  //    Query.add(query);
	      current_start += subrange_length;
	      Query.add(query);
	     
	    }
	    
	    
	    	
	    
	  
	  ExecutorService executorService = Executors.newFixedThreadPool(2000);
        
       List<Callable<FastMap<String,Double>>> lst = new ArrayList<Callable<FastMap<String,Double>>>();
    
       for(int i=0 ; i < Query.size(); i++ ){
       lst.add(new AudienceSegmentQueryExecutionThreads(Query.get(i),client,searchDao));
    /*   lst.add(new AudienceSegmentQueryExecutionThreads(query1,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query2,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query3,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query4,client,searchDao));*/
        
       // returns a list of Futures holding their status and results when all complete
       lst.add(new SubcategoryQueryExecutionThreads(Query.get(i),client,searchDao));
   /*    lst.add(new SubcategoryQueryExecutionThreads(query6,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query7,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query8,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query9,client,searchDao)); */
       }
       
       
       List<Future<FastMap<String,Double>>> maps = executorService.invokeAll(lst);
        
       System.out.println(maps.size() +" Responses recieved.\n");
        
       for(Future<FastMap<String,Double>> task : maps)
       {
    	   try{
           if(task!=null)
    	   System.out.println(task.get().toString());
    	   }
    	   catch(Exception e)
    	   {
    		   e.printStackTrace();
    		   continue;
    	   }
    	    
    	   
    	   }
        
       /* shutdown your thread pool, else your application will keep running */
       executorService.shutdown();
	  
	
	  //  //System.out.println(headers1);
	 //   //System.out.println(lines1);
	    
	    
       
       FastMap<String,Double> audiencemap = new FastMap<String,Double>();
       
       FastMap<String,Double> subcatmap = new FastMap<String,Double>();
       
       Double count1 = 0.0;
       
       Double count2 = 0.0;
       
       String key ="";
       String key1 = "";
       Double value = 0.0;
       Double vlaue1 = 0.0;
       
	    for (int i = 0; i < maps.size(); i++)
	    {
	    
	    	if(maps!=null && maps.get(i)!=null){
	        FastMap<String,Double> map = (FastMap<String, Double>) maps.get(i).get();
	    	
	       if(map.size() > 0){
	       
	       if(map.containsKey("audience_segment")==true){
	       for (Map.Entry<String, Double> entry : map.entrySet())
	    	 {
	    	  key = entry.getKey();
	    	  key = key.trim();
	    	  value=  entry.getValue();
	    	if(key.equals("audience_segment")==false) { 
	    	if(audiencemap.containsKey(key)==false)
	    	audiencemap.put(key,value);
	    	else
	    	{
	         count1 = audiencemap.get(key);
	         if(count1!=null)
	         audiencemap.put(key,count1+value);	
	    	}
	      }
	    }
	  }   

	       if(map.containsKey("subcategory")==true){
	       for (Map.Entry<String, Double> entry : map.entrySet())
	    	 {
	    	   key = entry.getKey();
	    	   key = key.trim();
	    	   value=  entry.getValue();
	    	if(key.equals("subcategory")==false) {    
	    	if(subcatmap.containsKey(key)==false)
	    	subcatmap.put(key,value);
	    	else
	    	{
	         count1 = subcatmap.get(key);
	         if(count1!=null)
	         subcatmap.put(key,count1+value);	
	    	}
	    }  
	    	
	   }
	      
	     	       }
	           
	       } 
	    
	    	} 	
	   }    
	    
	    String subcategory = null;
	   
	    if(audiencemap.size()>0){
	   
	    	for (Map.Entry<String, Double> entry : audiencemap.entrySet()) {
	    	//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
	    

	        PublisherReport obj = new PublisherReport();
	        
	   //     String[] data = ((String)lines.get(i)).split(",");
	        
	     //   if(data[0].trim().toLowerCase().contains("festivals"))
	      //  obj.setAudience_segment("");
	      //  else
	        obj.setAudience_segment( entry.getKey());	
	        obj.setCount(String.valueOf(entry.getValue()));
	      
	        if ((!entry.getKey().equals("tech")) && (!entry.getKey().equals("india")) && (!entry.getKey().trim().toLowerCase().equals("foodbeverage")) )
	        {
	         for (Map.Entry<String, Double> entry1 : subcatmap.entrySet()) {
	        	 
	        	    
	        	 
	        	 PublisherReport obj1 = new PublisherReport();
	            
	           
	            if (entry1.getKey().contains(entry.getKey()))
	            {
	              String substring = "_" + entry.getKey() + "_";
	              subcategory = entry1.getKey().replace(substring, "");
	           //   if(data[0].trim().toLowerCase().contains("festivals"))
	           //   obj1.setAudience_segment("");
	           //   else
	        
	              //System.out.println(" \n\n\n Key : " + subcategory + " Value : " + entry1.getValue());  
	              obj1.setAudience_segment(subcategory);
	              obj1.setCount(String.valueOf(entry1.getValue()));
	              obj.getAudience_segment_data().add(obj1);
	            }
	          }
	          pubreport.add(obj);
	        }
	      
	    }
	    }
	    return pubreport;
 
   
  }
  
  public List<PublisherReport> countISP(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,ISP FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by ISP", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if(data[0].trim().toLowerCase().equals("_ltd")==false){ 
        obj.setISP(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
       }
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countOrg(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT organisation FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " and organisation NOT IN (Select DISTINCT(ISP) FROM enhanceduserdatabeta1)", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setOrganisation(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public Set<String> getChannelList(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    String query = String.format("SELECT channel_name FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " Group by channel_name", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<String> finallines = new ArrayList();
    Set<String> data = new HashSet();
    data.addAll(lines);
    
    //System.out.println(headers);
    //System.out.println(lines);
    
    return data;
  }
  
  public List<PublisherReport> gettimeofdayQuarter(String startdate, String enddate)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 WHERE date between '" + startdate + "'" + " and " + "'" + enddate + "' GROUP BY HOUR(request_time)";
    
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> gettimeofdayDaily(String startdate, String enddate)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 WHERE date between '" + startdate + "'" + " and " + "'" + enddate + "' GROUP BY date_histogram(field='request_time','interval'='1d')";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> gettimeofday(String startdate, String enddate)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 WHERE date between '" + startdate + "'" + " and " + "'" + enddate + "' GROUP BY date_histogram(field='request_time','interval'='1h')";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countGender(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,gender FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by gender", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    
    //System.out.println(headers);
    //System.out.println(lines);
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setGender(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countAgegroup(String startdate, String enddate)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,agegroup FROM enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " group by agegroup", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    
    //System.out.println(headers);
    //System.out.println(lines);
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setAge(data[0]);
        obj.setCount(data[1]);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> getOrg(String startdate, String enddate)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query1 = "Select count(*),organisation from enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY organisation";
    CSVResult csvResult1 = getCsvResult(false, query1);
    List<String> headers1 = csvResult1.getHeaders();
    List<String> lines1 = csvResult1.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines1.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data1 = ((String)lines1.get(i)).split(",");
        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].contains("broadband")) && (!data1[0].contains("communication")) && (!data1[0].contains("cable")) && (!data1[0].contains("telecom")) && (!data1[0].contains("network")) && (!data1[0].contains("isp")) && (!data1[0].contains("hathway")) && (!data1[0].contains("internet")) && (!data1[0].contains("Sify")) && (!data1[0].toLowerCase().equals("_ltd")) && (!data1[0].equals("Googlebot")) && (!data1[0].equals("Bsnl")))
        {
          obj.setOrganisation(data1[0]);
          obj.setCount(data1[1]);
          
          pubreport.add(obj);
        }
      }
      //System.out.println(headers1);
      //System.out.println(lines1);
    }
    return pubreport;
  }
  
  public List<PublisherReport> getdayQuarterdata(String startdate, String enddate)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*),QuarterValue from enhanceduserdatabeta1 where date between '" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY QuarterValue";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if (data[0].equals("quarter1")) {
          data[0] = "quarter1 (00 - 04 AM)";
        }
        if (data[0].equals("quarter2")) {
          data[0] = "quarter2 (04 - 08 AM)";
        }
        if (data[0].equals("quarter3")) {
          data[0] = "quarter3 (08 - 12 AM)";
        }
        if (data[0].equals("quarter4")) {
          data[0] = "quarter4 (12 - 16 PM)";
        }
        if (data[0].equals("quarter5")) {
          data[0] = "quarter5 (16 - 20 PM)";
        }
        if (data[0].equals("quarter6")) {
          data[0] = "quarter6 (20 - 24 PM)";
        }
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countBrandNameChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    String query = "SELECT COUNT(*)as count,brandName FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by brandName";
    //System.out.println(query);
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if(data[0].trim().toLowerCase().contains("logitech")==false && data[0].trim().toLowerCase().contains("mozilla")==false && data[0].trim().toLowerCase().contains("web_browser")==false && data[0].trim().toLowerCase().contains("microsoft")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false){ 
        obj.setBrandname(data[0]);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
        } 
       }
  //    //System.out.println(headers);
  //    //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countBrowserChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = "SELECT COUNT(*)as count,browser_name FROM enhanceduserdatabeta1 where channel_name ='" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by browser_name";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setBrowser(data[0]);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countOSChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,system_os FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by system_os", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
  //  //System.out.println(headers);
  //  //System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setOs(data[0]);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countModelChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,modelName FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by modelName", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");

        if(data[0].trim().toLowerCase().contains("logitech_revue")==false && data[0].trim().toLowerCase().contains("mozilla_firefox")==false && data[0].trim().toLowerCase().contains("apple_safari")==false && data[0].trim().toLowerCase().contains("generic_web")==false && data[0].trim().toLowerCase().contains("google_compute")==false && data[0].trim().toLowerCase().contains("microsoft_xbox")==false && data[0].trim().toLowerCase().contains("google_chromecast")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false && data[0].trim().toLowerCase().contains("laptop")==false){    
        obj.setMobile_device_model_name(data[0]);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
      }
        
        }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countCityChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,city FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by city", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setCity(data[0]);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countfingerprintChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
	  
	  
	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
	  
    
	  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
		      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
	  
		 CSVResult csvResult00 = getCsvResult(false, query00);
		 List<String> headers00 = csvResult00.getHeaders();
		 List<String> lines00 = csvResult00.getLines();
		 List<PublisherReport> pubreport00 = new ArrayList();  
			  
		//  //System.out.println(headers00);
		//  //System.out.println(lines00);  
		  
		  for (int i = 0; i < lines00.size(); i++)
	      {
	       
	        String[] data = ((String)lines00.get(i)).split(",");
	  //      //System.out.println(data[0]);
	      }
		  
		  
		  
		Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
	    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where channel_name = '" + 
	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
	    CSVResult csvResult = getCsvResult(false, query);
	    List<String> headers = csvResult.getHeaders();
	    List<String> lines = csvResult.getLines();
	    List<PublisherReport> pubreport = new ArrayList();
	    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
	      for (int i = 0; i < lines.size(); i++)
	      {
	        PublisherReport obj = new PublisherReport();
	        
	        String[] data = ((String)lines.get(i)).split(",");
	        obj.setDate(data[0]);
	        obj.setReach(data[1]);
	        obj.setChannelName(channel_name);
	        pubreport.add(obj);
	      }
	    }
	    
    return pubreport;
  }
  
  public List<PublisherReport> countAudiencesegmentChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
      List<PublisherReport> pubreport = new ArrayList(); 
	  
	  String querya1 = "SELECT COUNT(DISTINCT(cookie_id)) FROM enhanceduserdata where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit 20000000";   
	  
	    //Divide count in different limits 
	
	  
	  List<String> Query = new ArrayList();
	  


	    System.out.println(querya1);
	    
	    final long startTime2 = System.currentTimeMillis();
		
	    
	    CSVResult csvResult1 = null;
		try {
			csvResult1 = AggregationModule.getCsvResult(false, querya1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    final long endTime2 = System.currentTimeMillis();
		
	    List<String> headers = csvResult1.getHeaders();
	    List<String> lines = csvResult1.getLines();
	    
	    
	    String count = lines.get(0);
	    Double countv1 = Double.parseDouble(count);
	    Double n = 0.0;
	    if(countv1 >= 250000)
	       n=10.0;
	    
	    if(countv1 >= 100000 && countv1 <= 250000 )
	       n=10.0;
	    
	    if(countv1 <= 100000 && countv1 > 100)
           n=10.0;	    
	   
	    if(countv1 <= 100)
	    	n=1.0;
	    
	    if(countv1 == 0)
	    {
	    	
	    	return pubreport;
	    	
	    }
	    
	    Double total_length = countv1 - 0;
	    Double subrange_length = total_length/n;	
	    
	    Double current_start = 0.0;
	    for (int i = 0; i < n; ++i) {
	      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
	      Double startlimit = current_start;
	      Double finallimit = current_start + subrange_length;
	      Double index = startlimit +1;
	      if(countv1 == 1)
	    	  index=0.0;
	      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdata where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "' Order by cookie_id limit "+index.intValue()+","+finallimit.intValue();  	
		  System.out.println(query);
	  //    Query.add(query);
	      current_start += subrange_length;
	      Query.add(query);
	     
	    }
	    
	    
	    	
	    
	  
	  ExecutorService executorService = Executors.newFixedThreadPool(2000);
        
       List<Callable<FastMap<String,Double>>> lst = new ArrayList<Callable<FastMap<String,Double>>>();
    
       for(int i=0 ; i < Query.size(); i++ ){
       lst.add(new AudienceSegmentQueryExecutionThreads(Query.get(i),client,searchDao));
    /*   lst.add(new AudienceSegmentQueryExecutionThreads(query1,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query2,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query3,client,searchDao));
       lst.add(new AudienceSegmentQueryExecutionThreads(query4,client,searchDao));*/
        
       // returns a list of Futures holding their status and results when all complete
       lst.add(new SubcategoryQueryExecutionThreads(Query.get(i),client,searchDao));
   /*    lst.add(new SubcategoryQueryExecutionThreads(query6,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query7,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query8,client,searchDao));
       lst.add(new SubcategoryQueryExecutionThreads(query9,client,searchDao)); */
       }
       
       
       List<Future<FastMap<String,Double>>> maps = executorService.invokeAll(lst);
        
       System.out.println(maps.size() +" Responses recieved.\n");
        
       for(Future<FastMap<String,Double>> task : maps)
       {
    	   try{
           if(task!=null)
    	   System.out.println(task.get().toString());
    	   }
    	   catch(Exception e)
    	   {
    		   e.printStackTrace();
    		   continue;
    	   }
    	    
    	   
    	   }
        
       /* shutdown your thread pool, else your application will keep running */
       executorService.shutdown();
	  
	
	  //  //System.out.println(headers1);
	 //   //System.out.println(lines1);
	    
	    
       
       FastMap<String,Double> audiencemap = new FastMap<String,Double>();
       
       FastMap<String,Double> subcatmap = new FastMap<String,Double>();
       
       Double count1 = 0.0;
       
       Double count2 = 0.0;
       
       String key ="";
       String key1 = "";
       Double value = 0.0;
       Double vlaue1 = 0.0;
       
	    for (int i = 0; i < maps.size(); i++)
	    {
	    
	    	if(maps!=null && maps.get(i)!=null){
	        FastMap<String,Double> map = (FastMap<String, Double>) maps.get(i).get();
	    	
	       if(map.size() > 0){
	       
	       if(map.containsKey("audience_segment")==true){
	       for (Map.Entry<String, Double> entry : map.entrySet())
	    	 {
	    	  key = entry.getKey();
	    	  key = key.trim();
	    	  value=  entry.getValue();
	    	if(key.equals("audience_segment")==false) { 
	    	if(audiencemap.containsKey(key)==false)
	    	audiencemap.put(key,value);
	    	else
	    	{
	         count1 = audiencemap.get(key);
	         if(count1!=null)
	         audiencemap.put(key,count1+value);	
	    	}
	      }
	    }
	  }   

	       if(map.containsKey("subcategory")==true){
	       for (Map.Entry<String, Double> entry : map.entrySet())
	    	 {
	    	   key = entry.getKey();
	    	   key = key.trim();
	    	   value=  entry.getValue();
	    	if(key.equals("subcategory")==false) {    
	    	if(subcatmap.containsKey(key)==false)
	    	subcatmap.put(key,value);
	    	else
	    	{
	         count1 = subcatmap.get(key);
	         if(count1!=null)
	         subcatmap.put(key,count1+value);	
	    	}
	    }  
	    	
	   }
	      
	     	       }
	           
	       } 
	    
	    	} 	
	   }    
	    
	    String subcategory = null;
	   
	    if(audiencemap.size()>0){
	   
	    	for (Map.Entry<String, Double> entry : audiencemap.entrySet()) {
	    	//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
	    

	        PublisherReport obj = new PublisherReport();
	        
	   //     String[] data = ((String)lines.get(i)).split(",");
	        
	     //   if(data[0].trim().toLowerCase().contains("festivals"))
	      //  obj.setAudience_segment("");
	      //  else
	        obj.setAudience_segment( entry.getKey());	
	        obj.setCount(String.valueOf(entry.getValue()));
	      
	        if ((!entry.getKey().equals("tech")) && (!entry.getKey().equals("india")) && (!entry.getKey().trim().toLowerCase().equals("foodbeverage")) )
	        {
	         for (Map.Entry<String, Double> entry1 : subcatmap.entrySet()) {
	        	 
	        	    
	        	 
	        	 PublisherReport obj1 = new PublisherReport();
	            
	           
	            if (entry1.getKey().contains(entry.getKey()))
	            {
	              String substring = "_" + entry.getKey() + "_";
	              subcategory = entry1.getKey().replace(substring, "");
	           //   if(data[0].trim().toLowerCase().contains("festivals"))
	           //   obj1.setAudience_segment("");
	           //   else
	        
	              //System.out.println(" \n\n\n Key : " + subcategory + " Value : " + entry1.getValue());  
	              obj1.setAudience_segment(subcategory);
	              obj1.setCount(String.valueOf(entry1.getValue()));
	              obj.getAudience_segment_data().add(obj1);
	            }
	          }
	          pubreport.add(obj);
	        }
	      
	    }
	    }
	    return pubreport;
  }
  
  public List<PublisherReport> gettimeofdayChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> countPinCodeChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,postalcode FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by postalcode", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    //System.out.println(headers);
    //System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setPostalcode(data[0]);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> countLatLongChannel(String startdate, String enddate, String channel_name)
    throws CsvExtractorException, Exception
  {
    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
    String query = String.format("SELECT COUNT(*)as count,latitude_longitude FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by latitude_longitude", new Object[] { "enhanceduserdatabeta1" });
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    //System.out.println(headers);
    //System.out.println(lines);
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        String[] dashcount = data[0].split("_");
        if ((dashcount.length == 3) && (data[0].charAt(data[0].length() - 1) != '_'))
        {
          if (!dashcount[2].isEmpty())
          {
            obj.setLatitude_longitude(data[0]);
            obj.setCount(data[1]);
            obj.setChannelName(channel_name);
          }
          pubreport.add(obj);
        }
      }
    }
    return pubreport;
  }
  
  public List<PublisherReport> gettimeofdayQuarterChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='4h')";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> gettimeofdayDailyChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1d')";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> getdayQuarterdataChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*),QuarterValue from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY QuarterValue";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if (data[0].equals("quarter1")) {
          data[0] = "quarter1 (00 - 04 AM)";
        }
        if (data[0].equals("quarter2")) {
          data[0] = "quarter2 (04 - 08 AM)";
        }
        if (data[0].equals("quarter3")) {
          data[0] = "quarter3 (08 - 12 AM)";
        }
        if (data[0].equals("quarter4")) {
          data[0] = "quarter4 (12 - 16 PM)";
        }
        if (data[0].equals("quarter5")) {
          data[0] = "quarter5 (16 - 20 PM)";
        }
        if (data[0].equals("quarter6")) {
          data[0] = "quarter6 (20 - 24 PM)";
        }
        obj.setTime_of_day(data[0]);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> getQueryFieldChannel(String queryfield,String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    
    //System.out.println(headers);
    //System.out.println(lines);
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
     //   String demographicproperties = demographicmap.get(data[0]);
            
            
        
            if(queryfield.equals("gender"))
        	obj.setGender(data[0]);
        
            if(queryfield.equals("device"))
        	obj.setDevice_type(data[0]);
        	
        	if(queryfield.equals("city")){
        		try{
        		String locationproperties = citycodeMap.get(data[0]);
		        data[0]=data[0].replace("_"," ").replace("-"," ");
		        obj.setCity(data[0]);
		        System.out.println(data[0]);
		        obj.setLocationcode(locationproperties);
        		}
        		catch(Exception e){
        			continue;
        		}
        		
        		} 
        	if(queryfield.equals("audience_segment"))
             obj.setAudience_segment(data[0]);
        	
        	if(queryfield.equals("reforiginal"))
	             obj.setReferrerSource(data[0]);
            	
        	if(queryfield.equals("agegroup"))
	             obj.setAge(data[0]);
            	
        	if(queryfield.equals("incomelevel"))
	          obj.setIncomelevel(data[0]);
        
         	
        	if(queryfield.equals("system_os")){
        		String osproperties = oscodeMap.get(data[0]);
		        data[0]=data[0].replace("_"," ").replace("-", " ");
		        obj.setOs(data[0]);
		        obj.setOscode(osproperties);
        	}
         	
        	if(queryfield.equals("modelName")){
        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
	        	
		        obj.setMobile_device_model_name(mobiledeviceproperties[1]);
		        System.out.println(mobiledeviceproperties[1]);
		        obj.setDevicecode(mobiledeviceproperties[0]);
		        System.out.println(mobiledeviceproperties[0]);
        	}
         	
        	if(queryfield.equals("brandName"))
	          obj.setBrandname(data[0]);
        
        	if(queryfield.equals("refcurrentoriginal"))
  	          obj.setPublisher_pages(data[0]);
        	
        	
     //   obj.setGender(data[0]);
     //   obj.setCode(code);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  
  
  public List<PublisherReport> getQueryFieldChannelFilter(String queryfield,String startdate, String enddate, String channel_name, Map<String,String>filter)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	        int size = filter.size();
	        String queryfilterbuilder = "";
	        String formattedString = "";
	        int j =0;
	        for (Map.Entry<String, String> entry : filter.entrySet())
	        {
	        	if (j==0){
	                formattedString = addCommaString(entry.getValue());
	        		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
	        	
	        	}
	            else{
	            formattedString = addCommaString(entry.getValue());	
	            queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
	       
	            }
	            j++;
	         
	        }
	  
	  
	        
	        String query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
		    System.out.println(query);
	        CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		    //System.out.println(headers);
		    //System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		        	if(queryfield.equals("city")){
		        		try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e)
		        		{
		        			continue;
		        		}
		        		
		        		}
		        	if(queryfield.equals("audience_segment"))
		             obj.setAudience_segment(data[0]);
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
			             obj.setAge(data[0]);
		            	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		     
		        	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        obj.setOs(data[0]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
		        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
		        	
			        obj.setMobile_device_model_name(mobiledeviceproperties[1]);
			        System.out.println(mobiledeviceproperties[1]);
			        obj.setDevicecode(mobiledeviceproperties[0]);
			        System.out.println(mobiledeviceproperties[0]);
		        	}
		         	
		        	if(queryfield.equals("brandName"))
			          obj.setBrandname(data[0]);
		        

		        	if(queryfield.equals("refcurrentoriginal"))
		  	          obj.setPublisher_pages(data[0]);
		        	
		        	
		        	
		        	
		        	//   obj.setCode(code);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        pubreport.add(obj);
		      }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    return pubreport;
		  }
  

  public static String convert(List<String> list) {
	    String res = "";
	    for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
	        res += iterator.next() + (iterator.hasNext() ? "," : "");
	    }
	    return res;
	}
  
  public static String addCommaString(String value) {
	    String res = "";
	    String [] parts = value.split(",");
	    for(int i =0; i<parts.length; i++){
	    	
	    	res = res+"'"+parts[i]+"'"+",";
	    	
	    }
        res = res.substring(0,res.length()-1);
       return res;
  }
  

  public List<PublisherReport> getQueryFieldChannelGroupBy(String queryfield,String startdate, String enddate, String channel_name, List<String> groupby)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    
	      
            String querygroupbybuilder = convert(groupby);
            
            String query = "";
            
            int  l=0;
            
         	query = "Select count(*),"+queryfield+","+querygroupbybuilder+" from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder;
		    System.out.println(query);
         	CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		    //System.out.println(headers);
		    //System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		        
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		        	if(queryfield.equals("city")){
		        		try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e){
		        			
		        			continue;
		        		}
		        		
		        		}
		        	if(queryfield.equals("audience_segment"))
		             obj.setAudience_segment(data[0]);
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
			             obj.setAge(data[0]);
		            	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		     
		        	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        obj.setOs(data[0]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
			          obj.setMobile_device_model_name(data[0]);
			          String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[1]);
				        System.out.println(mobiledeviceproperties[1]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]);
		        	}
		        	
		        	
		        	if(queryfield.equals("brandName"))
			          obj.setBrandname(data[0]);
		        
		        	

		        	if(queryfield.equals("refcurrentoriginal"))
		  	          obj.setPublisher_pages(data[0]);
		        	
		        	
		        	
		        	//   obj.setCode(code);
	            for(int k = 0; k < groupby.size(); k++)
	            {
	            	
	            	if(groupby.get(k).equals(queryfield)==false)
	            	{
	                try{
	            	if(groupby.get(k).equals("device"))
	            	obj.setDevice_type(data[k+1]);
	            	
	            	if(groupby.get(k).equals("city")){
	            		try{
	            		String locationproperties = citycodeMap.get(data[k+1]);
	    		        data[k+1]=data[k+1].replace("_"," ").replace("-"," ");
	    		        obj.setCity(data[k+1]);
	    		        System.out.println(data[k+1]);
	    		        obj.setLocationcode(locationproperties);
	            		}
	            		catch(Exception e)
	            		{
	            			continue;
	            		}
	            	}
	            	if(groupby.get(k).equals("audience_segment"))
		             obj.setAudience_segment(data[k+1]);
	            	
	            	
	            	if(groupby.get(k).equals("gender"))
			             obj.setGender(data[k+1]);
	            	
	            	
	            	if(groupby.get(k).equals("referrer"))
			             obj.setGender(data[k+1]);
		            	
	            	
	            	if(groupby.get(k).equals("subcategory"))
			             obj.setSubcategory(data[k+1]);
	            	
	            	if(groupby.get(k).equals("agegroup"))
			             obj.setAge(data[k+1]);
		            	
	            	if(groupby.get(k).equals("incomelevel"))
			          obj.setIncomelevel(data[k+1]);
		            	
                    l++;
	                }
	                catch(Exception e){
	                	continue;
	                }
	                
	                }
	            }
	           
	            if(l!=0)
		        obj.setCount(data[l+1]);
		        obj.setChannelName(channel_name);
		        pubreport.add(obj);
		        l=0;
		      }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    return pubreport;
		  }
  
  
  
  public List<PublisherReport> getQueryFieldChannelArticle(String queryfield,String startdate, String enddate, String channel_name,String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		    //System.out.println(headers);
		    //System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		     //   String demographicproperties = demographicmap.get(data[0]);
		            
		            if(queryfield.equals("gender"))
		        	obj.setGender(data[0]);
		        
		            if(queryfield.equals("device"))
		        	obj.setDevice_type(data[0]);
		        	
		        	if(queryfield.equals("city")){
		        		try{
		        		String locationproperties = citycodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-"," ");
				        obj.setCity(data[0]);
				        System.out.println(data[0]);
				        obj.setLocationcode(locationproperties);
		        		}
		        		catch(Exception e)
		        		{
		        			continue;
		        		}
		        		}
		        	if(queryfield.equals("audience_segment"))
		             obj.setAudience_segment(data[0]);
		        	
		        	if(queryfield.equals("reforiginal"))
			             obj.setReferrerSource(data[0]);
		            	
		        	if(queryfield.equals("agegroup"))
			             obj.setAge(data[0]);
		            	
		        	if(queryfield.equals("incomelevel"))
			          obj.setIncomelevel(data[0]);
		        
		        	
		        	if(queryfield.equals("system_os")){
		        		String osproperties = oscodeMap.get(data[0]);
				        data[0]=data[0].replace("_"," ").replace("-", " ");
				        obj.setOs(data[0]);
				        obj.setOscode(osproperties);
		        	}
		         	
		        	if(queryfield.equals("modelName")){
		        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
			        	
				        obj.setMobile_device_model_name(mobiledeviceproperties[1]);
				        System.out.println(mobiledeviceproperties[1]);
				        obj.setDevicecode(mobiledeviceproperties[0]);
				        System.out.println(mobiledeviceproperties[0]);
		        
		        	}
		        	if(queryfield.equals("brandName"))
			          obj.setBrandname(data[0]);
		        
		        	

		        	if(queryfield.equals("refcurrentoriginal"))
		  	          obj.setPublisher_pages(data[0]);
		        	
		        	
		     //   obj.setGender(data[0]);
		     //   obj.setCode(code);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  
		  
		  public List<PublisherReport> getQueryFieldChannelArticleFilter(String queryfield,String startdate, String enddate, String channel_name,String articlename, Map<String,String>filter)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    
			        int size = filter.size();
			        String queryfilterbuilder = "";
			        String formattedString = "";
			        int j =0;
			        for (Map.Entry<String, String> entry : filter.entrySet())
			        {
			        	if (j==0){
			                formattedString = addCommaString(entry.getValue());
			        		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
			        	
			        	}
			            else{
			            formattedString = addCommaString(entry.getValue());	
			            queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
			       
			            }
			            j++;
			         
			        }
			  
			  
			        
			        String query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
				    System.out.println(query);
			        CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    
				    //System.out.println(headers);
				    //System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				     //   String demographicproperties = demographicmap.get(data[0]);
				            if(queryfield.equals("gender"))
				        	obj.setGender(data[0]);
				        
				            if(queryfield.equals("device"))
				        	obj.setDevice_type(data[0]);
				        	
				        	if(queryfield.equals("city")){
				        		try{
				        		String locationproperties = citycodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-"," ");
						        obj.setCity(data[0]);
						        System.out.println(data[0]);
						        obj.setLocationcode(locationproperties);
				        		}
				        		catch(Exception e){
				        			continue;
				        		}
				        		
				        		}
				        	if(queryfield.equals("audience_segment"))
				             obj.setAudience_segment(data[0]);
				        	
				        	if(queryfield.equals("reforiginal"))
					             obj.setReferrerSource(data[0]);
				            	
				        	if(queryfield.equals("agegroup"))
					             obj.setAge(data[0]);
				            	
				        	if(queryfield.equals("incomelevel"))
					          obj.setIncomelevel(data[0]);
				    
				        	
				        	if(queryfield.equals("system_os")){
				        		String osproperties = oscodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-", " ");
						        obj.setOs(data[0]);
						        obj.setOscode(osproperties);
				        	}
				         	
				        	if(queryfield.equals("modelName")){
				        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
					        	
						        obj.setMobile_device_model_name(mobiledeviceproperties[1]);
						        System.out.println(mobiledeviceproperties[1]);
						        obj.setDevicecode(mobiledeviceproperties[0]);
						        System.out.println(mobiledeviceproperties[0]);
				        
				        	}
				        	if(queryfield.equals("brandName"))
					          obj.setBrandname(data[0]);
				        
				        
				        	if(queryfield.equals("refcurrentoriginal"))
				  	          obj.setPublisher_pages(data[0]);
				        	
				        	
				        	
				        	
				        	//   obj.setCode(code);
				        
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setArticle(articlename);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
		  

	
		  

		  public List<PublisherReport> getQueryFieldChannelArticleGroupBy(String queryfield,String startdate, String enddate, String channel_name, String articlename, List<String> groupby)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    
			      
		            String querygroupbybuilder = convert(groupby);
		            
		            String query = "";
		            
		            int  l=0;
		            
		         	query = "Select count(*),"+queryfield+","+querygroupbybuilder+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder;
				    System.out.println(query);
		         	CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    
				    //System.out.println(headers);
				    //System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				     //   String demographicproperties = demographicmap.get(data[0]);
				        
				            if(queryfield.equals("gender"))
				        	obj.setGender(data[0]);
				        
				            if(queryfield.equals("device"))
				        	obj.setDevice_type(data[0]);
				        	
				        	if(queryfield.equals("city")){
				        		try{
				        		String locationproperties = citycodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-"," ");
						        obj.setCity(data[0]);
						        System.out.println(data[0]);
						        obj.setLocationcode(locationproperties);
				        		}
				        		catch(Exception e)
				        		{
				        			continue;
				        			
				        		}
				        		
				        		}
				        	if(queryfield.equals("audience_segment"))
				             obj.setAudience_segment(data[0]);
				        	
				        	if(queryfield.equals("reforiginal"))
					             obj.setReferrerSource(data[0]);
				            	
				        	if(queryfield.equals("agegroup"))
					             obj.setAge(data[0]);
				            	
				        	if(queryfield.equals("incomelevel"))
					          obj.setIncomelevel(data[0]);
				    
				        	
				        	
				        	if(queryfield.equals("system_os")){
				        		String osproperties = oscodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-", " ");
						        obj.setOs(data[0]);
						        obj.setOscode(osproperties);
				        	}
				         	
				        	if(queryfield.equals("modelName")){
				        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
					        	
						        obj.setMobile_device_model_name(mobiledeviceproperties[1]);
						        System.out.println(mobiledeviceproperties[1]);
						        obj.setDevicecode(mobiledeviceproperties[0]);
						        System.out.println(mobiledeviceproperties[0]);
				        	}
				         	
				        	if(queryfield.equals("brandName"))
					          obj.setBrandname(data[0]);
				        
				        	
				        	if(queryfield.equals("refcurrentoriginal"))
				  	          obj.setPublisher_pages(data[0]);
				        	
				        	
				        	//   obj.setCode(code);
			            for(int k = 0; k < groupby.size(); k++)
			            {
			            	
			            	if(groupby.get(k).equals(queryfield)==false)
			            	{
			            	try{
			            	if(groupby.get(k).equals("device"))
			            	obj.setDevice_type(data[k+1]);
			            	
			            	if(groupby.get(k).equals("city"))
				            {
			            		try{
			            		String locationproperties = citycodeMap.get(data[k+1]);
			    		        data[k+1]=data[k+1].replace("_"," ").replace("-"," ");
			    		        obj.setCity(data[k+1]);
			    		        System.out.println(data[k+1]);
			    		        obj.setLocationcode(locationproperties);
			            		}
			            		catch(Exception e)
			            		{
			            			continue;
			            		}
			            		}
			            	
			            	if(groupby.get(k).equals("audience_segment"))
				             obj.setAudience_segment(data[k+1]);
			            	
			            	
			            	if(groupby.get(k).equals("gender"))
					             obj.setGender(data[k+1]);
			            	
			            	if(groupby.get(k).equals("subcategory"))
					             obj.setSubcategory(data[k+1]);
			            	
			            	
			            	if(groupby.get(k).equals("referrer"))
					             obj.setGender(data[k+1]);
				            	
			            	if(groupby.get(k).equals("agegroup"))
					             obj.setAge(data[k+1]);
				            	
			            	if(groupby.get(k).equals("incomelevel"))
					          obj.setIncomelevel(data[k+1]);
				            	
		                    l++;
			            	}
			            	catch(Exception e){
			            		continue;
			            	}
			            	
			            	}
			            }
				        
			            if(l!=0)
				        obj.setCount(data[l+1]);
				        obj.setChannelName(channel_name);
				        obj.setArticle(articlename);
				        pubreport.add(obj);
				        l=0;
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
		  
		  public List<PublisherReport> getQueryFieldChannelSection(String queryfield,String startdate, String enddate, String channel_name,String sectionid)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionid+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    
				    //System.out.println(headers);
				    //System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				     //   String demographicproperties = demographicmap.get(data[0]);
				            
				            if(queryfield.equals("gender"))
				        	obj.setGender(data[0]);
				        
				            if(queryfield.equals("device"))
				        	obj.setDevice_type(data[0]);
				        	
				        	if(queryfield.equals("city")){
				        		try{
				        		String locationproperties = citycodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-"," ");
						        obj.setCity(data[0]);
						        System.out.println(data[0]);
						        obj.setLocationcode(locationproperties);
				        		}
				        		catch(Exception e)
				        		{
				        			continue;
				        		}
				        		
				        		}
				        	if(queryfield.equals("audience_segment"))
				             obj.setAudience_segment(data[0]);
				        	
				        	if(queryfield.equals("reforiginal"))
					             obj.setReferrerSource(data[0]);
				            	
				        	if(queryfield.equals("agegroup"))
					             obj.setAge(data[0]);
				            	
				        	if(queryfield.equals("incomelevel"))
					          obj.setIncomelevel(data[0]);
				        
				        	
				        	if(queryfield.equals("system_os")){
				        		String osproperties = oscodeMap.get(data[0]);
						        data[0]=data[0].replace("_"," ").replace("-", " ");
						        obj.setOs(data[0]);
						        obj.setOscode(osproperties);
				        	}
				         	
				        	if(queryfield.equals("modelName")){
				        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
					        	
						        obj.setMobile_device_model_name(mobiledeviceproperties[1]);
						        System.out.println(mobiledeviceproperties[1]);
						        obj.setDevicecode(mobiledeviceproperties[0]);
						        System.out.println(mobiledeviceproperties[0]);
				        
				        	}
				        	
				        	if(queryfield.equals("brandName"))
					          obj.setBrandname(data[0]);
				        

				        	if(queryfield.equals("refcurrentoriginal"))
				  	          obj.setPublisher_pages(data[0]);
				        	
				        	
				        	
				     //   obj.setGender(data[0]);
				     //   obj.setCode(code);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionid);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  
				  
				  public List<PublisherReport> getQueryFieldChannelSectionFilter(String queryfield,String startdate, String enddate, String channel_name, String sectionname, Map<String,String>filter)
						    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
						  {
						    
					        int size = filter.size();
					        String queryfilterbuilder = "";
					        String formattedString = "";
					        int j =0;
					        for (Map.Entry<String, String> entry : filter.entrySet())
					        {
					        	if (j==0){
					                formattedString = addCommaString(entry.getValue());
					        		queryfilterbuilder = queryfilterbuilder+ entry.getKey() + " in " + "("+formattedString+")";
					        	
					        	}
					            else{
					            formattedString = addCommaString(entry.getValue());	
					            queryfilterbuilder = queryfilterbuilder+ " and "+ entry.getKey() + " in " + "("+formattedString+")";
					       
					            }
					            j++;
					         
					        }
					  
					  
					        
					        String query = "Select count(*),"+queryfield+" from enhanceduserdatabeta1 where "+queryfilterbuilder+" and refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield;
						    System.out.println(query);
					        CSVResult csvResult = getCsvResult(false, query);
						    List<String> headers = csvResult.getHeaders();
						    List<String> lines = csvResult.getLines();
						    List<PublisherReport> pubreport = new ArrayList();
						    
						    //System.out.println(headers);
						    //System.out.println(lines);
						    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
						    {
						      for (int i = 0; i < lines.size(); i++)
						      {
						        PublisherReport obj = new PublisherReport();
						        
						        String[] data = ((String)lines.get(i)).split(",");
						     //   String demographicproperties = demographicmap.get(data[0]);
						            if(queryfield.equals("gender"))
						        	obj.setGender(data[0]);
						        
						            if(queryfield.equals("device"))
						        	obj.setDevice_type(data[0]);
						        	
						        	if(queryfield.equals("city")){
						        	    try{
						        		String locationproperties = citycodeMap.get(data[0]);
								        data[0]=data[0].replace("_"," ").replace("-"," ");
								        obj.setCity(data[0]);
								        System.out.println(data[0]);
								        obj.setLocationcode(locationproperties);
						        	    }
						        	    catch(Exception e)
						        	    {
						        	    	continue;
						        	    }
						        	    
						        	    }
						        	if(queryfield.equals("audience_segment"))
						             obj.setAudience_segment(data[0]);
						        	
						        	if(queryfield.equals("reforiginal"))
							             obj.setReferrerSource(data[0]);
						            	
						        	if(queryfield.equals("agegroup"))
							             obj.setAge(data[0]);
						            	
						        	if(queryfield.equals("incomelevel"))
							          obj.setIncomelevel(data[0]);
						     
						        	
						        	if(queryfield.equals("system_os")){
						        		String osproperties = oscodeMap.get(data[0]);
								        data[0]=data[0].replace("_"," ").replace("-", " ");
								        obj.setOs(data[0]);
								        obj.setOscode(osproperties);
						        	}
						         	
						        	if(queryfield.equals("modelName")){
						        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
							        	
								        obj.setMobile_device_model_name(mobiledeviceproperties[1]);
								        System.out.println(mobiledeviceproperties[1]);
								        obj.setDevicecode(mobiledeviceproperties[0]);
								        System.out.println(mobiledeviceproperties[0]);
						        	}
						         	
						        	if(queryfield.equals("brandName"))
							          obj.setBrandname(data[0]);
						        

						        	if(queryfield.equals("refcurrentoriginal"))
						  	          obj.setPublisher_pages(data[0]);
						        	
						        	
						        	//   obj.setCode(code);
						        obj.setCount(data[1]);
						        obj.setSection(sectionname);
						        obj.setChannelName(channel_name);
						        pubreport.add(obj);
						      }
						      //System.out.println(headers);
						      //System.out.println(lines);
						    }
						    return pubreport;
						  }
				  

				
				  public List<PublisherReport> getQueryFieldChannelSectionGroupBy(String queryfield,String startdate, String enddate, String channel_name, String sectionname,List<String> groupby)
						    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
						  {
						    
					      
				            String querygroupbybuilder = convert(groupby);
				            
				            String query = "";
				            
				            int  l=0;
				            
				         	query = "Select count(*),"+queryfield+","+querygroupbybuilder+" from enhanceduserdatabeta1 where refcurrentoriginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY "+queryfield+","+querygroupbybuilder;
						    System.out.println(query);
				         	CSVResult csvResult = getCsvResult(false, query);
						    List<String> headers = csvResult.getHeaders();
						    List<String> lines = csvResult.getLines();
						    List<PublisherReport> pubreport = new ArrayList();
						    
						    //System.out.println(headers);
						    //System.out.println(lines);
						    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
						    {
						      for (int i = 0; i < lines.size(); i++)
						      {
						        PublisherReport obj = new PublisherReport();
						        
						        String[] data = ((String)lines.get(i)).split(",");
						     //   String demographicproperties = demographicmap.get(data[0]);
						        
						            if(queryfield.equals("gender"))
						        	obj.setGender(data[0]);
						        
						            if(queryfield.equals("device"))
						        	obj.setDevice_type(data[0]);
						        	
						        	if(queryfield.equals("city")){
						        		try{
						        		String locationproperties = citycodeMap.get(data[0]);
								        data[0]=data[0].replace("_"," ").replace("-"," ");
								        obj.setCity(data[0]);
								        System.out.println(data[0]);
								        obj.setLocationcode(locationproperties);
						        		}
						        		catch(Exception e)
						        		{
						        			continue;
						        		}
						        		}
						        	if(queryfield.equals("audience_segment"))
						             obj.setAudience_segment(data[0]);
						        	
						        	if(queryfield.equals("reforiginal"))
							             obj.setReferrerSource(data[0]);
						            	
						        	if(queryfield.equals("agegroup"))
							             obj.setAge(data[0]);
						            	
						        	if(queryfield.equals("incomelevel"))
							          obj.setIncomelevel(data[0]);
						     
						        	
						        	if(queryfield.equals("system_os")){
						        		String osproperties = oscodeMap.get(data[0]);
								        data[0]=data[0].replace("_"," ").replace("-", " ");
								        obj.setOs(data[0]);
								        obj.setOscode(osproperties);
						        	}
						         	
						        	if(queryfield.equals("modelName")){
						        		String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
							        	
								        obj.setMobile_device_model_name(mobiledeviceproperties[1]);
								        System.out.println(mobiledeviceproperties[1]);
								        obj.setDevicecode(mobiledeviceproperties[0]);
								        System.out.println(mobiledeviceproperties[0]);
						        	}
						         	
						        	if(queryfield.equals("brandName"))
							          obj.setBrandname(data[0]);
						        

						        	if(queryfield.equals("refcurrentoriginal"))
						  	          obj.setPublisher_pages(data[0]);
						        	
						        	
						        	
						        	
						        	//   obj.setCode(code);
					            for(int k = 0; k < groupby.size(); k++)
					            {
					            	
					            	if(groupby.get(k).equals(queryfield)==false)
					            	{
					            	try{
					            	if(groupby.get(k).equals("device"))
					            	obj.setDevice_type(data[k+1]);
					            	
					            	if(groupby.get(k).equals("city"))
					            	{
					            		try{
					            		String locationproperties = citycodeMap.get(data[k+1]);
					    		        data[k+1]=data[k+1].replace("_"," ").replace("-"," ");
					    		        obj.setCity(data[k+1]);
					    		        System.out.println(data[k+1]);
					    		        obj.setLocationcode(locationproperties);
					            		}
					            		catch(Exception e)
					            		{
					            			continue;
					            		}
					            	}
					            	
					            	if(groupby.get(k).equals("audience_segment"))
						             obj.setAudience_segment(data[k+1]);
					            	
					            	
					            	if(groupby.get(k).equals("gender"))
							             obj.setGender(data[k+1]);
					            	
					            	
					            	if(groupby.get(k).equals("subcategory"))
							             obj.setSubcategory(data[k+1]);
					            	
					            	if(groupby.get(k).equals("referrer"))
							             obj.setGender(data[k+1]);
						            	
					            	if(groupby.get(k).equals("agegroup"))
							             obj.setAge(data[k+1]);
						            	
					            	if(groupby.get(k).equals("incomelevel"))
							          obj.setIncomelevel(data[k+1]);
						            	
				                    l++;
					            	}
					            	catch(Exception e){
					            	continue;
					            	}
					            	}
					            }
						        
					            if(l!=0)
					            obj.setCount(data[l+1]);
						        obj.setSection(sectionname);
						        obj.setChannelName(channel_name);
						        pubreport.add(obj);
						        l=0;
						      }
						      //System.out.println(headers);
						      //System.out.println(lines);
						    }
						    return pubreport;
						  }
				  
				  	  
  
  
  public List<PublisherReport> getAgegroupChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*),agegroup from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY agegroup";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
    //    String demographicproperties = demographicmap.get(data[0]);
        obj.setAge(data[0]);
  //      obj.setCode(code);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
      }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> getISPChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query = "Select count(*),ISP from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY ISP";
    CSVResult csvResult = getCsvResult(false, query);
    List<String> headers = csvResult.getHeaders();
    List<String> lines = csvResult.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data = ((String)lines.get(i)).split(",");
        if(data[0].trim().toLowerCase().equals("_ltd")==false){ 
        obj.setISP(data[0]);
        obj.setCount(data[1]);
        obj.setChannelName(channel_name);
        pubreport.add(obj);
         }
        }
      //System.out.println(headers);
      //System.out.println(lines);
    }
    return pubreport;
  }
  
  public List<PublisherReport> getOrgChannel(String startdate, String enddate, String channel_name)
    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
  {
    String query1 = "Select count(*),organisation from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY organisation";
    CSVResult csvResult1 = getCsvResult(false, query1);
    List<String> headers1 = csvResult1.getHeaders();
    List<String> lines1 = csvResult1.getLines();
    List<PublisherReport> pubreport = new ArrayList();
    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
    {
      for (int i = 0; i < lines1.size(); i++)
      {
        PublisherReport obj = new PublisherReport();
        
        String[] data1 = ((String)lines1.get(i)).split(",");
        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("communication")) && (!data1[0].trim().toLowerCase().contains("cable")) && (!data1[0].trim().toLowerCase().contains("telecom")) && (!data1[0].trim().toLowerCase().contains("network")) && (!data1[0].trim().toLowerCase().contains("isp")) && (!data1[0].trim().toLowerCase().contains("hathway")) && (!data1[0].trim().toLowerCase().contains("internet")) && (!data1[0].trim().toLowerCase().equals("_ltd")) && (!data1[0].trim().toLowerCase().contains("googlebot")) && (!data1[0].trim().toLowerCase().contains("sify")) && (!data1[0].trim().toLowerCase().contains("bsnl")) && (!data1[0].trim().toLowerCase().contains("reliance")) && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("tata")) && (!data1[0].trim().toLowerCase().contains("nextra")))
        {
          obj.setOrganisation(data1[0]);
          obj.setCount(data1[1]);
          obj.setChannelName(channel_name);
          pubreport.add(obj);
        }
      }
      //System.out.println(headers1);
      //System.out.println(lines1);
    }
    return pubreport;
  }
  
  
  
  public List<PublisherReport> countBrandNameChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    String query = "SELECT COUNT(*)as count,brandName FROM enhanceduserdatabeta1 where referrer= '"+articlename+"' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by brandName";
		    //System.out.println(query);
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        if(data[0].trim().toLowerCase().contains("logitech")==false && data[0].trim().toLowerCase().contains("mozilla")==false && data[0].trim().toLowerCase().contains("web_browser")==false && data[0].trim().toLowerCase().contains("microsoft")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false){ 
		        obj.setBrandname(data[0]);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		        } 
		       }
		  //    //System.out.println(headers);
		  //    //System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countBrowserChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,browser_name FROM enhanceduserdatabeta1 where channel_name ='" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by browser_name";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setBrowser(data[0]);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		      //System.out.println(headers);
		      //System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countOSChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,system_os FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by system_os";
		    System.out.println(query);
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        String osproperties = oscodeMap.get(data[0]);
		        data[0]=data[0].replace("_"," ").replace("-", " ");
		        obj.setOs(data[0]);
		        obj.setOscode(osproperties);
		        System.out.println(data[0]);
		        obj.setCount(data[1]);
		        System.out.println(osproperties);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		    }
		    
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countModelChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,modelName FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by modelName";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");

		        if(data[0].trim().toLowerCase().contains("logitech_revue")==false && data[0].trim().toLowerCase().contains("mozilla_firefox")==false && data[0].trim().toLowerCase().contains("apple_safari")==false && data[0].trim().toLowerCase().contains("generic_web")==false && data[0].trim().toLowerCase().contains("google_compute")==false && data[0].trim().toLowerCase().contains("microsoft_xbox")==false && data[0].trim().toLowerCase().contains("google_chromecast")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false && data[0].trim().toLowerCase().contains("laptop")==false){    
		        String[] mobiledeviceproperties = devicecodeMap.get(data[0]).split(",");
		        	
		        obj.setMobile_device_model_name(mobiledeviceproperties[1]);
		        System.out.println(mobiledeviceproperties[1]);
		        obj.setDevicecode(mobiledeviceproperties[0]);
		        System.out.println(mobiledeviceproperties[0]);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		        
		        }
		    }
		  
		    System.out.println(pubreport.toString());
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countCityChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,city FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by city";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    
		    
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        String locationproperties = citycodeMap.get(data[0]);
		        data[0]=data[0].replace("_"," ").replace("-"," ");
		        obj.setCity(data[0]);
		        System.out.println(data[0]);
		        obj.setLocationcode(locationproperties);
		        System.out.println(locationproperties);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		    }
		   
		    System.out.println(pubreport.toString());
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countfingerprintChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
			  
			  
		//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
			  
		    
			  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where reforiginal= '"+articlename+"' and channel_name = '" + 
				      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
			  
			//	 CSVResult csvResult00 = getCsvResult(false, query00);
				// List<String> headers00 = csvResult00.getHeaders();
		//		 List<String> lines00 = csvResult00.getLines();
			//	 List<PublisherReport> pubreport00 = new ArrayList();  
				
				 
			//	System.out.println(headers00);
			//	System.out.println(lines00);  
				  
				//  for (int i = 0; i < lines00.size(); i++)
			    //  {
			       
			     //   String[] data = ((String)lines00.get(i)).split(",");
			  //      //System.out.println(data[0]);
			     
				  
				  
				  
				Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
			    String query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + 
			      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
			      CSVResult csvResult = getCsvResult(false, query);
			      List<String> headers = csvResult.getHeaders();
			      List<String> lines = csvResult.getLines();
			      List<PublisherReport> pubreport = new ArrayList();
			      System.out.println(headers);
			      System.out.println(lines);
			      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
			      for (int i = 0; i < lines.size(); i++)
			      {
			        PublisherReport obj = new PublisherReport();
			        
			        String[] data = ((String)lines.get(i)).split(",");
			       // obj.setDate(data[0]);
			        obj.setReach(data[0]);
			        obj.setChannelName(channel_name);
			        obj.setArticle(articlename);
			        pubreport.add(obj);
			      }
			    }  
			    
		    return pubreport;
		  }
		  
	
		  
		  public List<PublisherReport> countfingerprintChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename)
				    throws CsvExtractorException, Exception
				  {
					  
					  
				//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where reforiginal= '"+articlename+"' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
						Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + 
					      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					      System.out.println(headers);
					      System.out.println(lines);
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        obj.setDate(data[0]);
					        obj.setReach(data[1]);
					        obj.setChannelName(channel_name);
					        obj.setArticle(articlename);
					        pubreport.add(obj);
					      }
					    }  
					    
				    return pubreport;
				  }
		  
		  
		  public List<PublisherReport> counttotalvisitorsChannelArticle(String startdate, String enddate, String channel_name, String articlename)
				    throws CsvExtractorException, Exception
				  {
					  
					  
				//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
						Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(*) as visits FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + 
					      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
					      CSVResult csvResult = getCsvResult(false, query);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					      System.out.println(headers);
					      System.out.println(lines);
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					       // obj.setDate(data[0]);
					        obj.setTotalvisits(data[0]);
					        obj.setChannelName(channel_name);
					        obj.setArticle(articlename);
					        pubreport.add(obj);
					      }
					    }  
					    
				    return pubreport;
				  }
				  
			
				  
				  public List<PublisherReport> counttotalvisitorsChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setTotalvisits(data[1]);
							        obj.setChannelName(channel_name);
							        obj.setArticle(articlename);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
				  
		  public List<PublisherReport> countAudiencesegmentChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		      List<PublisherReport> pubreport = new ArrayList(); 
			  
			  String querya1 = "SELECT COUNT(DISTINCT(cookie_id)) FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit 20000000";   
			  
			    //Divide count in different limits 
			
			  
			  List<String> Query = new ArrayList();
			  


			    System.out.println(querya1);
			    
			    final long startTime2 = System.currentTimeMillis();
				
			    
			    CSVResult csvResult1 = null;
				try {
					csvResult1 = AggregationModule.getCsvResult(false, querya1);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    
			    final long endTime2 = System.currentTimeMillis();
				
			    List<String> headers = csvResult1.getHeaders();
			    List<String> lines = csvResult1.getLines();
			    
			    
			    String count = lines.get(0);
			    Double countv1 = Double.parseDouble(count);
			    Double n = 0.0;
			    if(countv1 >= 250000)
			       n=10.0;
			    
			    if(countv1 >= 100000 && countv1 <= 250000 )
			       n=10.0;
			    
			    if(countv1 <= 100000 && countv1 > 100)
		           n=10.0;	    
			   
			    if(countv1 <= 100)
			    	n=1.0;
			    
			    if(countv1 == 0)
			    {
			    	
			    	return pubreport;
			    	
			    }
			    
			    Double total_length = countv1 - 0;
			    Double subrange_length = total_length/n;	
			    
			    Double current_start = 0.0;
			    for (int i = 0; i < n; ++i) {
			      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
			      Double startlimit = current_start;
			      Double finallimit = current_start + subrange_length;
			      Double index = startlimit +1;
			      if(countv1 == 1)
			    	  index=0.0;
			      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "' Order by cookie_id limit "+index.intValue()+","+finallimit.intValue();  	
				  System.out.println(query);
			  //    Query.add(query);
			      current_start += subrange_length;
			      Query.add(query);
			      
			    }
			    
			    
			    	
			    
			  
			  ExecutorService executorService = Executors.newFixedThreadPool(2000);
		        
		       List<Callable<FastMap<String,Double>>> lst = new ArrayList<Callable<FastMap<String,Double>>>();
		    
		       for(int i=0 ; i < Query.size(); i++ ){
		       lst.add(new AudienceSegmentQueryExecutionThreads(Query.get(i),client,searchDao));
		    /*   lst.add(new AudienceSegmentQueryExecutionThreads(query1,client,searchDao));
		       lst.add(new AudienceSegmentQueryExecutionThreads(query2,client,searchDao));
		       lst.add(new AudienceSegmentQueryExecutionThreads(query3,client,searchDao));
		       lst.add(new AudienceSegmentQueryExecutionThreads(query4,client,searchDao));*/
		        
		       // returns a list of Futures holding their status and results when all complete
		       lst.add(new SubcategoryQueryExecutionThreads(Query.get(i),client,searchDao));
		   /*    lst.add(new SubcategoryQueryExecutionThreads(query6,client,searchDao));
		       lst.add(new SubcategoryQueryExecutionThreads(query7,client,searchDao));
		       lst.add(new SubcategoryQueryExecutionThreads(query8,client,searchDao));
		       lst.add(new SubcategoryQueryExecutionThreads(query9,client,searchDao)); */
		       }
		       
		       
		       List<Future<FastMap<String,Double>>> maps = executorService.invokeAll(lst);
		        
		       System.out.println(maps.size() +" Responses recieved.\n");
		        
		       for(Future<FastMap<String,Double>> task : maps)
		       {
		    	   try{
		           if(task!=null)
		    	   System.out.println(task.get().toString());
		    	   }
		    	   catch(Exception e)
		    	   {
		    		   e.printStackTrace();
		    		   continue;
		    	   }
		    	    
		    	   
		    	   }
		        
		       /* shutdown your thread pool, else your application will keep running */
		       executorService.shutdown();
			  
			
			  //  //System.out.println(headers1);
			 //   //System.out.println(lines1);
			    
			    
		       
		       FastMap<String,Double> audiencemap = new FastMap<String,Double>();
		       
		       FastMap<String,Double> subcatmap = new FastMap<String,Double>();
		       
		       Double count1 = 0.0;
		       
		       Double count2 = 0.0;
		       
		       String key ="";
		       String key1 = "";
		       Double value = 0.0;
		       Double vlaue1 = 0.0;
		       
			    for (int i = 0; i < maps.size(); i++)
			    {
			    
			    	if(maps!=null && maps.get(i)!=null){
			        FastMap<String,Double> map = (FastMap<String, Double>) maps.get(i).get();
			    	
			       if(map.size() > 0){
			       
			       if(map.containsKey("audience_segment")==true){
			       for (Map.Entry<String, Double> entry : map.entrySet())
			    	 {
			    	  key = entry.getKey();
			    	  key = key.trim();
			    	  value=  entry.getValue();
			    	if(key.equals("audience_segment")==false) { 
			    	if(audiencemap.containsKey(key)==false)
			    	audiencemap.put(key,value);
			    	else
			    	{
			         count1 = audiencemap.get(key);
			         if(count1!=null)
			         audiencemap.put(key,count1+value);	
			    	}
			      }
			    }
			  }   

			       if(map.containsKey("subcategory")==true){
			       for (Map.Entry<String, Double> entry : map.entrySet())
			    	 {
			    	   key = entry.getKey();
			    	   key = key.trim();
			    	   value=  entry.getValue();
			    	if(key.equals("subcategory")==false) {    
			    	if(subcatmap.containsKey(key)==false)
			    	subcatmap.put(key,value);
			    	else
			    	{
			         count1 = subcatmap.get(key);
			         if(count1!=null)
			         subcatmap.put(key,count1+value);	
			    	}
			    }  
			    	
			   }
			      
			     	       }
			           
			       } 
			    
			    	} 	
			   }    
			    
			    String subcategory = null;
			   
			    if(audiencemap.size()>0){
			   
			    	for (Map.Entry<String, Double> entry : audiencemap.entrySet()) {
			    	//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
			    

			        PublisherReport obj = new PublisherReport();
			        
			   //     String[] data = ((String)lines.get(i)).split(",");
			        
			     //   if(data[0].trim().toLowerCase().contains("festivals"))
			      //  obj.setAudience_segment("");
			      //  else
			        obj.setAudience_segment( entry.getKey());	
			        obj.setCount(String.valueOf(entry.getValue()));
			      
			        if ((!entry.getKey().equals("tech")) && (!entry.getKey().equals("india")) && (!entry.getKey().trim().toLowerCase().equals("foodbeverage")) )
			        {
			         for (Map.Entry<String, Double> entry1 : subcatmap.entrySet()) {
			        	 
			        	    
			        	 
			        	 PublisherReport obj1 = new PublisherReport();
			            
			           
			            if (entry1.getKey().contains(entry.getKey()))
			            {
			              String substring = "_" + entry.getKey() + "_";
			              subcategory = entry1.getKey().replace(substring, "");
			           //   if(data[0].trim().toLowerCase().contains("festivals"))
			           //   obj1.setAudience_segment("");
			           //   else
			        
			              //System.out.println(" \n\n\n Key : " + subcategory + " Value : " + entry1.getValue());  
			              obj1.setAudience_segment(subcategory);
			              obj1.setCount(String.valueOf(entry1.getValue()));
			              obj.getAudience_segment_data().add(obj1);
			            }
			          }
			          pubreport.add(obj);
			        }
			      
			    }
			    }
			    return pubreport;
		  }
		  
		  public List<PublisherReport> gettimeofdayChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*) from enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setTime_of_day(data[0]);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		     
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countPinCodeChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,postalcode FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by postalcode";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        String[] data1 = data[0].split("_");
		        String locationproperties  = citycodeMap.get(data1[0]);
		        obj.setPostalcode(data[0]);
		        obj.setCount(data[1]);
		        obj.setLocationcode(locationproperties);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> countLatLongChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws CsvExtractorException, Exception
		  {
		    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
		    String query = "SELECT COUNT(*)as count,latitude_longitude FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by latitude_longitude";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        String[] data1 = data[0].split("_");
		        String locationproperties  = citycodeMap.get(data1[0]);
		        String[] dashcount = data[0].split("_");
		        if ((dashcount.length == 3) && (data[0].charAt(data[0].length() - 1) != '_'))
		        {
		          if (!dashcount[2].isEmpty())
		          {
		            obj.setLatitude_longitude(data[0]);
		            obj.setCount(data[1]);
		            obj.setLocationcode(locationproperties);
		            obj.setChannelName(channel_name);
		            obj.setArticle(articlename);
		          }
		          pubreport.add(obj);
		        }
		      }
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> gettimeofdayQuarterChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*) from enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='4h')";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setTime_of_day(data[0]);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		     
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> gettimeofdayDailyChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*) from enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1d')";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setTime_of_day(data[0]);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		      System.out.println(headers);
		      System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> getdayQuarterdataChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*),QuarterValue from enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY QuarterValue";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    System.out.println(headers);
		      System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        if (data[0].equals("quarter1")) {
		          data[0] = "quarter1 (00 - 04 AM)";
		        }
		        if (data[0].equals("quarter2")) {
		          data[0] = "quarter2 (04 - 08 AM)";
		        }
		        if (data[0].equals("quarter3")) {
		          data[0] = "quarter3 (08 - 12 AM)";
		        }
		        if (data[0].equals("quarter4")) {
		          data[0] = "quarter4 (12 - 16 PM)";
		        }
		        if (data[0].equals("quarter5")) {
		          data[0] = "quarter5 (16 - 20 PM)";
		        }
		        if (data[0].equals("quarter6")) {
		          data[0] = "quarter6 (20 - 24 PM)";
		        }
		        obj.setTime_of_day(data[0]);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		      System.out.println(headers);
		      System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> getGenderChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*),gender from enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY gender";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    List<PublisherReport> pubreport = new ArrayList();
		    
		    System.out.println(headers);
		    System.out.println(lines);
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setGender(data[0]);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		      System.out.println(headers);
		      System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> getAgegroupChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*),agegroup from enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY agegroup";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        obj.setAge(data[0]);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		      }
		    
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> getISPChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query = "Select count(*),ISP from enhanceduserdatabeta1 where referrer= '"+articlename+"' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY ISP";
		    CSVResult csvResult = getCsvResult(false, query);
		    List<String> headers = csvResult.getHeaders();
		    List<String> lines = csvResult.getLines();
		    System.out.println(headers);
		    System.out.println(lines);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data = ((String)lines.get(i)).split(",");
		        if(data[0].trim().toLowerCase().equals("_ltd")==false){ 
		        obj.setISP(data[0]);
		        obj.setCount(data[1]);
		        obj.setChannelName(channel_name);
		        obj.setArticle(articlename);
		        pubreport.add(obj);
		         }
		        }
		     // System.out.println(headers);
		     // System.out.println(lines);
		    }
		    return pubreport;
		  }
		  
		  public List<PublisherReport> getOrgChannelArticle(String startdate, String enddate, String channel_name, String articlename)
		    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
		  {
		    String query1 = "Select count(*),organisation from enhanceduserdatabeta1 where referrer= '"+articlename+"' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY organisation";
		    CSVResult csvResult1 = getCsvResult(false, query1);
		    List<String> headers1 = csvResult1.getHeaders();
		    List<String> lines1 = csvResult1.getLines();
		    System.out.println(headers1);
		      System.out.println(lines1);
		    List<PublisherReport> pubreport = new ArrayList();
		    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
		    {
		      for (int i = 0; i < lines1.size(); i++)
		      {
		        PublisherReport obj = new PublisherReport();
		        
		        String[] data1 = ((String)lines1.get(i)).split(",");
		        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("communication")) && (!data1[0].trim().toLowerCase().contains("cable")) && (!data1[0].trim().toLowerCase().contains("telecom")) && (!data1[0].trim().toLowerCase().contains("network")) && (!data1[0].trim().toLowerCase().contains("isp")) && (!data1[0].trim().toLowerCase().contains("hathway")) && (!data1[0].trim().toLowerCase().contains("internet")) && (!data1[0].trim().toLowerCase().equals("_ltd")) && (!data1[0].trim().toLowerCase().contains("googlebot")) && (!data1[0].trim().toLowerCase().contains("sify")) && (!data1[0].trim().toLowerCase().contains("bsnl")) && (!data1[0].trim().toLowerCase().contains("reliance")) && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("tata")) && (!data1[0].trim().toLowerCase().contains("nextra")))
		        {
		          obj.setOrganisation(data1[0]);
		          obj.setCount(data1[1]);
		          obj.setChannelName(channel_name);
		          obj.setArticle(articlename);
		          pubreport.add(obj);
		        }
		      }
		    //  System.out.println(headers1);
		    //  System.out.println(lines1);
		    }
		    return pubreport;
		  }
		  
		  
		  public List<PublisherReport> getChannelSectionArticleList(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),reforiginal from enhanceduserdatabeta1 where reforiginal like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY reforiginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				      
				        
				          obj.setPublisher_pages(data1[0]);
				          obj.setCount(data1[1]);
				          obj.setChannelName(channel_name);
				          obj.setSection(sectionname);
				          pubreport.add(obj);
				        
				      }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				    }
				    return pubreport;
				  }
		  
		  
		  
		  public List<PublisherReport> getChannelArticleReferrerList(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),reforiginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY reforiginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				        if ((data1[0].trim().toLowerCase().contains("facebook") || (data1[0].trim().toLowerCase().contains("google"))))
				        {
				          //if(data1[0].equals()) 
				         
				          obj.setReferrerSource(data1[0]);
				          obj.setCount(data1[1]);
				          obj.setChannelName(channel_name);
				          obj.setArticle(articlename);
				          pubreport.add(obj);
				        }
				      }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				    }
				    return pubreport;
				  }
		  
		  
		  public List<PublisherReport> getChannelArticleReferrerList1(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),reforiginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY reforiginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				//    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				 //   {
				    
				    String data0="";
				    String data1="";
				    
				    for (int i = 0; i < 5; i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				      //  String[] data1 = ((String)lines1.get(i)).split(",");
				       
				          //if(data1[0].equals()) 
				         
				          if(i == 0){
				          data0="http://m.facebook.com";
				          data1 = "15.0";
				          }
				          

				          if(i == 1){
				          data0="http://www.facebook.com";
				          data1 = "5.0";
				          }
				          
				          
				          if(i == 2){
					          data0="http://l.facebook.com";
					          data1 = "3.0";
					          }
					    
				          
				          if(i == 3){
					          data0="http://www.google.co.pk";
					          data1 = "3.0";
					          }
					          
				          if(i==4){
				        	  data0="http://www.google.co.in";
				              data1 = "2.0";
				          }
				              
				           obj.setReferrerSource(data0);
				          obj.setCount(data1);
				          obj.setChannelName(channel_name);
				          obj.setArticle(articlename);
				          pubreport.add(obj);
				        
				   //   }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				   }
				    return pubreport;
		  
		  
		  
		  
		  
				      }  
		  
		  
		 
		  public List<PublisherReport> getDeviceTypeChannelArticle(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),reforiginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY reforiginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				//    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				 //   {
				    String data0="";
				    String data1="";
				    
				    for (int i = 0; i < 3; i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        
				       
				          //if(data1[0].equals()) 
				         
				          if(i == 0){
				          data0="Mobile";
				          data1 = "18.0";
				          }
				          

				          if(i == 1){
				          data0="Tablet";
				          data1 = "5.0";
				          }
				          
				          
				          if(i == 2){
					          data0="Desktop";
					          data1 = "5.0";
					      }
					    
				        
				          obj.setDevice_type(data0);
				          obj.setCount(data1);
				          obj.setChannelName(channel_name);
				          obj.setArticle(articlename);
				          pubreport.add(obj);
				        
				   //   }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				   }
				    return pubreport;
		  
		  
		  
		  
		  
				      }  
		  
		  
		
		  public List<PublisherReport> getIncomeChannelArticle(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),reforiginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY reforiginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				//    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				 //   {
				   
				    String data0="";
				    String data1="";
				    
				    for (int i = 0; i < 3; i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				       // String[] data1 = ((String)lines1.get(i)).split(",");
				       
				          //if(data1[0].equals()) 
				         
				          if(i == 0){
				          data0="Medium";
				          data1 = "15.0";
				          }
				          

				          if(i == 1){
				          data0="High";
				          data1 = "6.0";
				          }
				          
				          
				          if(i == 2){
					          data0="Low";
					          data1 = "7.0";
					      }
					    
				        
				          obj.setIncomelevel(data0);
				          obj.setCount(data1);
				          obj.setChannelName(channel_name);
				          obj.setArticle(articlename);
				          pubreport.add(obj);
				        
				   //   }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				   }
				    return pubreport;
		  
		  
		  
		  
		  
				      }  
		  
		  
		  public List<PublisherReport> getArticleMetaData(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),reforiginal from enhanceduserdatabeta1 where refcurrentoriginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY reforiginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				//    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				 //   {
				        PublisherReport obj = new PublisherReport();
				     
				     
					    
				        
				          obj.setArticleAuthor("admin");
				          obj.setArticleTags("filmfare,shahid kapoor,deepika padukone,bollywood");
				          obj.setChannelName(channel_name);
				          obj.setArticle(articlename);
				          pubreport.add(obj);
				        
				   //   }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				  
				    return pubreport;
		  
		  
		  
		  
		  
				      }  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  
		  public List<PublisherReport> getChannelArticleReferredPostsList(String startdate, String enddate, String channel_name, String articlename)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),clickurloriginal from enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY clickurloriginal";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				          obj.setPublisher_pages(data1[0]);
				          obj.setCount(data1[1]);
				          obj.setChannelName(channel_name);
				          obj.setSection(articlename);
				          pubreport.add(obj);
				        
				      }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				    }
				    return pubreport;
				  }
		  
		  
		  
				  
		  
		  public List<PublisherReport> getChannelSectionArticleCount(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),referrer from enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY referrer";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("communication")) && (!data1[0].trim().toLowerCase().contains("cable")) && (!data1[0].trim().toLowerCase().contains("telecom")) && (!data1[0].trim().toLowerCase().contains("network")) && (!data1[0].trim().toLowerCase().contains("isp")) && (!data1[0].trim().toLowerCase().contains("hathway")) && (!data1[0].trim().toLowerCase().contains("internet")) && (!data1[0].trim().toLowerCase().equals("_ltd")) && (!data1[0].trim().toLowerCase().contains("googlebot")) && (!data1[0].trim().toLowerCase().contains("sify")) && (!data1[0].trim().toLowerCase().contains("bsnl")) && (!data1[0].trim().toLowerCase().contains("reliance")) && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("tata")) && (!data1[0].trim().toLowerCase().contains("nextra")))
				        {
				          obj.setPublisher_pages(data1[0]);
				          obj.setCount(data1[1]);
				          obj.setChannelName(channel_name);
				          obj.setSection(sectionname);
				          pubreport.add(obj);
				        }
				      }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				    }
				    return pubreport;
				  }
		  
		  public List<PublisherReport> countBrandNameChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    String query = "SELECT COUNT(*)as count,brandName FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by brandName";
				    //System.out.println(query);
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if(data[0].trim().toLowerCase().contains("logitech")==false && data[0].trim().toLowerCase().contains("mozilla")==false && data[0].trim().toLowerCase().contains("web_browser")==false && data[0].trim().toLowerCase().contains("microsoft")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false){ 
				        obj.setBrandname(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				        } 
				       }
				  //    //System.out.println(headers);
				  //    //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countBrowserChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,browser_name FROM enhanceduserdatabeta1 where channel_name ='" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by browser_name";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setBrowser(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countOSChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,system_os FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by system_os";
				    System.out.println(query);
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setOs(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countModelChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,modelName FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by modelName";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");

				        if(data[0].trim().toLowerCase().contains("logitech_revue")==false && data[0].trim().toLowerCase().contains("mozilla_firefox")==false && data[0].trim().toLowerCase().contains("apple_safari")==false && data[0].trim().toLowerCase().contains("generic_web")==false && data[0].trim().toLowerCase().contains("google_compute")==false && data[0].trim().toLowerCase().contains("microsoft_xbox")==false && data[0].trim().toLowerCase().contains("google_chromecast")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false && data[0].trim().toLowerCase().contains("laptop")==false){    
				        obj.setMobile_device_model_name(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				        
				        }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countCityChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,city FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by city";
				    System.out.println(query);
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    
				    
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setCity(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countfingerprintChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
					  
					  
				//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
						Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
					      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
					      CSVResult csvResult = getCsvResult(false, query);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					      System.out.println(headers);
					      System.out.println(lines);
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					       // obj.setDate(data[0]);
					        obj.setReach(data[0]);
					        obj.setChannelName(channel_name);
					        obj.setSection(sectionname);
					        pubreport.add(obj);
					      }
					    }  
					    
				    return pubreport;
				  }
				  
			
				  
				  public List<PublisherReport> countfingerprintChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        obj.setDate(data[0]);
							        obj.setReach(data[1]);
							        obj.setChannelName(channel_name);
							        obj.setSection(sectionname);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
				  
				  
				  public List<PublisherReport> counttotalvisitorsChannelSection(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(*) as visits FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							      System.out.println(headers);
							      System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							       // obj.setDate(data[0]);
							        obj.setTotalvisits(data[0]);
							        obj.setChannelName(channel_name);
							        obj.setSection(sectionname);
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
						  
					
						  
						  public List<PublisherReport> counttotalvisitorsChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
										      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        obj.setChannelName(channel_name);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
				  
				  
						  public List<PublisherReport> counttotalvisitorsChannelSectionDateHourlywise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									//  String query00 = "SELECT date_histogram(field=request_time,interval=1h)cookie_id FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
									//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        obj.setChannelName(channel_name);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
				  
				  
						  public List<PublisherReport> counttotalvisitorsChannelSectionDateHourlyMinutewise(String startdate, String enddate, String channel_name, String sectionname)
								    throws CsvExtractorException, Exception
								  {
									  
									  
								//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
									  
								    
									//  String query00 = "SELECT date_histogram(field=request_time,interval=1h)cookie_id FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
									//	      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
									  
									//	 CSVResult csvResult00 = getCsvResult(false, query00);
										// List<String> headers00 = csvResult00.getHeaders();
								//		 List<String> lines00 = csvResult00.getLines();
									//	 List<PublisherReport> pubreport00 = new ArrayList();  
										
										 
									//	System.out.println(headers00);
									//	System.out.println(lines00);  
										  
										//  for (int i = 0; i < lines00.size(); i++)
									    //  {
									       
									     //   String[] data = ((String)lines00.get(i)).split(",");
									  //      //System.out.println(data[0]);
									     
										  
										  
										  
										Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
									    String query = "SELECT count(*)as visits FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
									      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1m')";
									      CSVResult csvResult = getCsvResult(false, query);
									      List<String> headers = csvResult.getHeaders();
									      List<String> lines = csvResult.getLines();
									      List<PublisherReport> pubreport = new ArrayList();
									      System.out.println(headers);
									      System.out.println(lines);
									      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
									      for (int i = 0; i < lines.size(); i++)
									      {
									        PublisherReport obj = new PublisherReport();
									        
									        String[] data = ((String)lines.get(i)).split(",");
									        obj.setDate(data[0]);
									        obj.setTotalvisits(data[1]);
									        obj.setChannelName(channel_name);
									        obj.setSection(sectionname);
									        pubreport.add(obj);
									      }
									    }  
									    
								    return pubreport;
								  }
				  
				  
				  
				  
				  
				  
				  
				  
				  
						  
				  public List<PublisherReport> countAudiencesegmentChannelSection(String startdate, String enddate, String channel_name, String articlename)
				    throws CsvExtractorException, Exception
				  {
				      List<PublisherReport> pubreport = new ArrayList(); 
					  
					  String querya1 = "SELECT COUNT(DISTINCT(cookie_id)) FROM enhanceduserdatabeta1 where referrer= '"+articlename+"' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit 20000000";   
					  
					    //Divide count in different limits 
					
					  
					  List<String> Query = new ArrayList();
					  


					    System.out.println(querya1);
					    
					    final long startTime2 = System.currentTimeMillis();
						
					    
					    CSVResult csvResult1 = null;
						try {
							csvResult1 = AggregationModule.getCsvResult(false, querya1);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					    
					    final long endTime2 = System.currentTimeMillis();
						
					    List<String> headers = csvResult1.getHeaders();
					    List<String> lines = csvResult1.getLines();
					    
					    
					    String count = lines.get(0);
					    Double countv1 = Double.parseDouble(count);
					    Double n = 0.0;
					    if(countv1 >= 250000)
					       n=10.0;
					    
					    if(countv1 >= 100000 && countv1 <= 250000 )
					       n=10.0;
					    
					    if(countv1 <= 100000 && countv1 > 100)
				           n=10.0;	    
					   
					    if(countv1 <= 100)
					    	n=1.0;
					    
					    if(countv1 == 0)
					    {
					    	
					    	return pubreport;
					    	
					    }
					    
					    Double total_length = countv1 - 0;
					    Double subrange_length = total_length/n;	
					    
					    Double current_start = 0.0;
					    for (int i = 0; i < n; ++i) {
					      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
					      Double startlimit = current_start;
					      Double finallimit = current_start + subrange_length;
					      Double index = startlimit +1;
					      if(countv1 == 1)
					    	  index=0.0;
					      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdatabeta1 where referrer= '"+articlename+"' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "' Order by cookie_id limit "+index.intValue()+","+finallimit.intValue();  	
						  System.out.println(query);
					  //    Query.add(query);
					      current_start += subrange_length;
					      Query.add(query);
					      
					    }
					    
					    
					    	
					    
					  
					  ExecutorService executorService = Executors.newFixedThreadPool(2000);
				        
				       List<Callable<FastMap<String,Double>>> lst = new ArrayList<Callable<FastMap<String,Double>>>();
				    
				       for(int i=0 ; i < Query.size(); i++ ){
				       lst.add(new AudienceSegmentQueryExecutionThreads(Query.get(i),client,searchDao));
				    /*   lst.add(new AudienceSegmentQueryExecutionThreads(query1,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query2,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query3,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query4,client,searchDao));*/
				        
				       // returns a list of Futures holding their status and results when all complete
				       lst.add(new SubcategoryQueryExecutionThreads(Query.get(i),client,searchDao));
				   /*    lst.add(new SubcategoryQueryExecutionThreads(query6,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query7,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query8,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query9,client,searchDao)); */
				       }
				       
				       
				       List<Future<FastMap<String,Double>>> maps = executorService.invokeAll(lst);
				        
				       System.out.println(maps.size() +" Responses recieved.\n");
				        
				       for(Future<FastMap<String,Double>> task : maps)
				       {
				    	   try{
				           if(task!=null)
				    	   System.out.println(task.get().toString());
				    	   }
				    	   catch(Exception e)
				    	   {
				    		   e.printStackTrace();
				    		   continue;
				    	   }
				    	    
				    	   
				    	   }
				        
				       /* shutdown your thread pool, else your application will keep running */
				       executorService.shutdown();
					  
					
					  //  //System.out.println(headers1);
					 //   //System.out.println(lines1);
					    
					    
				       
				       FastMap<String,Double> audiencemap = new FastMap<String,Double>();
				       
				       FastMap<String,Double> subcatmap = new FastMap<String,Double>();
				       
				       Double count1 = 0.0;
				       
				       Double count2 = 0.0;
				       
				       String key ="";
				       String key1 = "";
				       Double value = 0.0;
				       Double vlaue1 = 0.0;
				       
					    for (int i = 0; i < maps.size(); i++)
					    {
					    
					    	if(maps!=null && maps.get(i)!=null){
					        FastMap<String,Double> map = (FastMap<String, Double>) maps.get(i).get();
					    	
					       if(map.size() > 0){
					       
					       if(map.containsKey("audience_segment")==true){
					       for (Map.Entry<String, Double> entry : map.entrySet())
					    	 {
					    	  key = entry.getKey();
					    	  key = key.trim();
					    	  value=  entry.getValue();
					    	if(key.equals("audience_segment")==false) { 
					    	if(audiencemap.containsKey(key)==false)
					    	audiencemap.put(key,value);
					    	else
					    	{
					         count1 = audiencemap.get(key);
					         if(count1!=null)
					         audiencemap.put(key,count1+value);	
					    	}
					      }
					    }
					  }   

					       if(map.containsKey("subcategory")==true){
					       for (Map.Entry<String, Double> entry : map.entrySet())
					    	 {
					    	   key = entry.getKey();
					    	   key = key.trim();
					    	   value=  entry.getValue();
					    	if(key.equals("subcategory")==false) {    
					    	if(subcatmap.containsKey(key)==false)
					    	subcatmap.put(key,value);
					    	else
					    	{
					         count1 = subcatmap.get(key);
					         if(count1!=null)
					         subcatmap.put(key,count1+value);	
					    	}
					    }  
					    	
					   }
					      
					     	       }
					           
					       } 
					    
					    	} 	
					   }    
					    
					    String subcategory = null;
					   
					    if(audiencemap.size()>0){
					   
					    	for (Map.Entry<String, Double> entry : audiencemap.entrySet()) {
					    	//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
					    

					        PublisherReport obj = new PublisherReport();
					        
					   //     String[] data = ((String)lines.get(i)).split(",");
					        
					     //   if(data[0].trim().toLowerCase().contains("festivals"))
					      //  obj.setAudience_segment("");
					      //  else
					        obj.setAudience_segment( entry.getKey());	
					        obj.setCount(String.valueOf(entry.getValue()));
					      
					        if ((!entry.getKey().equals("tech")) && (!entry.getKey().equals("india")) && (!entry.getKey().trim().toLowerCase().equals("foodbeverage")) )
					        {
					         for (Map.Entry<String, Double> entry1 : subcatmap.entrySet()) {
					        	 
					        	    
					        	 
					        	 PublisherReport obj1 = new PublisherReport();
					            
					           
					            if (entry1.getKey().contains(entry.getKey()))
					            {
					              String substring = "_" + entry.getKey() + "_";
					              subcategory = entry1.getKey().replace(substring, "");
					           //   if(data[0].trim().toLowerCase().contains("festivals"))
					           //   obj1.setAudience_segment("");
					           //   else
					        
					              //System.out.println(" \n\n\n Key : " + subcategory + " Value : " + entry1.getValue());  
					              obj1.setAudience_segment(subcategory);
					              obj1.setCount(String.valueOf(entry1.getValue()));
					              obj.getAudience_segment_data().add(obj1);
					            }
					          }
					          pubreport.add(obj);
					        }
					      
					    }
					    }
					    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				     
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countPinCodeChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,postalcode FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by postalcode";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setPostalcode(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countLatLongChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,latitude_longitude FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by latitude_longitude";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        String[] dashcount = data[0].split("_");
				        if ((dashcount.length == 3) && (data[0].charAt(data[0].length() - 1) != '_'))
				        {
				          if (!dashcount[2].isEmpty())
				          {
				            obj.setLatitude_longitude(data[0]);
				            obj.setCount(data[1]);
				            obj.setChannelName(channel_name);
				            obj.setSection(sectionname);
				          }
				          pubreport.add(obj);
				        }
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayQuarterChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='4h')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				     
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayDailyChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1d')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				      System.out.println(headers);
				      System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getdayQuarterdataChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),QuarterValue from enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY QuarterValue";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    System.out.println(headers);
				      System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if (data[0].equals("quarter1")) {
				          data[0] = "quarter1 (00 - 04 AM)";
				        }
				        if (data[0].equals("quarter2")) {
				          data[0] = "quarter2 (04 - 08 AM)";
				        }
				        if (data[0].equals("quarter3")) {
				          data[0] = "quarter3 (08 - 12 AM)";
				        }
				        if (data[0].equals("quarter4")) {
				          data[0] = "quarter4 (12 - 16 PM)";
				        }
				        if (data[0].equals("quarter5")) {
				          data[0] = "quarter5 (16 - 20 PM)";
				        }
				        if (data[0].equals("quarter6")) {
				          data[0] = "quarter6 (20 - 24 PM)";
				        }
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				      System.out.println(headers);
				      System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getGenderChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),gender from enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY gender";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    
				    System.out.println(headers);
				    System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setGender(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				      System.out.println(headers);
				      System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getAgegroupChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),agegroup from enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY agegroup";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setAge(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				      }
				    
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getISPChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),ISP from enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY ISP";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    System.out.println(headers);
				    System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if(data[0].trim().toLowerCase().equals("_ltd")==false){ 
				        obj.setISP(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        obj.setSection(sectionname);
				        pubreport.add(obj);
				         }
				        }
				     // System.out.println(headers);
				     // System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getOrgChannelSection(String startdate, String enddate, String channel_name, String sectionname)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),organisation from enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY organisation";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    System.out.println(headers1);
				      System.out.println(lines1);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("communication")) && (!data1[0].trim().toLowerCase().contains("cable")) && (!data1[0].trim().toLowerCase().contains("telecom")) && (!data1[0].trim().toLowerCase().contains("network")) && (!data1[0].trim().toLowerCase().contains("isp")) && (!data1[0].trim().toLowerCase().contains("hathway")) && (!data1[0].trim().toLowerCase().contains("internet")) && (!data1[0].trim().toLowerCase().equals("_ltd")) && (!data1[0].trim().toLowerCase().contains("googlebot")) && (!data1[0].trim().toLowerCase().contains("sify")) && (!data1[0].trim().toLowerCase().contains("bsnl")) && (!data1[0].trim().toLowerCase().contains("reliance")) && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("tata")) && (!data1[0].trim().toLowerCase().contains("nextra")))
				        {
				          obj.setOrganisation(data1[0]);
				          obj.setCount(data1[1]);
				          obj.setChannelName(channel_name);
				          obj.setSection(sectionname);
				          pubreport.add(obj);
				        }
				      }
				    //  System.out.println(headers1);
				    //  System.out.println(lines1);
				    }
				    return pubreport;
				  }
				  
				  
				
				  
				  
				  
				  public List<PublisherReport> getChannelSectionReferrerList(String startdate, String enddate, String channel_name, String sectionname)
						    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
						  {
						    String query1 = "Select count(*),referrer from enhanceduserdatabeta1 where refcurrent like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY referrer";
						    CSVResult csvResult1 = getCsvResult(false, query1);
						    List<String> headers1 = csvResult1.getHeaders();
						    List<String> lines1 = csvResult1.getLines();
						    System.out.println(headers1);
						      System.out.println(lines1);
						    List<PublisherReport> pubreport = new ArrayList();
						    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
						    {
						      for (int i = 0; i < lines1.size(); i++)
						      {
						        PublisherReport obj = new PublisherReport();
						        
						        String[] data1 = ((String)lines1.get(i)).split(",");
						        if ((data1[0].trim().toLowerCase().contains("facebook") || (data1[0].trim().toLowerCase().contains("google"))))
						        {
						          obj.setReferrerSource(data1[0]);
						          obj.setCount(data1[1]);
						          obj.setChannelName(channel_name);
						          obj.setSection(sectionname);
						          pubreport.add(obj);
						        }
						      }
						    //  System.out.println(headers1);
						    //  System.out.println(lines1);
						    }
						    return pubreport;
						  }
				  
				  
				  public List<PublisherReport> getChannelSectionReferredPostsList(String startdate, String enddate, String channel_name, String sectionname)
						    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
						  {
						    String query1 = "Select count(*),clickedurl from enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY clickedurl";
						    CSVResult csvResult1 = getCsvResult(false, query1);
						    List<String> headers1 = csvResult1.getHeaders();
						    List<String> lines1 = csvResult1.getLines();
						    System.out.println(headers1);
						      System.out.println(lines1);
						    List<PublisherReport> pubreport = new ArrayList();
						    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
						    {
						      for (int i = 0; i < lines1.size(); i++)
						      {
						        PublisherReport obj = new PublisherReport();
						        
						        String[] data1 = ((String)lines1.get(i)).split(",");
						          obj.setPublisher_pages(data1[0]);
						          obj.setCount(data1[1]);
						          obj.setChannelName(channel_name);
						          obj.setSection(sectionname);
						          pubreport.add(obj);
						        
						      }
						    //  System.out.println(headers1);
						    //  System.out.println(lines1);
						    }
						    return pubreport;
						  }
				  
				  
				  
				  public List<PublisherReport> countNewUsersChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
							//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
							    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query00);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							//      System.out.println(headers);
							//      System.out.println(lines);
							      Double count = 0.0;
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							       
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        if (Double.parseDouble(data[1].trim()) < 2.0)
							        {
							        count++;
							        
							        }
							        
							       }
							    }  
							
							      PublisherReport obj = new PublisherReport();
							      obj.setCount(count.toString());
							      obj.setSection(sectionname);
							      pubreport.add(obj);
							      System.out.println("Section:"+sectionname+"Count:"+count);
							      
						    return pubreport;
						  }
				  
				  
				  public List<PublisherReport> countReturningUsersChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
							  
							  
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					   //   System.out.println(headers);
					   //   System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) >= 2.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      obj.setCount(count.toString());
					      obj.setSection(sectionname);
					      pubreport.add(obj);
					      System.out.println("Section:"+sectionname+"Count:"+count);
					      
				          return pubreport;
						  }
				  
				  
				  
				  public List<PublisherReport> countLoyalUsersChannelSectionDatewise(String startdate, String enddate, String channel_name, String sectionname)
						    throws CsvExtractorException, Exception
						  {
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					//      System.out.println(headers);
					 //     System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) > 7.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      obj.setCount(count.toString());
					      obj.setSection(sectionname);
					      pubreport.add(obj);
					      System.out.println("Section:"+sectionname+"Count:"+count);
					      
				          return pubreport;
							  
						
						  }
				  
				  public List<PublisherReport> countNewUsersChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
							//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
							    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query00);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							 //     System.out.println(headers);
							 //     System.out.println(lines);
							      Double count = 0.0;
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							       
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        if (Double.parseDouble(data[1].trim()) < 2.0)
							        {
							        count++;
							        
							        }
							        
							       }
							    }  
							
							      PublisherReport obj = new PublisherReport();
							      obj.setCount(count.toString());
							      obj.setArticle(articlename);
							      pubreport.add(obj);
							      System.out.println("Article:"+articlename+"Count:"+count);
							      
						    return pubreport;
						  }
				  
				  
				  public List<PublisherReport> countReturningUsersChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
							  
							  
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					//      System.out.println(headers);
					//      System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) >= 2.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      obj.setCount(count.toString());
					      obj.setArticle(articlename);
					      pubreport.add(obj);
					      System.out.println("Article:"+articlename+"Count:"+count);
					      
				          return pubreport;
						  }
				  
				  
				  
				  public List<PublisherReport> countLoyalUsersChannelArticleDatewise(String startdate, String enddate, String channel_name, String articlename)
						    throws CsvExtractorException, Exception
						  {
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where reforiginal like '%"+articlename+"%' and channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					  //    System.out.println(headers);
					  //    System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) > 7.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      obj.setCount(count.toString());
					      obj.setArticle(articlename);
					      pubreport.add(obj);
					      System.out.println("Article:"+articlename+"Count:"+count);
					      
				          return pubreport;
						  }			  
						
				  
				  
				  
				  public List<PublisherReport> countNewUsersChannelDatewise(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
							//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
							    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
							      CSVResult csvResult = getCsvResult(false, query00);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							//      System.out.println(headers);
							 //     System.out.println(lines);
							      Double count = 0.0;
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							       
							        
							        String[] data = ((String)lines.get(i)).split(",");
							        if (Double.parseDouble(data[1].trim()) < 2.0)
							        {
							        count++;
							        
							        }
							        
							       }
							    }  
							
							      PublisherReport obj = new PublisherReport();
							      obj.setCount(count.toString());
							      obj.setChannelName(channel_name);
							      pubreport.add(obj);
							   
							      
						    return pubreport;
						  }
				  
				  
				  public List<PublisherReport> countReturningUsersChannelDatewise(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
							  
							  
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					   //   System.out.println(headers);
					   //   System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) >= 2.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      obj.setCount(count.toString());
					      obj.setChannelName(channel_name);
					      pubreport.add(obj);
					      
				          return pubreport;
						  }
				  
				  
				  
				  public List<PublisherReport> countLoyalUsersChannelDatewise(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
						      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
					//	 CSVResult csvResult00 = getCsvResult(false, query00);
						// List<String> headers00 = csvResult00.getHeaders();
				//		 List<String> lines00 = csvResult00.getLines();
					//	 List<PublisherReport> pubreport00 = new ArrayList();  
						
						 
					//	System.out.println(headers00);
					//	System.out.println(lines00);  
						  
						//  for (int i = 0; i < lines00.size(); i++)
					    //  {
					       
					     //   String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					     
						  
						  
						  
					//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
					    //  channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					      CSVResult csvResult = getCsvResult(false, query00);
					      List<String> headers = csvResult.getHeaders();
					      List<String> lines = csvResult.getLines();
					      List<PublisherReport> pubreport = new ArrayList();
					 //     System.out.println(headers);
					 //     System.out.println(lines);
					      Double count = 0.0;
					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					       
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        if (Double.parseDouble(data[1].trim()) > 7.0)
					        {
					        count++;
					        
					        }
					        
					       }
					    }  
					
					      PublisherReport obj = new PublisherReport();
					      obj.setCount(count.toString());
					      obj.setChannelName(channel_name);
					      pubreport.add(obj);
					  
					      
				          return pubreport;
						  }			 
				 
				
				  
				  public List<PublisherReport> counttotalvisitorsChannel(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(*) as visits FROM enhanceduserdatabeta1 where channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							   //   System.out.println(headers);
							  //    System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							       // obj.setDate(data[0]);
							        obj.setTotalvisits(data[0]);
							        obj.setChannelName(channel_name);
							        
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }
  
				  
				  public List<PublisherReport> countUniqueVisitorsChannel(String startdate, String enddate, String channel_name)
						    throws CsvExtractorException, Exception
						  {
							  
							  
						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
							  
						    
							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
								      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
							  
							//	 CSVResult csvResult00 = getCsvResult(false, query00);
								// List<String> headers00 = csvResult00.getHeaders();
						//		 List<String> lines00 = csvResult00.getLines();
							//	 List<PublisherReport> pubreport00 = new ArrayList();  
								
								 
							//	System.out.println(headers00);
							//	System.out.println(lines00);  
								  
								//  for (int i = 0; i < lines00.size(); i++)
							    //  {
							       
							     //   String[] data = ((String)lines00.get(i)).split(",");
							  //      //System.out.println(data[0]);
							     
								  
								  
								  
								Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
							    String query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where channel_name = '" + 
							      channel_name + "' and date between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
							      CSVResult csvResult = getCsvResult(false, query);
							      List<String> headers = csvResult.getHeaders();
							      List<String> lines = csvResult.getLines();
							      List<PublisherReport> pubreport = new ArrayList();
							  //    System.out.println(headers);
							   //   System.out.println(lines);
							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
							      for (int i = 0; i < lines.size(); i++)
							      {
							        PublisherReport obj = new PublisherReport();
							        
							        String[] data = ((String)lines.get(i)).split(",");
							       // obj.setDate(data[0]);
							        obj.setReach(data[0]);
							        obj.setChannelName(channel_name);
							        
							        pubreport.add(obj);
							      }
							    }  
							    
						    return pubreport;
						  }	  
				  
				  
				  
				  public List<PublisherReport> countBrandNameChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    String query = "SELECT COUNT(*)as count,brandName FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by brandName";
				    //System.out.println(query);
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if(data[0].trim().toLowerCase().contains("logitech")==false && data[0].trim().toLowerCase().contains("mozilla")==false && data[0].trim().toLowerCase().contains("web_browser")==false && data[0].trim().toLowerCase().contains("microsoft")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false){ 
				        obj.setBrandname(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				        } 
				       }
				  //    //System.out.println(headers);
				  //    //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countBrowserChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = "SELECT COUNT(*)as count,browser_name FROM enhanceduserdatabeta1 where channel_name ='" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by browser_name";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setBrowser(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countOSChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = String.format("SELECT COUNT(*)as count,system_os FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by system_os", new Object[] { "enhanceduserdatabeta1" });
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				  //  //System.out.println(headers);
				  //  //System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setOs(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countModelChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = String.format("SELECT COUNT(*)as count,modelName FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by modelName", new Object[] { "enhanceduserdatabeta1" });
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");

				        if(data[0].trim().toLowerCase().contains("logitech_revue")==false && data[0].trim().toLowerCase().contains("mozilla_firefox")==false && data[0].trim().toLowerCase().contains("apple_safari")==false && data[0].trim().toLowerCase().contains("generic_web")==false && data[0].trim().toLowerCase().contains("google_compute")==false && data[0].trim().toLowerCase().contains("microsoft_xbox")==false && data[0].trim().toLowerCase().contains("google_chromecast")==false && data[0].trim().toLowerCase().contains("opera")==false && data[0].trim().toLowerCase().contains("epiphany")==false && data[0].trim().toLowerCase().contains("laptop")==false){    
				        obj.setMobile_device_model_name(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				      }
				        
				        }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countCityChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = String.format("SELECT COUNT(*)as count,city FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by city", new Object[] { "enhanceduserdatabeta1" });
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setCity(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countfingerprintChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
					  
					  
					  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
					  
				    
					  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
						      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
					  
						 CSVResult csvResult00 = getCsvResult(false, query00);
						 List<String> headers00 = csvResult00.getHeaders();
						 List<String> lines00 = csvResult00.getLines();
						 List<PublisherReport> pubreport00 = new ArrayList();  
							  
						//  //System.out.println(headers00);
						//  //System.out.println(lines00);  
						  
						  for (int i = 0; i < lines00.size(); i++)
					      {
					       
					        String[] data = ((String)lines00.get(i)).split(",");
					  //      //System.out.println(data[0]);
					      }
						  
						  
						  
						Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
					    String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where channel_name = '" + 
					      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
					    CSVResult csvResult = getCsvResult(false, query);
					    List<String> headers = csvResult.getHeaders();
					    List<String> lines = csvResult.getLines();
					    List<PublisherReport> pubreport = new ArrayList();
					    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
					      for (int i = 0; i < lines.size(); i++)
					      {
					        PublisherReport obj = new PublisherReport();
					        
					        String[] data = ((String)lines.get(i)).split(",");
					        obj.setDate(data[0]);
					        obj.setReach(data[1]);
					        obj.setChannelName(channel_name);
					        pubreport.add(obj);
					      }
					    }
					    
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countAudiencesegmentChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				      List<PublisherReport> pubreport = new ArrayList(); 
					  
					  String querya1 = "SELECT COUNT(DISTINCT(cookie_id)) FROM enhanceduserdata where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate +"' limit 20000000";   
					  
					    //Divide count in different limits 
					
					  
					  List<String> Query = new ArrayList();
					  


					    System.out.println(querya1);
					    
					    final long startTime2 = System.currentTimeMillis();
						
					    
					    CSVResult csvResult1 = null;
						try {
							csvResult1 = AggregationModule.getCsvResult(false, querya1);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					    
					    final long endTime2 = System.currentTimeMillis();
						
					    List<String> headers = csvResult1.getHeaders();
					    List<String> lines = csvResult1.getLines();
					    
					    
					    String count = lines.get(0);
					    Double countv1 = Double.parseDouble(count);
					    Double n = 0.0;
					    if(countv1 >= 250000)
					       n=10.0;
					    
					    if(countv1 >= 100000 && countv1 <= 250000 )
					       n=10.0;
					    
					    if(countv1 <= 100000 && countv1 > 100)
				           n=10.0;	    
					   
					    if(countv1 <= 100)
					    	n=1.0;
					    
					    if(countv1 == 0)
					    {
					    	
					    	return pubreport;
					    	
					    }
					    
					    Double total_length = countv1 - 0;
					    Double subrange_length = total_length/n;	
					    
					    Double current_start = 0.0;
					    for (int i = 0; i < n; ++i) {
					      System.out.println("Smaller range: [" + current_start + ", " + (current_start + subrange_length) + "]");
					      Double startlimit = current_start;
					      Double finallimit = current_start + subrange_length;
					      Double index = startlimit +1;
					      if(countv1 == 1)
					    	  index=0.0;
					      String query = "SELECT DISTINCT(cookie_id) FROM enhanceduserdata where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "' Order by cookie_id limit "+index.intValue()+","+finallimit.intValue();  	
						  System.out.println(query);
					  //    Query.add(query);
					      current_start += subrange_length;
					      Query.add(query);
					     
					    }
					    
					    
					    	
					    
					  
					  ExecutorService executorService = Executors.newFixedThreadPool(2000);
				        
				       List<Callable<FastMap<String,Double>>> lst = new ArrayList<Callable<FastMap<String,Double>>>();
				    
				       for(int i=0 ; i < Query.size(); i++ ){
				       lst.add(new AudienceSegmentQueryExecutionThreads(Query.get(i),client,searchDao));
				    /*   lst.add(new AudienceSegmentQueryExecutionThreads(query1,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query2,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query3,client,searchDao));
				       lst.add(new AudienceSegmentQueryExecutionThreads(query4,client,searchDao));*/
				        
				       // returns a list of Futures holding their status and results when all complete
				       lst.add(new SubcategoryQueryExecutionThreads(Query.get(i),client,searchDao));
				   /*    lst.add(new SubcategoryQueryExecutionThreads(query6,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query7,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query8,client,searchDao));
				       lst.add(new SubcategoryQueryExecutionThreads(query9,client,searchDao)); */
				       }
				       
				       
				       List<Future<FastMap<String,Double>>> maps = executorService.invokeAll(lst);
				        
				       System.out.println(maps.size() +" Responses recieved.\n");
				        
				       for(Future<FastMap<String,Double>> task : maps)
				       {
				    	   try{
				           if(task!=null)
				    	   System.out.println(task.get().toString());
				    	   }
				    	   catch(Exception e)
				    	   {
				    		   e.printStackTrace();
				    		   continue;
				    	   }
				    	    
				    	   
				    	   }
				        
				       /* shutdown your thread pool, else your application will keep running */
				       executorService.shutdown();
					  
					
					  //  //System.out.println(headers1);
					 //   //System.out.println(lines1);
					    
					    
				       
				       FastMap<String,Double> audiencemap = new FastMap<String,Double>();
				       
				       FastMap<String,Double> subcatmap = new FastMap<String,Double>();
				       
				       Double count1 = 0.0;
				       
				       Double count2 = 0.0;
				       
				       String key ="";
				       String key1 = "";
				       Double value = 0.0;
				       Double vlaue1 = 0.0;
				       
					    for (int i = 0; i < maps.size(); i++)
					    {
					    
					    	if(maps!=null && maps.get(i)!=null){
					        FastMap<String,Double> map = (FastMap<String, Double>) maps.get(i).get();
					    	
					       if(map.size() > 0){
					       
					       if(map.containsKey("audience_segment")==true){
					       for (Map.Entry<String, Double> entry : map.entrySet())
					    	 {
					    	  key = entry.getKey();
					    	  key = key.trim();
					    	  value=  entry.getValue();
					    	if(key.equals("audience_segment")==false) { 
					    	if(audiencemap.containsKey(key)==false)
					    	audiencemap.put(key,value);
					    	else
					    	{
					         count1 = audiencemap.get(key);
					         if(count1!=null)
					         audiencemap.put(key,count1+value);	
					    	}
					      }
					    }
					  }   

					       if(map.containsKey("subcategory")==true){
					       for (Map.Entry<String, Double> entry : map.entrySet())
					    	 {
					    	   key = entry.getKey();
					    	   key = key.trim();
					    	   value=  entry.getValue();
					    	if(key.equals("subcategory")==false) {    
					    	if(subcatmap.containsKey(key)==false)
					    	subcatmap.put(key,value);
					    	else
					    	{
					         count1 = subcatmap.get(key);
					         if(count1!=null)
					         subcatmap.put(key,count1+value);	
					    	}
					    }  
					    	
					   }
					      
					     	       }
					           
					       } 
					    
					    	} 	
					   }    
					    
					    String subcategory = null;
					   
					    if(audiencemap.size()>0){
					   
					    	for (Map.Entry<String, Double> entry : audiencemap.entrySet()) {
					    	//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
					    

					        PublisherReport obj = new PublisherReport();
					        
					   //     String[] data = ((String)lines.get(i)).split(",");
					        
					     //   if(data[0].trim().toLowerCase().contains("festivals"))
					      //  obj.setAudience_segment("");
					      //  else
					        obj.setAudience_segment( entry.getKey());	
					        obj.setCount(String.valueOf(entry.getValue()));
					      
					        if ((!entry.getKey().equals("tech")) && (!entry.getKey().equals("india")) && (!entry.getKey().trim().toLowerCase().equals("foodbeverage")) )
					        {
					         for (Map.Entry<String, Double> entry1 : subcatmap.entrySet()) {
					        	 
					        	    
					        	 
					        	 PublisherReport obj1 = new PublisherReport();
					            
					           
					            if (entry1.getKey().contains(entry.getKey()))
					            {
					              String substring = "_" + entry.getKey() + "_";
					              subcategory = entry1.getKey().replace(substring, "");
					           //   if(data[0].trim().toLowerCase().contains("festivals"))
					           //   obj1.setAudience_segment("");
					           //   else
					        
					              //System.out.println(" \n\n\n Key : " + subcategory + " Value : " + entry1.getValue());  
					              obj1.setAudience_segment(subcategory);
					              obj1.setCount(String.valueOf(entry1.getValue()));
					              obj.getAudience_segment_data().add(obj1);
					            }
					          }
					          pubreport.add(obj);
					        }
					      
					    }
					    }
					    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1h')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countPinCodeChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = String.format("SELECT COUNT(*)as count,postalcode FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by postalcode", new Object[] { "enhanceduserdatabeta1" });
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    //System.out.println(headers);
				    //System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setPostalcode(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> countLatLongChannelLive(String startdate, String enddate, String channel_name)
				    throws CsvExtractorException, Exception
				  {
				    Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				    String query = String.format("SELECT COUNT(*)as count,latitude_longitude FROM enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by latitude_longitude", new Object[] { "enhanceduserdatabeta1" });
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    //System.out.println(headers);
				    //System.out.println(lines);
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        String[] dashcount = data[0].split("_");
				        if ((dashcount.length == 3) && (data[0].charAt(data[0].length() - 1) != '_'))
				        {
				          if (!dashcount[2].isEmpty())
				          {
				            obj.setLatitude_longitude(data[0]);
				            obj.setCount(data[1]);
				            obj.setChannelName(channel_name);
				          }
				          pubreport.add(obj);
				        }
				      }
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayQuarterChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='4h')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> gettimeofdayDailyChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*) from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY date_histogram(field='request_time','interval'='1d')";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getdayQuarterdataChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),QuarterValue from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY QuarterValue";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if (data[0].equals("quarter1")) {
				          data[0] = "quarter1 (00 - 04 AM)";
				        }
				        if (data[0].equals("quarter2")) {
				          data[0] = "quarter2 (04 - 08 AM)";
				        }
				        if (data[0].equals("quarter3")) {
				          data[0] = "quarter3 (08 - 12 AM)";
				        }
				        if (data[0].equals("quarter4")) {
				          data[0] = "quarter4 (12 - 16 PM)";
				        }
				        if (data[0].equals("quarter5")) {
				          data[0] = "quarter5 (16 - 20 PM)";
				        }
				        if (data[0].equals("quarter6")) {
				          data[0] = "quarter6 (20 - 24 PM)";
				        }
				        obj.setTime_of_day(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getGenderChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),gender from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY gender";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    
				    //System.out.println(headers);
				    //System.out.println(lines);
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setGender(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getAgegroupChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),agegroup from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY agegroup";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        obj.setAge(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				      }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getISPChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query = "Select count(*),ISP from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY ISP";
				    CSVResult csvResult = getCsvResult(false, query);
				    List<String> headers = csvResult.getHeaders();
				    List<String> lines = csvResult.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data = ((String)lines.get(i)).split(",");
				        if(data[0].trim().toLowerCase().equals("_ltd")==false){ 
				        obj.setISP(data[0]);
				        obj.setCount(data[1]);
				        obj.setChannelName(channel_name);
				        pubreport.add(obj);
				         }
				        }
				      //System.out.println(headers);
				      //System.out.println(lines);
				    }
				    return pubreport;
				  }
				  
				  public List<PublisherReport> getOrgChannelLive(String startdate, String enddate, String channel_name)
				    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  {
				    String query1 = "Select count(*),organisation from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY organisation";
				    CSVResult csvResult1 = getCsvResult(false, query1);
				    List<String> headers1 = csvResult1.getHeaders();
				    List<String> lines1 = csvResult1.getLines();
				    List<PublisherReport> pubreport = new ArrayList();
				    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				    {
				      for (int i = 0; i < lines1.size(); i++)
				      {
				        PublisherReport obj = new PublisherReport();
				        
				        String[] data1 = ((String)lines1.get(i)).split(",");
				        if ((data1[0].length() > 3) && (data1[0].charAt(0) != '_') && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("communication")) && (!data1[0].trim().toLowerCase().contains("cable")) && (!data1[0].trim().toLowerCase().contains("telecom")) && (!data1[0].trim().toLowerCase().contains("network")) && (!data1[0].trim().toLowerCase().contains("isp")) && (!data1[0].trim().toLowerCase().contains("hathway")) && (!data1[0].trim().toLowerCase().contains("internet")) && (!data1[0].trim().toLowerCase().equals("_ltd")) && (!data1[0].trim().toLowerCase().contains("googlebot")) && (!data1[0].trim().toLowerCase().contains("sify")) && (!data1[0].trim().toLowerCase().contains("bsnl")) && (!data1[0].trim().toLowerCase().contains("reliance")) && (!data1[0].trim().toLowerCase().contains("broadband")) && (!data1[0].trim().toLowerCase().contains("tata")) && (!data1[0].trim().toLowerCase().contains("nextra")))
				        {
				          obj.setOrganisation(data1[0]);
				          obj.setCount(data1[1]);
				          obj.setChannelName(channel_name);
				          pubreport.add(obj);
				        }
				      }
				      //System.out.println(headers1);
				      //System.out.println(lines1);
				    }
				    return pubreport;
				  }
				  
				    
				  				  public List<PublisherReport> countNewUsersChannelLiveDatewise(String startdate, String enddate, String channel_name)
				  						    throws CsvExtractorException, Exception
				  						  {
				  							  
				  							  
				  						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
				  							  
				  						    
				  							  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
				  								      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
				  							  
				  							//	 CSVResult csvResult00 = getCsvResult(false, query00);
				  								// List<String> headers00 = csvResult00.getHeaders();
				  						//		 List<String> lines00 = csvResult00.getLines();
				  							//	 List<PublisherReport> pubreport00 = new ArrayList();  
				  								
				  								 
				  							//	System.out.println(headers00);
				  							//	System.out.println(lines00);  
				  								  
				  								//  for (int i = 0; i < lines00.size(); i++)
				  							    //  {
				  							       
				  							     //   String[] data = ((String)lines00.get(i)).split(",");
				  							  //      //System.out.println(data[0]);
				  							     
				  								  
				  								  
				  								  
				  							//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				  							  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
				  							    //  channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
				  							      CSVResult csvResult = getCsvResult(false, query00);
				  							      List<String> headers = csvResult.getHeaders();
				  							      List<String> lines = csvResult.getLines();
				  							      List<PublisherReport> pubreport = new ArrayList();
				  							//      System.out.println(headers);
				  							 //     System.out.println(lines);
				  							      Double count = 0.0;
				  							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				  							      for (int i = 0; i < lines.size(); i++)
				  							      {
				  							       
				  							        
				  							        String[] data = ((String)lines.get(i)).split(",");
				  							        if (Double.parseDouble(data[1].trim()) < 2.0)
				  							        {
				  							        count++;
				  							        
				  							        }
				  							        
				  							       }
				  							    }  
				  							
				  							      PublisherReport obj = new PublisherReport();
				  							      obj.setCount(count.toString());
				  							      obj.setChannelName(channel_name);
				  							      pubreport.add(obj);
				  							   
				  							      
				  						    return pubreport;
				  						  }
				  				  
				  				  
				  				  public List<PublisherReport> countReturningUsersChannelLiveDatewise(String startdate, String enddate, String channel_name)
				  						    throws CsvExtractorException, Exception
				  						  {
				  							  
				  							  
				  					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
				  						      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
				  					  
				  					//	 CSVResult csvResult00 = getCsvResult(false, query00);
				  						// List<String> headers00 = csvResult00.getHeaders();
				  				//		 List<String> lines00 = csvResult00.getLines();
				  					//	 List<PublisherReport> pubreport00 = new ArrayList();  
				  						
				  						 
				  					//	System.out.println(headers00);
				  					//	System.out.println(lines00);  
				  						  
				  						//  for (int i = 0; i < lines00.size(); i++)
				  					    //  {
				  					       
				  					     //   String[] data = ((String)lines00.get(i)).split(",");
				  					  //      //System.out.println(data[0]);
				  					     
				  						  
				  						  
				  						  
				  					//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				  					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
				  					    //  channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
				  					      CSVResult csvResult = getCsvResult(false, query00);
				  					      List<String> headers = csvResult.getHeaders();
				  					      List<String> lines = csvResult.getLines();
				  					      List<PublisherReport> pubreport = new ArrayList();
				  					   //   System.out.println(headers);
				  					   //   System.out.println(lines);
				  					      Double count = 0.0;
				  					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				  					      for (int i = 0; i < lines.size(); i++)
				  					      {
				  					       
				  					        
				  					        String[] data = ((String)lines.get(i)).split(",");
				  					        if (Double.parseDouble(data[1].trim()) >= 2.0)
				  					        {
				  					        count++;
				  					        
				  					        }
				  					        
				  					       }
				  					    }  
				  					
				  					      PublisherReport obj = new PublisherReport();
				  					      obj.setCount(count.toString());
				  					      obj.setChannelName(channel_name);
				  					      pubreport.add(obj);
				  					      
				  				          return pubreport;
				  						  }
				  				  
				  				  
				  				  
				  				  public List<PublisherReport> countLoyalUsersChannelLiveDatewise(String startdate, String enddate, String channel_name)
				  						    throws CsvExtractorException, Exception
				  						  {
				  					  String query00 = "SELECT COUNT(*)as count, cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
				  						      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
				  					  
				  					//	 CSVResult csvResult00 = getCsvResult(false, query00);
				  						// List<String> headers00 = csvResult00.getHeaders();
				  				//		 List<String> lines00 = csvResult00.getLines();
				  					//	 List<PublisherReport> pubreport00 = new ArrayList();  
				  						
				  						 
				  					//	System.out.println(headers00);
				  					//	System.out.println(lines00);  
				  						  
				  						//  for (int i = 0; i < lines00.size(); i++)
				  					    //  {
				  					       
				  					     //   String[] data = ((String)lines00.get(i)).split(",");
				  					  //      //System.out.println(data[0]);
				  					     
				  						  
				  						  
				  						  
				  					//	Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				  					  //  String query = "SELECT count(distinct(cookie_id))as reach,date FROM enhanceduserdatabeta1 where referrer like '%"+sectionname+"%' and channel_name = '" + 
				  					    //  channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " group by date";
				  					      CSVResult csvResult = getCsvResult(false, query00);
				  					      List<String> headers = csvResult.getHeaders();
				  					      List<String> lines = csvResult.getLines();
				  					      List<PublisherReport> pubreport = new ArrayList();
				  					 //     System.out.println(headers);
				  					 //     System.out.println(lines);
				  					      Double count = 0.0;
				  					      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				  					      for (int i = 0; i < lines.size(); i++)
				  					      {
				  					       
				  					        
				  					        String[] data = ((String)lines.get(i)).split(",");
				  					        if (Double.parseDouble(data[1].trim()) > 7.0)
				  					        {
				  					        count++;
				  					        
				  					        }
				  					        
				  					       }
				  					    }  
				  					
				  					      PublisherReport obj = new PublisherReport();
				  					      obj.setCount(count.toString());
				  					      obj.setChannelName(channel_name);
				  					      pubreport.add(obj);
				  					  
				  					      
				  				          return pubreport;
				  						  }			 
				  				 
				  				
				  				  
				  				  public List<PublisherReport> counttotalvisitorsChannelLive(String startdate, String enddate, String channel_name)
				  						    throws CsvExtractorException, Exception
				  						  {
				  							  
				  							  
				  						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
				  							  
				  						    
				  							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
				  								      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
				  							  
				  							//	 CSVResult csvResult00 = getCsvResult(false, query00);
				  								// List<String> headers00 = csvResult00.getHeaders();
				  						//		 List<String> lines00 = csvResult00.getLines();
				  							//	 List<PublisherReport> pubreport00 = new ArrayList();  
				  								
				  								 
				  							//	System.out.println(headers00);
				  							//	System.out.println(lines00);  
				  								  
				  								//  for (int i = 0; i < lines00.size(); i++)
				  							    //  {
				  							       
				  							     //   String[] data = ((String)lines00.get(i)).split(",");
				  							  //      //System.out.println(data[0]);
				  							     
				  								  
				  								  
				  								  
				  								Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				  							    String query = "SELECT count(*) as visits FROM enhanceduserdatabeta1 where channel_name = '" + 
				  							      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
				  							      CSVResult csvResult = getCsvResult(false, query);
				  							      List<String> headers = csvResult.getHeaders();
				  							      List<String> lines = csvResult.getLines();
				  							      List<PublisherReport> pubreport = new ArrayList();
				  							   //   System.out.println(headers);
				  							  //    System.out.println(lines);
				  							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				  							      for (int i = 0; i < lines.size(); i++)
				  							      {
				  							        PublisherReport obj = new PublisherReport();
				  							        
				  							        String[] data = ((String)lines.get(i)).split(",");
				  							       // obj.setDate(data[0]);
				  							        obj.setTotalvisits(data[0]);
				  							        obj.setChannelName(channel_name);
				  							        
				  							        pubreport.add(obj);
				  							      }
				  							    }  
				  							    
				  						    return pubreport;
				  						  }
				    
				  				  
				  				  public List<PublisherReport> countUniqueVisitorsChannelLive(String startdate, String enddate, String channel_name)
				  						    throws CsvExtractorException, Exception
				  						  {
				  							  
				  							  
				  						//	  System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
				  							  
				  						    
				  							  String query00 = "SELECT cookie_id FROM enhanceduserdatabeta1 where channel_name = '" + 
				  								      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" +"group by cookie_id limit 20000000";
				  							  
				  							//	 CSVResult csvResult00 = getCsvResult(false, query00);
				  								// List<String> headers00 = csvResult00.getHeaders();
				  						//		 List<String> lines00 = csvResult00.getLines();
				  							//	 List<PublisherReport> pubreport00 = new ArrayList();  
				  								
				  								 
				  							//	System.out.println(headers00);
				  							//	System.out.println(lines00);  
				  								  
				  								//  for (int i = 0; i < lines00.size(); i++)
				  							    //  {
				  							       
				  							     //   String[] data = ((String)lines00.get(i)).split(",");
				  							  //      //System.out.println(data[0]);
				  							     
				  								  
				  								  
				  								  
				  								Aggregations result = query(String.format("SELECT COUNT(*),brandName,browser_name FROM enhanceduserdatabeta1 group by brandName,browser_name", new Object[] { "enhanceduserprofilestore" }));
				  							    String query = "SELECT count(distinct(cookie_id))as reach FROM enhanceduserdatabeta1 where channel_name = '" + 
				  							      channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'";
				  							      CSVResult csvResult = getCsvResult(false, query);
				  							      List<String> headers = csvResult.getHeaders();
				  							      List<String> lines = csvResult.getLines();
				  							      List<PublisherReport> pubreport = new ArrayList();
				  							  //    System.out.println(headers);
				  							   //   System.out.println(lines);
				  							      if ((lines != null) && (!lines.isEmpty()) && (!((String)lines.get(0)).isEmpty())) {
				  							      for (int i = 0; i < lines.size(); i++)
				  							      {
				  							        PublisherReport obj = new PublisherReport();
				  							        
				  							        String[] data = ((String)lines.get(i)).split(",");
				  							       // obj.setDate(data[0]);
				  							        obj.setReach(data[0]);
				  							        obj.setChannelName(channel_name);
				  							        
				  							        pubreport.add(obj);
				  							      }
				  							    }  
				  							    
				  						    return pubreport;
				  						  }	  
								  	  
				  				public List<PublisherReport> getTopPostsbyTotalPageviewschannelLive(String startdate, String enddate, String channel_name)
				  					    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  					  {
				  					    String query1 = "Select count(*),referrer from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY referrer";
				  					    CSVResult csvResult1 = getCsvResult(false, query1);
				  					    List<String> headers1 = csvResult1.getHeaders();
				  					    List<String> lines1 = csvResult1.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  					    List<PublisherReport> pubreport = new ArrayList();
				  					    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				  					    {
				  					      for (int i = 0; i < lines1.size(); i++)
				  					      {
				  					        PublisherReport obj = new PublisherReport();
				  					        
				  					        String[] data1 = ((String)lines1.get(i)).split(",");
				  					          obj.setPublisher_pages(data1[0]);
				  					          obj.setCount(data1[1]);
				  					          obj.setChannelName(channel_name);
				  					          
				  					          pubreport.add(obj);
				  					        
				  					      }
				  					    //  System.out.println(headers1);
				  					    //  System.out.println(lines1);
				  					    }
				  					    return pubreport;
				  					  }
								  	  			  
				  				
				  				public List<PublisherReport> getTopPostsbyUniqueViewschannelLive(String startdate, String enddate, String channel_name)
				  					    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  					  {
				  					    String query1 = "Select count(distinct(cookies)),referrer from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY referrer";
				  					    CSVResult csvResult1 = getCsvResult(false, query1);
				  					    List<String> headers1 = csvResult1.getHeaders();
				  					    List<String> lines1 = csvResult1.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  					    List<PublisherReport> pubreport = new ArrayList();
				  					    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				  					    {
				  					      for (int i = 0; i < lines1.size(); i++)
				  					      {
				  					        PublisherReport obj = new PublisherReport();
				  					        
				  					        String[] data1 = ((String)lines1.get(i)).split(",");
				  					          obj.setPublisher_pages(data1[0]);
				  					          obj.setCount(data1[1]);
				  					          obj.setChannelName(channel_name);
				  					          
				  					          pubreport.add(obj);
				  					        
				  					      }
				  					    //  System.out.println(headers1);
				  					    //  System.out.println(lines1);
				  					    }
				  					    return pubreport;
				  					  }
								  	  			  
				  				  				
				
				  				public List<PublisherReport> getRefererPostsChannelLive(String startdate, String enddate, String channel_name)
				  					    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  					  {
				  					 String query1 = "Select count(*),referrer from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY referrer";
				  					    CSVResult csvResult1 = getCsvResult(false, query1);
				  					    List<String> headers1 = csvResult1.getHeaders();
				  					    List<String> lines1 = csvResult1.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  					    List<PublisherReport> pubreport = new ArrayList();
				  					    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				  					    {
				  					      for (int i = 0; i < lines1.size(); i++)
				  					      {
				  					        PublisherReport obj = new PublisherReport();
				  					        
				  					        String[] data1 = ((String)lines1.get(i)).split(",");
				  					        if ((data1[0].trim().toLowerCase().contains("facebook") || (data1[0].trim().toLowerCase().contains("google"))))
				  					        {
				  					          obj.setReferrerSource(data1[0]);
				  					          obj.setCount(data1[1]);
				  					          obj.setChannelName(channel_name);
				  					         
				  					          pubreport.add(obj);
				  					        }
				  					      }
				  					    //  System.out.println(headers1);
				  					    //  System.out.println(lines1);
				  					    }
				  					    return pubreport;
				  					  }
				  				
				  				public List<PublisherReport> getNewContentChannelLive(String startdate, String enddate, String channel_name)
				  					    throws SQLFeatureNotSupportedException, SqlParseException, CsvExtractorException, Exception
				  					  {
				  					    String query1 = "Select referrer from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time between " + "'" + startdate + "'" + " and " + "'" + enddate + "'" + " GROUP BY referrer";
				  					    CSVResult csvResult1 = getCsvResult(false, query1);
				  					    List<String> headers1 = csvResult1.getHeaders();
				  					    List<String> lines1 = csvResult1.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  				
				  					    String query2 = "Select referrer from enhanceduserdatabeta1 where channel_name = '" + channel_name + "' and request_time < " + "'" + startdate + "' GROUP BY referrer";
				  					    CSVResult csvResult2 = getCsvResult(false, query2);
				  					    List<String> headers2 = csvResult2.getHeaders();
				  					    List<String> lines2 = csvResult2.getLines();
				  					    System.out.println(headers1);
				  					      System.out.println(lines1);
				  				     
				  					     Set<String> list2 = new HashSet<String>();
				  					     list2.addAll(lines2);
				  					      
				  					     for(int i=0;i<lines1.size();i++){
				  					    	 
				  					    	 if(list2.contains(lines1.get(i))){
				  					    		 lines1.remove(i);
				  					    		 
				  					     }
				  					    	 
				  					   }
				  					     
				  					     
				  					      
				  					      List<PublisherReport> pubreport = new ArrayList();
				  					    if ((lines1 != null) && (!lines1.isEmpty()) && (!((String)lines1.get(0)).isEmpty()))
				  					    {
				  					      for (int i = 0; i < lines1.size(); i++)
				  					      {
				  					        PublisherReport obj = new PublisherReport();
				  					        
				  					        String data1 = (String)lines1.get(i);
				  					      
				  					          obj.setPublisher_pages(data1);
				  					          obj.setChannelName(channel_name);
				  					         
				  					          pubreport.add(obj);
				  					       
				  					      }
				  					    //  System.out.println(headers1);
				  					    //  System.out.println(lines1);
				  					    }
				  					    return pubreport;
				  					  }	
				  				
				  				
				  			  
				  				
				  				
				  				
  
  public static CSVResult getCsvResult(boolean flat, String query)
    throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException
  {
    return getCsvResult(flat, query, false, false);
  }
  
  public static CSVResult getCsvResult(boolean flat, String query, boolean includeScore, boolean includeType)
    throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException
  {
    SearchDao searchDao = getSearchDao();
    QueryAction queryAction = searchDao.explain(query);
    Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
    return new CSVResultsExtractor(includeScore, includeType).extractResults(execution, flat, ",");
  }
  
  public static void sumTest()
    throws IOException, SqlParseException, SQLFeatureNotSupportedException
  {}
  
  private static Aggregations query(String query)
    throws SqlParseException, SQLFeatureNotSupportedException
  {
    SqlElasticSearchRequestBuilder select = getSearchRequestBuilder(query);
    return ((SearchResponse)select.get()).getAggregations();
  }
  
  private static SqlElasticSearchRequestBuilder getSearchRequestBuilder(String query)
    throws SqlParseException, SQLFeatureNotSupportedException
  {
    SearchDao searchDao = getSearchDao();
    return (SqlElasticSearchRequestBuilder)searchDao.explain(query).explain();
  }
  
  private static InetSocketTransportAddress getTransportAddress()
  {
    String host = System.getenv("ES_TEST_HOST");
    String port = System.getenv("ES_TEST_PORT");
    if (host == null)
    {
      host = "172.16.101.132";
      //System.out.println("ES_TEST_HOST enviroment variable does not exist. choose default 'localhost'");
    }
    if (port == null)
    {
      port = "9300";
      //System.out.println("ES_TEST_PORT enviroment variable does not exist. choose default '9300'");
    }
    //System.out.println(String.format("Connection details: host: %s. port:%s.", new Object[] { host, port }));
    return new InetSocketTransportAddress(host, Integer.parseInt(port));
  }
}
