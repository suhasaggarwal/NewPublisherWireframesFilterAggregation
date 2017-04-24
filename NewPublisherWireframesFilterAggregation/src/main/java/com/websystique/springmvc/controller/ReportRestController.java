package com.websystique.springmvc.controller;

import java.net.URLDecoder;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import util.EncryptionModule;

import com.publisherdata.Daos.AggregationModule;
import com.publisherdata.model.PublisherReport;
import com.publisherdata.model.User;
import com.websystique.springmvc.model.Reports;
import com.websystique.springmvc.service.ReportService;

@RestController
public class ReportRestController {

	@Autowired
	ReportService reportService;  //Service which will do all data retrieval/manipulation work

	//-------------------Retrieve Report with Id--------------------------------------------------------
/*	
	@RequestMapping(value = "/report/{id}/{dateRange}/{channel_name}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReport(@PathVariable("id") long id,@PathVariable("dateRange") String dateRange,@PathVariable("channel_name") String channel_name){
		System.out.println("Fetching Report with id " + id);
		List<PublisherReport> report = reportService.extractReportsChannelNames(id,dateRange,channel_name);
		if (report == null) {
			System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
	}
*/
	
	@RequestMapping(value = "/report/{id}/{dateRange}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReport(@PathVariable("id") long id,@PathVariable("dateRange") String dateRange, HttpServletRequest request) {
	
		 List<PublisherReport> report = null;
		
		 String token;	
	     String userdetails;
	     String [] userinfo;
	     String emailId = null;
	     String status = null;
		 User user = new User();
	     Cookie[] cookies = request.getCookies();
	            if(cookies != null){
	     for(int i = 0; i < cookies.length ; i++){
	            if(cookies[i].getName().equals("AUTHTOKEN")){
           //Fetch User details and return in json format
	              token = cookies[i].getValue();
	              userdetails=EncryptionModule.decrypt(null, null,URLDecoder.decode(token));
	              userinfo = userdetails.split(":");
                emailId = userinfo[0];
	            }
	        }
	     
	      }
		
		
	            if(emailId.equalsIgnoreCase("vinay.rajput@cuberoot.co"))
	    		{
	            	
	            	
	            	System.out.println("Fetching Report with id " + id);
			        report =  reportService.extractReportsChannel(id,dateRange,"Adda52");
			        if (report == null){
			        System.out.println("Report with id " + id + " not found");
				    return new ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND); 	
			        }
	            	
	            	
	    		}
	            else{
		    
	            	
	            System.out.println("Fetching Report with id " + id);
		        report =  reportService.extractReports(id,dateRange);
		        if (report == null){
		        System.out.println("Report with id " + id + " not found");
			    return new ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND);
	     	  
		        }
	          }      
		
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
}
	
	@RequestMapping(value = "/report/{id}/{dateRange}/{channel}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReport(@PathVariable("id") long id,@PathVariable("dateRange") String dateRange,@PathVariable("channel") String channel_name) {
		System.out.println("Fetching Report with id " + id);
		List<PublisherReport> report =  reportService.extractReportsChannel(id,dateRange,channel_name);
		if (report == null){
		    System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
}
	
	@RequestMapping(value = "/report/{id}/{dateRange}/channelList", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Set<String>> getList(@PathVariable("id") long id,@PathVariable("dateRange") String dateRange,HttpServletRequest request) {
		
		
		 String token;	
	     String userdetails;
	     String [] userinfo;
	     String emailId = null;
	     String status = null;
		 User user = new User();
	     Cookie[] cookies = request.getCookies();
	            if(cookies != null){
	     for(int i = 0; i < cookies.length ; i++){
	            if(cookies[i].getName().equals("AUTHTOKEN")){
            //Fetch User details and return in json format
	              token = cookies[i].getValue();
	              userdetails=EncryptionModule.decrypt(null, null,URLDecoder.decode(token));
	              userinfo = userdetails.split(":");
                 emailId = userinfo[0];
	            }
	        }
	     
	      }
		
		
		
		
		
		System.out.println("Fetching Report with id " + id);
		Set<String> list =  reportService.extractChannelNames(id,dateRange);
		if (list == null){
		    System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<Set<String>>(HttpStatus.NOT_FOUND);
		}
		
		if(emailId.equalsIgnoreCase("vinay.rajput@cuberoot.co"))
		{
			list.clear();
			list.add("momagic");
			list.add("opera");
			list.add("cricbuzz");
			list.add("cricbuzz_mob");
			list.add("taboola");
			list.add("forkmedia");
			list.add("inuxu_native");
			list.add("ixigo");
			list.add("spidio");
			list.add("shopclues");
			list.add("espn");
			list.add("gamooga");
			
			
		}
		
		return new ResponseEntity<Set<String>>(list, HttpStatus.OK);
}	


/*

	@RequestMapping(value = "/report/v1/{id}/ArticleAPIs/{dateRange}/{channel}/{articlename}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReportArticle(@PathVariable("id") long id,@PathVariable("dateRange") String dateRange,@PathVariable("channel") String channel_name,@PathVariable("articlename") String articlename) {
		System.out.println("Fetching Report with id " + id);
		List<PublisherReport> report =  reportService.extractReportsChannelArticle(id,dateRange,channel_name,articlename);
		if (report == null){
		    System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
}
*/

	@RequestMapping(value = "/report/v1/{QueryField}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReportqueryField(@PathVariable("QueryField") String queryfield,@RequestParam("dateRange") String dateRange,@RequestParam("siteId") String siteId,@RequestParam("sectionid") String sectionId,@RequestParam("articleid") String articleid,@RequestParam("city") String city,@RequestParam("gender") String gender,@RequestParam("agegroup") String agegroup,@RequestParam("audience_segment") String audience_segment,@RequestParam("income_level") String income_level,@RequestParam("device_type")String device_type,@RequestParam("Aggregateby")String group_by) {
	

    AggregationModule module =  AggregationModule.getInstance();
    try {
		module.setUp();
	} catch (Exception e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
    List<PublisherReport> report =null;
    Map<String,String>FilterMap = new HashMap<String,String>();
    List<String> groupby1 = new ArrayList<String>();	
    String[] groupbyparts = group_by.split(",");
    for(int i=0; i < groupbyparts.length; i++){
    	
    	groupby1.add(groupbyparts[i]);
    	
    }
	//if((dateRange !=null && dateRange.isEmpty()==false)&& (siteId !=null && siteId.isEmpty()==false) && ((city !=null && city.isEmpty()==false) || (gender !=null && gender.isEmpty()==false) || (agegroup !=null && agegroup.isEmpty()==false) || (audience_segment !=null && audience_segment.isEmpty()==false) ||  (income_level !=null && income_level.isEmpty()==false) || (device_type !=null && device_type.isEmpty()==false))  )
	
    if(city !=null && city.isEmpty()==false) 
        FilterMap.put("city", city);

		if(gender !=null && gender.isEmpty()==false) 
          FilterMap.put("gender",gender);

			if(agegroup !=null && agegroup.isEmpty()==false) 
             FilterMap.put("agegroup", agegroup);

				if(audience_segment !=null && audience_segment.isEmpty()==false) 
                     FilterMap.put("audience_segment", audience_segment);

					if(income_level !=null && income_level.isEmpty()==false) 
                        FilterMap.put("income_level", income_level);	


					   if(device_type !=null && device_type.isEmpty()==false) 
                            FilterMap.put("device_type", device_type);
    

					   
					   
					   
      
    if(dateRange !=null && dateRange.isEmpty()==false )
	{
		
		if(siteId !=null && siteId.isEmpty()==false){
	
	         
	              if(FilterMap.size() == 0 ){
	            	 
	            	  if(group_by==null || group_by.isEmpty())
	            	  {
	            		  
	            		  if(sectionId==null||sectionId.isEmpty())
	            		  {  
	            			  if(articleid==null||articleid.isEmpty())
	            			  {
	         	            	 
	            	  String [] dateInterval = dateRange.split(",");
	            	  
	            	  try {
						report =  module.getQueryFieldChannel(queryfield,dateInterval[0], dateInterval[1], "Womanseraindia_indiagate");
					} catch (SQLFeatureNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SqlParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (CsvExtractorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            	  
	            	  return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);  
	            			  
	            			  }
	            			  
	            		  }
	            		  
	            	  }
	            	  
	            	  
	              }
	
	              
                  if(FilterMap.size() == 0 ){
	            	  
	            	  
                	  if(group_by==null || group_by.isEmpty()){
                		  
                		if(sectionId!=null||sectionId.isEmpty()==false)  {
                		    
                		  
                	  
                	  
                	  String [] dateInterval = dateRange.split(",");
	            	  
	            	  try {
						report =  module.getQueryFieldChannelSection(queryfield,dateInterval[0], dateInterval[1], "Womanseraindia_indiagate",sectionId);
					} catch (SQLFeatureNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SqlParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (CsvExtractorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            	  

              		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
                		
                		}
                		
                	  }
	            	  
	            	  
	            	  
	              }
	              
                  
                  
                  
                   if(FilterMap.size() > 0 ){
	            	  
                	   
                	   if(group_by==null || group_by.isEmpty()){
                		   
                		   
                		 if(sectionId.trim()!=null && sectionId.trim().isEmpty()==false){
                	   
                	   
	            	  String [] dateInterval = dateRange.split(",");
	            	  
	            	  try {
						report =  module.getQueryFieldChannelSectionFilter(queryfield,dateInterval[0], dateInterval[1], "Womanseraindia_indiagate",sectionId,FilterMap);
					} catch (SQLFeatureNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SqlParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (CsvExtractorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            	
                		
	            	  return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK); 
                		 }
                		  
                		 
                	   } 
	            	  
	            
	            	
	              }
                  
                  
                  
                  
                  
                  
                  
                  
                  
                    
                if(FilterMap.size() == 0  ){
	            	  
	            	
                	if(group_by==null || group_by.isEmpty()){
                		
                		
                		
                	  if(articleid!=null && articleid.isEmpty()==false) {
                	
                	
                	String [] dateInterval = dateRange.split(",");
	            	  
	            	  try {
						report =  module.getQueryFieldChannelArticle(queryfield,dateInterval[0], dateInterval[1], "Womanseraindia_indiagate",articleid);
					} catch (SQLFeatureNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SqlParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (CsvExtractorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            	  
	            	  return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
                	  
                	  }
                	  
                	
                	
                	}
	            	  
	            	  
	            	 
	              }
                  
                  
                if(FilterMap.size() > 0 ){
	            	  
	            	 
                	if(group_by==null || group_by.isEmpty()){
                		
                		
                	if(articleid!=null && articleid.isEmpty()==false){
                		
                	
                	String [] dateInterval = dateRange.split(",");
	            	  
	            	  try {
						report =  module.getQueryFieldChannelArticleFilter(queryfield,dateInterval[0], dateInterval[1], "Womanseraindia_indiagate",articleid,FilterMap);
					} catch (SQLFeatureNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SqlParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (CsvExtractorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            	  
                
	            		
	                	return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
                	}
                
                	}	  
	            	  
	            	  
	              }
                  
                  
                  
                  
                  
                  
                  
                  
                  if(FilterMap.size() == 0){
	            	 
                	  if(group_by==null || group_by.isEmpty()) {
                		  
                		  
                	  if(sectionId==null||sectionId.isEmpty()) {
                		  
                		
                	   if(articleid==null||articleid.isEmpty()){
                		  
                		  
                	  
	            	  String [] dateInterval = dateRange.split(",");
	            	  
	            	  try {
						report =  module.getQueryFieldChannel(queryfield,dateInterval[0], dateInterval[1], "Womanseraindia_indiagate");
					} catch (SQLFeatureNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SqlParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (CsvExtractorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            	  
	            	  return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
	            	  
                	   }
                	   
                	  }
                	  
                	
                	  
                	  }
	              }
	              

	              if(FilterMap.size() > 0 ){
	            	
	            	  
	            	  if(sectionId==null||sectionId.isEmpty())
	            		  { 
	            		 
	            	  if(articleid==null||articleid.isEmpty())
	            	  {
	            		  
	            		  
	            	  
	            	  String [] dateInterval = dateRange.split(",");
	            	  
	            	  try {
						report =  module.getQueryFieldChannelFilter(queryfield,dateInterval[0], dateInterval[1], "Womanseraindia_indiagate",FilterMap);
					} catch (SQLFeatureNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SqlParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (CsvExtractorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            	  return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
	            	  
	            	  }
	            	  
	            	 
	            		  
	            		  
	            		  }
	            	  
	            	 
	            	 
	              }
	
	
                  if(group_by!=null && group_by.isEmpty()==false){
	            	  
	            	 if(sectionId==null||sectionId.isEmpty())
	            		 {
	            		 
	            		 if (articleid==null||articleid.isEmpty())
	            		 {
                	  
                	  
                	  String [] dateInterval = dateRange.split(",");
	            	  
	            	  try {
						report =  module.getQueryFieldChannelGroupBy(queryfield,dateInterval[0], dateInterval[1], "Womanseraindia_indiagate",groupby1);
					} catch (SQLFeatureNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SqlParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (CsvExtractorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            	  
	            		
	            	  return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK); 
	            		 
	            		 
	            		 }
	            		 
	            		
		            	 
	            		 
	            		 }	 
	            	 
               }	
	
	
	
                if(group_by!=null && group_by.isEmpty()==false ){
	            	  
	            	 
                	if(sectionId!=null && sectionId.isEmpty()==false){
                		
                
                	   if(articleid==null||articleid.isEmpty()){
                	
                	String [] dateInterval = dateRange.split(",");
	            	  
	            	  try {
						report =  module.getQueryFieldChannelSectionGroupBy(queryfield,dateInterval[0], dateInterval[1], "Womanseraindia_indiagate",sectionId,groupby1);
					} catch (SQLFeatureNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SqlParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (CsvExtractorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                	  
	           	   return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
               	 
                	   
                	   
                	   }
                	   
                
                	} 
	            	 
               }	
	
	 
                if(group_by!=null && group_by.isEmpty()==false){
	            	  
	            	 
                	if(sectionId==null||sectionId.isEmpty())
                	{
                	
                		if(articleid!=null && articleid.isEmpty()==false){
                			
                		
                	String [] dateInterval = dateRange.split(",");
	            	  
	            	  try {
						report =  module.getQueryFieldChannelArticleGroupBy(queryfield,dateInterval[0], dateInterval[1], "Womanseraindia_indiagate",articleid,groupby1);
					} catch (SQLFeatureNotSupportedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SqlParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (CsvExtractorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            	  
                	
	            	  return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);	
                		
                		
                		}
                		
                		
                	}	
	            	  
	            	 
             }	
	
	
	
	
	
	
	
	
		}
	
	
	
	}
	
    if (report == null){
	  
		return new ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND);
	}
    
    return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
	}
	             


	@RequestMapping(value = "/report/{id}/SectionAPIs/{dateRange}/{channel}/{sectionname}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReportSection(@PathVariable("id") long id,@PathVariable("dateRange") String dateRange,@PathVariable("channel") String channel_name,@PathVariable("sectionname") String sectionname) {
		System.out.println("Fetching Report with id " + id);
		List<PublisherReport> report =  reportService.extractReportsChannelSection(id,dateRange,channel_name,sectionname);
		if (report == null){
		    System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
}



	@RequestMapping(value = "/report/{id}/LiveAPIs/{channel}/{timeduration}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReportLiveData(@PathVariable("id") long id,@PathVariable("channel") String channel_name,@PathVariable("timeduration") String timeduration) {
		System.out.println("Fetching Report with id " + id);
		
		String starttimestamp="";
		String endtimestamp="";
		String dateRange = "";
		if(timeduration.equals("Last_24_Hours"))
		{
			
			
			DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();
	        endtimestamp=sdf.format(cal.getTime()).toString();
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.HOUR_OF_DAY, -24);
			starttimestamp = sdf.format(calendar.getTime()).toString();
	        
	        dateRange=starttimestamp+","+endtimestamp;
		    dateRange = dateRange.replace("/", "-");
		}
			
		if(timeduration.equals("Last_15_minutes"))
		{
			DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();
	        endtimestamp=sdf.format(cal.getTime()).toString();
			
			cal.add(Calendar.MINUTE, -15);
			starttimestamp = sdf.format(cal.getTime()).toString();
	        dateRange=starttimestamp+","+endtimestamp;
	        dateRange = dateRange.replace("/", "-");
			
	    }
				
				
		List<PublisherReport> report =  reportService.extractReportsChannelLive(id,dateRange,channel_name);
		if (report == null){
		    System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
}





}
