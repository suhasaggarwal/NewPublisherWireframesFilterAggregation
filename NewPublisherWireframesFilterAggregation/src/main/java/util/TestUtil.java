package util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TestUtil {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String starttimestamp= "";
		String endtimestamp="";
		String dateRange="";
		DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
        endtimestamp=sdf.format(cal.getTime()).toString();
		
		cal.add(Calendar.MINUTE, -15);
		starttimestamp = sdf.format(cal.getTime()).toString();
        dateRange=starttimestamp+","+endtimestamp;
		System.out.println(dateRange);
		
		
	}

}
