import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by shashi on 28/03/16.
 */
public class test {

    public static void main(String[] args) {

        String startDateString = "06/27/2007";
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
        final long teamWindowDurationMs=20000;
        Date startDate;
        try {
            startDate = df.parse(startDateString);
            String newDateString = df.format(startDate);
            System.out.println(newDateString);
            System.out.println(startDate.getTime());
            long timestampinlong =startDate.getTime();
            System.out.println((timestampinlong / teamWindowDurationMs) * teamWindowDurationMs);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    }
