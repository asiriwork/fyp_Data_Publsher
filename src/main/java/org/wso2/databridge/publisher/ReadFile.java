package main.java.org.wso2.databridge.publisher;

import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.*;

import java.io.*;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

public class ReadFile extends Thread {

	BufferedReader reader;
	StringTokenizer st =  null;
	String[] PIDdata = new String[32];
	String line = null;
	
	public void read(){
		
		try {
			URL url = getClass().getResource("obd2Data.csv");
			reader = new BufferedReader(new FileReader(url.getPath()));
			line=reader.readLine();
		} catch (FileNotFoundException e) {
			System.out.println("Unable to read File " + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void run() {
		try {
            setTrustStoreParams();
            Thread.sleep(2000);

            //according to the convention the authentication port will be 7611+100= 7711 and its host will be the same
            DataPublisher dataPublisher = new DataPublisher("tcp://localhost:7613", "admin", "admin");
            String streamId1 = dataPublisher.defineStream("{" +
                    "  'name':'org.wso2.fyp.ObdTelematics'," +
                    "  'version':'2.3.1'," +
                    "  'nickName': 'Stock Quote Information'," +
                    "  'description': 'Some Desc'," +
                    "  'tags':['foo', 'bar']," +
                    "  'metaData':[" +
                    "          {'name':'ipAdd','type':'STRING'}" +
                    "  ]," +
                    "  'payloadData':[" +
                    "          {'name':'speed','type':'DOUBLE'}," +
                    "          {'name':'time','type':'LONG'}" +
                    "  ]" +
                    "}");
            //log.info("1st stream defined: "+streamId1);
//
			while((line=reader.readLine())!=null){
				st =  new StringTokenizer(line,",");
				
				for(int x = 0; x < 32; x++){
					
					PIDdata[x] = st.nextToken();
				}
                Double d = Double.valueOf(PIDdata[29]);
                Double speed = d*5/18;
                String time =  (PIDdata[1]);
                System.out.println("Speed: "+speed+" Time: "+time+ "speed in kmh: "+d);

                Date date = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS").parse(time);
                //System.out.println(date);
                long timeMili = date.getTime();
                System.out.println(date.getTime());





                dataPublisher.publish(streamId1, new Object[]{"127.0.0.1"}, null, new Object[]{speed, timeMili});
				Thread.sleep(1000);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (StreamDefinitionException e) {
            e.printStackTrace();
        } catch (AgentException e) {
            e.printStackTrace();
        } catch (DifferentStreamDefinitionAlreadyDefinedException e) {
            e.printStackTrace();
        } catch (TransportException e) {
            e.printStackTrace();
        } catch (AuthenticationException e) {
            e.printStackTrace();
        } catch (MalformedStreamDefinitionException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public static void setTrustStoreParams() {
        File filePath = new File("src/main/resources");
        if (!filePath.exists()) {
            filePath = new File("resources");
        }
        String trustStore = filePath.getAbsolutePath();
        System.setProperty("javax.net.ssl.trustStore", trustStore + "/client-truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

    }
	
	public static void main(String[] args){
		
		ReadFile readFile = new ReadFile();
		readFile.read();
		readFile.start();
	}
}
