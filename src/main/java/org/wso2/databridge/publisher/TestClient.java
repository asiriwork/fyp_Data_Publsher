/*
*  Copyright (c) 2005-2012, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package main.java.org.wso2.databridge.publisher;

import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.AuthenticationException;
import org.wso2.carbon.databridge.commons.exception.DifferentStreamDefinitionAlreadyDefinedException;
import org.wso2.carbon.databridge.commons.exception.MalformedStreamDefinitionException;
import org.wso2.carbon.databridge.commons.exception.StreamDefinitionException;
import org.wso2.carbon.databridge.commons.exception.TransportException;
import org.wso2.carbon.databridge.commons.exception.UndefinedEventTypeException;

import java.io.File;
import java.net.MalformedURLException;
import java.util.logging.Logger;

public class TestClient {
    Logger log = Logger.getLogger("org.wso2.drawbridge.publisher");

    public static void main(String[] args)
            throws UndefinedEventTypeException, AgentException,
                   MalformedURLException, AuthenticationException,
                   MalformedStreamDefinitionException, StreamDefinitionException,
                   TransportException, InterruptedException,
                   DifferentStreamDefinitionAlreadyDefinedException {

        TestClient testClient = new TestClient();
        testClient.testSendingEvent();
    }


    public void testSendingEvent()
            throws MalformedURLException, AuthenticationException, TransportException,
                   AgentException, UndefinedEventTypeException,
                   DifferentStreamDefinitionAlreadyDefinedException,
                   InterruptedException,
                   MalformedStreamDefinitionException,
                   StreamDefinitionException {

        setTrustStoreParams();
        Thread.sleep(2000);

        //according to the convention the authentication port will be 7611+100= 7711 and its host will be the same
        DataPublisher dataPublisher = new DataPublisher("tcp://localhost:7611", "admin", "admin");
        String streamId1 = dataPublisher.defineStream("{" +
                                                      "  'name':'org.wso2.esb.MediatorStatistics'," +
                                                      "  'version':'2.3.0'," +
                                                      "  'nickName': 'Stock Quote Information'," +
                                                      "  'description': 'Some Desc'," +
                                                      "  'tags':['foo', 'bar']," +
                                                      "  'metaData':[" +
                                                      "          {'name':'ipAdd','type':'STRING'}" +
                                                      "  ]," +
                                                      "  'payloadData':[" +
                                                      "          {'name':'symbol','type':'STRING'}," +
                                                      "          {'name':'price','type':'DOUBLE'}," +
                                                      "          {'name':'volume','type':'INT'}," +
                                                      "          {'name':'max','type':'DOUBLE'}," +
                                                      "          {'name':'min','type':'Double'}" +
                                                      "  ]" +
                                                      "}");
        log.info("1st stream defined: "+streamId1);

        //In this case correlation data is null
        dataPublisher.publish(streamId1, new Object[]{"127.0.0.1"}, null, new Object[]{"IBM", 102.8, 1000, 120.6, 70.4});
        log.info("Event published to 1st stream");

        //NOTE the version of this stream is different to the 1st stream
        String streamId2 = dataPublisher.defineStream("{" +
                                                      "  'name':'org.wso2.esb.MediatorStatistics'," +
                                                      "  'version':'2.4.0'," +                              //Different version
                                                      "  'nickName': 'Stock Quote Information'," +
                                                      "  'description': 'Some Desc'," +
                                                      "  'tags':['foo', 'bar']," +
                                                      "  'metaData':[" +
                                                      "          {'name':'ipAdd','type':'STRING'}" +
                                                      "  ]," +
                                                      "  'correlationData':[" +
                                                      "          {'name':'correlationId','type':'STRING'}" +
                                                      "  ]," +
                                                      "  'payloadData':[" +
                                                      "          {'name':'symbol','type':'STRING'}," +
                                                      "          {'name':'price','type':'DOUBLE'}," +
                                                      "          {'name':'volume','type':'INT'}," +
                                                      "          {'name':'max','type':'DOUBLE'}," +
                                                      "          {'name':'min','type':'Double'}" +
                                                      "  ]" +
                                                      "}");

        log.info("2nd stream defined: "+streamId1);

        //In this case correlation data is null

        dataPublisher.publish(streamId2, new Object[]{"127.0.0.1"}, new Object[]{null}, new Object[]{"IBM", 97.8, 500, 120.6, 70.4});
        log.info("Event published to 2nd stream");

        dataPublisher.publish(streamId2, new Object[]{"127.0.0.1"}, new Object[]{"HD34"}, new Object[]{"IBM", 96.8, 300, 120.6, 70.4});
        log.info("Event published to 2nd stream");

        dataPublisher.publish(streamId2, new Object[]{"127.0.0.1"}, new Object[]{"HD36"}, new Object[]{"IBM", 89.3, 400, 120.6, 70.4});
        log.info("Event published to 2nd stream");

        dataPublisher.publish(streamId1, new Object[]{"127.0.0.1"}, null, new Object[]{"IBM", 100.8, 1100, 120.6, 70.4});
        log.info("Event published to 2nd stream");

        Thread.sleep(3000);
        dataPublisher.stop();
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

}
