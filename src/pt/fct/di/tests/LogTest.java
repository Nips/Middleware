package pt.fct.di.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.StringTokenizer;

import pt.fct.di.db.ConsistencyLevel;
import pt.fct.di.ops.Delete;
import pt.fct.di.ops.Put;
import pt.fct.di.serverProxy.service.ServerService;
import pt.fct.di.serverProxy.service.ServiceException;

public class LogTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			FileReader fis = new FileReader(new File("testSection/testLog.txt"));
			BufferedReader br = new BufferedReader(fis);
			
			Properties p = new Properties();
			p.setProperty("remoteValues", "true");
			p.setProperty("syncWaitingTime", "500");
			p.setProperty("dbname", "pt.fct.di.db.MongoDB");
			p.setProperty("hosts", "127.0.0.1:27017");
			p.setProperty("mongodb.writeConcern", "normal");
			ServerService ss = new ServerService();
			ss.setProperties(p);
			ss.init();
			long[] versionVector = new long[10];
			
			String line = "";
			StringTokenizer st = null;
			while((line = br.readLine()) != null)
			{
				st = new StringTokenizer(line);
				String[] command = new String[st.countTokens()];
				int elem = 0;
				while(st.hasMoreTokens())
				{
					command[elem] = st.nextToken();
					System.out.print(command[elem]+" ");
					elem++;
				}
				System.out.println("\n");
				
				if(command[0].equals("put"))
				{
					HashMap<String, String> fields = new HashMap<String, String>();
					for(int i=5; i<command.length-1; i+=2)
					{
						fields.put(command[i], command[i+1]);
						//System.out.println(fields.get(command[i]));
					}
					
					Put op = new Put(Integer.parseInt(command[1]), Integer.parseInt(command[2]),
							command[3], command[4], fields, ConsistencyLevel.ONE);
//					op.setID(Integer.parseInt(command[1]));
//					op.setOpSeq(Integer.parseInt(command[2]));
//					op.setColumnFamily(command[3]);
//					op.setRowKey(command[4]);
//					op.setValues(fields);
					versionVector[op.getID()] = Long.parseLong(command[command.length-1]);
					op.setVersionVector(versionVector);
					
					ss.orderTransformExecute(op);
					ss.logToString();
					
				}
				else if(command[0].equals("delete"))
				{
					HashSet<String> fields = new HashSet<String>();
					for(int i=5; i<command.length-1; i++)
					{
						fields.add(command[i]);
						//System.out.println("field: "+command[i]+" exists "+fields.contains(command[i]));
					}
					
					Delete op = new Delete(Integer.parseInt(command[1]), Integer.parseInt(command[2]),
							command[3], command[4], fields, ConsistencyLevel.ONE);
//					op.setID(Integer.parseInt(command[1]));
//					op.setOpSeq(Integer.parseInt(command[2]));
//					op.setColumnFamily(command[3]);
//					op.setRowKey(command[4]);
//					op.setFields(fields);
					versionVector[op.getID()] = Long.parseLong(command[command.length-1]);
					op.setVersionVector(versionVector);
					
					ss.orderTransformExecute(op);
					ss.logToString();
				}
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
