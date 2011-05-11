package pt.fct.di.tests;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import pt.fct.di.serverProxy.sync.VersionVector;
import pt.fct.di.util.Pair;

public class VersionVectorTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		VersionVector vv = new VersionVector();
		
		/***************TestSection***************/
		int client1 = 1;
		int client2 = 2;
		int client4 = 4;
		int client7 = 7;
		
		vv.setClientVersion(client1);
		vv.setClientVersion(client2);
		vv.setClientVersion(client4);
		vv.setClientVersion(client2);
		
		for(int i=0; i<10; i++) System.out.println("Client "+i+": "+vv.getClientVersion(i));
		System.out.println("\n");
		
		vv.setClientVersion(client7);
		vv.setClientVersion(client7);
		vv.setClientVersion(client7);
		
		for(int i=0; i<10; i++) System.out.println("Client "+i+": "+vv.getClientVersion(i));
		System.out.println("\n");
		
		long[] otherVector = {0,1,2,0,1,0,0,3,0,0,0};
		System.out.println("Updated? "+vv.updated(otherVector)); //true
		
		long[] otherVector2 = {0,1,1,0,1,0,0,2,0,0,0};
		System.out.println("Updated? "+vv.updated(otherVector2)); //true
		
		long[] otherVector3 = {0,1,1,0,1,0,0,4,0,0,0};
		System.out.println("Updated? "+vv.updated(otherVector3)); //true

		long[] otherVector4 = {0,1,1,0,1,0,0,5,0,0,0};
		System.out.println("Updated? "+vv.updated(otherVector4)); //false
		
		long[] otherVector5 = {0,1,1,0,1,0,0,5,0,0,0};
		List<Pair<Integer, Long>> missedUpdates = new LinkedList<Pair<Integer,Long>>();
		boolean result = vv.compare(otherVector5, missedUpdates);
		System.out.println("Updated? "+result); //false
		if(!result)
		{
			Iterator<Pair<Integer, Long>> it = missedUpdates.iterator();
			System.out.print("Missed: ");
			while(it.hasNext())
				System.out.print(it.next().toString()+", ");
			System.out.println("\n");
		}
		
		long[] otherVector6 = {0,1,1,0,4,0,0,5,0,3,0};
		List<Pair<Integer, Long>> missedUpdates2 = new LinkedList<Pair<Integer,Long>>();
		boolean result2 = vv.compare(otherVector6, missedUpdates2);
		System.out.println("Updated? "+result2); //false
		if(!result2)
		{
			Iterator<Pair<Integer, Long>> it = missedUpdates2.iterator();
			System.out.print("Missed: ");
			while(it.hasNext())
				System.out.print(it.next().toString()+", ");
			System.out.println("\n");
		}
	}

	//All tests passed ;)
}
