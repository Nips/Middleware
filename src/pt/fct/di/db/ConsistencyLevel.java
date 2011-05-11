package pt.fct.di.db;

/**
 * ConsistencyLevel is an enum that controls the write and read behavior based on the number of servers indicated in properties.
 * Different levels of consistency have different meanings depending if a client is doing is a write or a read operation. For a Read
 * operation we define R as the number of servers that must participate in a successful read operation. And for a write we denote with
 * a W. To achieve strongly consistent behavior W and R must be set such that W+R > Number_of_servers, that is the client always sees the
 * most recently write. Of these, the most interesting is to do QUORUM reads and writes, which gives you consistency while still allowing 
 * availability in the face of node failures up to half of #servers. Of course if latency is more important than consistency then you can
 * use lower values for either or both.
 * 
 * Write:
 *      ZERO    Ensure nothing. A write happens asynchronously in background
 *      ANY     Ensure that the write has been written once somewhere.
 *      ONE     Ensure that the write has been written to at least 1 servers's commit log and persistently stored before responding 
 *      		to the client.
 *      QUORUM  Ensure that the write has been written to #Servers / 2 + 1 servers before responding to the client.
 *      ALL     Ensure that the write is written to all servers before responding to the client.
 * 
 * Read:
 *      ZERO    Not supported, because it doesn't make sense.
 *      ANY     Not supported. You probably want ONE instead.
 *      ONE     Will return the record returned by the first server to respond.
 *      QUORUM  Will query all servers and return the record with the most recent timestamp once it has at least a
 *              majority of replicas reported.
 *      ALL     Queries all servers and returns the record with the most recent timestamp.
 *  
 * @author andre_goncalves@di
 *
 */

public enum ConsistencyLevel 
{
	ZERO(0),
	ONE(1),
	QUORUM(2),
	ALL(5),
	ANY(6);

	private final int _value;
	
	private ConsistencyLevel(int value)
	{
		this._value = value;
	}
	
	/**
	 * Get the integer value of this enum value.
	 */
	public int getValue()
	{
		return this._value;
	}
	
	/**
	 * Find a the enum type by its integer value, as defined in the Thrift IDL.
	 * @return null if the value is not found.
	 */
	public static ConsistencyLevel findByValue(int value) { 
		switch (value) {
			case 0:
				return ZERO;
			case 1:
				return ONE;
			case 2:
				return QUORUM;
			case 5:
				return ALL;
			case 6:
				return ANY;
		default:
			return null;
		}
	}
}
