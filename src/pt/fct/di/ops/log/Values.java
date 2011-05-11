package pt.fct.di.ops.log;

import java.util.Map;
import java.util.Set;

import pt.fct.di.db.ConsistencyLevel;
import pt.fct.di.db.DB;

public class Values {

	private Map<String,String> _localValues;
	private DB _database;
	
	public Values(DB database)
	{
		_database = database;
		_localValues = null;
	}
	
	public Values(Map<String,String> values)
	{
		_localValues = values;
		_database = null;
	}
	
	public void setLocalValues(Map<String,String> newValues)
	{
		if(isLocalValuesSet()) _localValues = newValues;	
	}
	
	public Map<String,String> getValues(String column_family, String key, Set<String> fields)
	{
		if(_localValues != null) return _localValues;
		return _database.read(column_family, key, fields, ConsistencyLevel.ONE).getValues();
	}
	
	public boolean isLocalValuesSet()
	{
		return _localValues != null;
	}
	
	public boolean isDatabaseSet()
	{
		return _database != null;
	}
}
