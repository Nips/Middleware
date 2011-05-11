package pt.fct.di.ops;

import java.util.Map;
import java.util.Set;

public class FieldsOrValues {

	private Set<String> _fields;
	private Map<String, String> _values;
	
	public FieldsOrValues(){ }
	
	public FieldsOrValues(Set<String> fields)
	{
		_fields = fields;
		_values = null;
	}
	
	public FieldsOrValues(Map<String,String> values)
	{
		_values = values;
		_fields = null;
	}
	
	public void setFields(Set<String> fields)
	{
		_fields = fields;
	}
	
	public Set<String> getFields()
	{
		return _fields;
	}
	
	public void unsetFields()
	{
		_fields = null;
	}
	
	public boolean isSetFields()
	{
		return _fields != null;
	}
	
	public void setFieldsIsSet(boolean value) 
	{
		if (!value) this._fields = null;
	}
	
	public void setValues(Map<String,String> values)
	{
		_values = values;
	}
	
	public Map<String,String> getValues()
	{
		return _values;
	}
	
	public void unsetValues()
	{
		_values = null;
	}
	
	public boolean isSetValues()
	{
		return _values != null;
	}
	
	public void setValuesIsSet(boolean value) 
	{
		if (!value) this._values = null;
	}
	
	public int getSize()
	{
		if(isSetFields()) return _fields.size();
		else return _values.size();
	}
	
	
}
