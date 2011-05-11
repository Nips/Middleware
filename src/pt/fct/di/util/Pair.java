package pt.fct.di.util;

public class Pair<K,V> {

	private K _value1;
	private V _value2;
	
	public Pair(K val1, V val2)
	{
		this._value1 = val1;
		this._value2 = val2;
	}

	public K get_value1() {
		return _value1;
	}

	public V get_value2() {
		return _value2;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_value1 == null) ? 0 : _value1.hashCode());
		result = prime * result + ((_value2 == null) ? 0 : _value2.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		Pair other = (Pair) obj;
		if (_value1 == null) {
			if (other._value1 != null)
				return false;
		} else if (!_value1.equals(other._value1))
			return false;
		if (_value2 == null) {
			if (other._value2 != null)
				return false;
		} else if (!_value2.equals(other._value2))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Pair [_value1=" + _value1 + ", _value2=" + _value2 + "]";
	}
}
