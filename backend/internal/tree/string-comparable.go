package tree

type StringComparable struct {
	Value string
}

func (s StringComparable) Compare(other interface{}) int {
	otherStr, ok := other.(StringComparable)
	if !ok {
		panic("Cannot compare with non-StringComparable type")
	}

	if s.Value < otherStr.Value {
		return -1
	} else if s.Value > otherStr.Value {
		return 1
	}
	return 0
}

func (s StringComparable) Length() int {
	return len(s.Value)
}
