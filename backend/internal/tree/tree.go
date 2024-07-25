package tree

type Comparable interface {
	Compare(other interface{}) int
}

type Tree interface {
	Insert(key Comparable, value interface{}) bool
	Delete(key Comparable) bool
	Search(key Comparable) (interface{}, bool)
	InOrderTraversal(visit func(key Comparable, value interface{}))
}
