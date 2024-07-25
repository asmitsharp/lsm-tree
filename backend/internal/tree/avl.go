package tree

type AVLNode struct {
	Key         Comparable
	Value       interface{}
	Left, Right *AVLNode
	Height      int
}

type AVLTree struct {
	Root *AVLNode
}

func NewAVLTree() *AVLTree {
	return &AVLTree{}
}

func (t *AVLTree) Insert(key Comparable, value interface{}) bool {
	t.Root = insert(t.Root, key, value)
	return true
}

func (t *AVLTree) Delete(key Comparable) bool {
	t.Root = delete(t.Root, key)
	return false
}

func (t *AVLTree) Search(key Comparable) (interface{}, bool) {
	return search(t.Root, key)
}

func (t *AVLTree) InOrderTraversal(visit func(key Comparable, value interface{})) {
	inOrderTraversal(t.Root, visit)
}

//-------------------------UTILITY FUNCTIONS-------------------------------------//

func insert(node *AVLNode, key Comparable, value interface{}) *AVLNode {
	if node == nil {
		return &AVLNode{Key: key, Value: value, Height: 1}
	}

	if key.Compare(node.Key) < 0 {
		node.Left = insert(node.Left, key, value)
	} else if key.Compare(node.Key) > 0 {
		node.Right = insert(node.Right, key, value)
	} else {
		node.Key = key
		node.Value = value
		return node
	}

	updateHeight(node)
	return balance(node)
}

func delete(node *AVLNode, key Comparable) *AVLNode {
	if node == nil {
		return nil
	}

	if key.Compare(node.Key) < 0 {
		node.Left = delete(node.Left, key)
	} else if key.Compare(node.Key) > 0 {
		node.Right = delete(node.Right, key)
	} else {
		if node.Left == nil {
			return node.Right
		} else if node.Right == nil {
			return node.Left
		}

		tmp := minValueNode(node)
		node.Key = tmp.Key
		node.Value = tmp.Value
		node.Right = delete(node.Right, tmp.Key)
	}

	if node == nil {
		return node
	}
	updateHeight(node)
	return balance(node)
}

func search(node *AVLNode, key Comparable) (interface{}, bool) {
	if node == nil {
		return nil, false
	}

	if key.Compare(node.Key) == 0 {
		return node.Value, true
	} else if key.Compare(node.Key) < 0 {
		return search(node.Left, key)
	} else {
		return search(node.Right, key)
	}
}

func inOrderTraversal(node *AVLNode, visit func(key Comparable, value interface{})) {
	if node != nil {
		inOrderTraversal(node.Left, visit)
		visit(node.Key, node.Value)
		inOrderTraversal(node.Right, visit)
	}
}

func balance(node *AVLNode) *AVLNode {
	if balanceFactor(node) > 1 {
		if balanceFactor(node.Left) < 0 {
			node.Left = leftRotate(node.Left)
		}
		return rightRotate(node)
	}
	if balanceFactor(node) < -1 {
		if balanceFactor(node.Right) > 0 {
			node.Right = rightRotate(node.Right)
		}
		return leftRotate(node)
	}
	return node
}

func height(node *AVLNode) int {
	if node == nil {
		return 0
	}
	return node.Height
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func balanceFactor(node *AVLNode) int {
	if node == nil {
		return 0
	}
	return height(node.Left) - height(node.Right)
}

func updateHeight(node *AVLNode) {
	node.Height = 1 + max(height(node.Left), height(node.Right))
}

func rightRotate(node *AVLNode) *AVLNode {
	n1 := node.Left
	n2 := n1.Right

	n1.Right = node
	node.Left = n2

	updateHeight(node)
	updateHeight(n1)

	return n1
}

func leftRotate(node *AVLNode) *AVLNode {
	n1 := node.Right
	n2 := n1.Left

	n1.Left = node
	node.Right = n2

	updateHeight(node)
	updateHeight(n1)

	return n1
}

func minValueNode(node *AVLNode) *AVLNode {
	current := node
	for current.Left != nil {
		current = current.Left
	}
	return current
}

//--------------------------------------END-------------------------------------//
