package libstore

type KeyNotFound struct{}
type ItemNotFound struct{}
type WrongServer struct{}
type ItemExists struct{}
type NotReady struct{}
type InvalidStatus struct{}

func (e *KeyNotFound) Error() string   { return "key not found" }
func (e *ItemNotFound) Error() string  { return "item not found" }
func (e *WrongServer) Error() string   { return "wrong server" }
func (e *ItemExists) Error() string    { return "item exists" }
func (e *NotReady) Error() string      { return "not ready" }
func (e *InvalidStatus) Error() string { return "invalid status" }
