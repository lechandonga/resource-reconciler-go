package integration

func (e *testEnv) qAdd(name string) error {
	return e.ctrl.Enqueue(name)
}
