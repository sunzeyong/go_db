package server

func assert(i bool, msg string) {
	if !i {
		panic("internal err: " + msg)
	}
}
