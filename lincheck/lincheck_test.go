package lincheck

import "testing"

func put(c int, k, v string, call, ret int64) Operation {
	return Operation{Client: c, Kind: "put", Key: k, Value: v, Call: call, Return: ret}
}
func app(c int, k, v string, call, ret int64) Operation {
	return Operation{Client: c, Kind: "append", Key: k, Value: v, Call: call, Return: ret}
}
func get(c int, k, out string, call, ret int64) Operation {
	return Operation{Client: c, Kind: "get", Key: k, Output: out, Call: call, Return: ret}
}

func TestCheck(t *testing.T) {
	cases := []struct {
		name string
		ok   bool
		h    []Operation
	}{
		{"empty", true, nil},
		{"sequential", true, []Operation{put(1, "x", "a", 1, 2), get(2, "x", "a", 3, 4)}},
		{"read of missing key is empty", true, []Operation{get(1, "x", "", 1, 2)}},
		{"stale read after write returned", false, []Operation{
			put(1, "x", "a", 1, 2), put(1, "x", "b", 3, 4), get(2, "x", "a", 5, 6)}},
		{"concurrent read may see old or new", true, []Operation{
			put(1, "x", "a", 1, 2), put(1, "x", "b", 3, 6), get(2, "x", "a", 4, 5)}},
		{"concurrent read may see new", true, []Operation{
			put(1, "x", "a", 1, 2), put(1, "x", "b", 3, 6), get(2, "x", "b", 4, 5)}},
		// Two reads that both overlap the write but not each other must not see
		// new-then-old: once a read saw "b", later reads must too.
		{"new then old", false, []Operation{
			put(1, "x", "b", 1, 10), get(2, "x", "b", 2, 3), get(3, "x", "", 4, 5)}},
		{"appends in some order", true, []Operation{
			app(1, "x", "1", 1, 5), app(2, "x", "2", 2, 6), get(3, "x", "21", 7, 8)}},
		{"append applied twice", false, []Operation{
			app(1, "x", "1", 1, 2), get(2, "x", "11", 3, 4)}},
		{"pending write may have happened", true, []Operation{
			put(1, "x", "a", 1, Pending), get(2, "x", "a", 2, 3)}},
		{"pending write may not have happened", true, []Operation{
			put(1, "x", "a", 1, Pending), get(2, "x", "", 2, 3)}},
		{"keys are independent", false, []Operation{
			put(1, "x", "a", 1, 2), put(1, "y", "b", 3, 4), get(2, "y", "", 5, 6)}},
		{"value nobody wrote", false, []Operation{put(1, "x", "a", 1, 2), get(2, "x", "z", 3, 4)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := Check(c.h)
			if (err == nil) != c.ok {
				t.Fatalf("linearizable = %v, want %v (%v)", err == nil, c.ok, err)
			}
		})
	}
}

// Many concurrent appends to one key: the search has to stay fast on histories
// the size the simulator produces.
func TestCheckManyConcurrent(t *testing.T) {
	var h []Operation
	var want string
	for i := 0; i < 200; i++ {
		v := string(rune('a' + i%26))
		h = append(h, app(i%5, "k", v, int64(10*i), int64(10*i+15)))
		want += v
	}
	h = append(h, get(9, "k", want, 5000, 5001))
	if err := Check(h); err != nil {
		t.Fatal(err)
	}
}
