package sqlkit

// Rewrite used to rewrite sql
// TODO ...
type Rewrite struct {
	Query  string `json:"query"`
	ArgOps map[uint]ArgOp
}

func (r Rewrite) Args(args ...interface{}) ([]interface{}, error) {
	if r.ArgOps == nil {
		return args, nil
	}
	var result []interface{}
	for i := range args {
		if op, ok := r.ArgOps[uint(i)]; ok {
			if op == DelOp {
				continue
			}
		}
		result = append(result, args[i])
	}
	return result, nil
}

type ArgOp int

const (
	NoOp ArgOp = iota
	DelOp
)
