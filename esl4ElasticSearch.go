package esl4ElasticSearch

import (
	"errors"
	"fmt"
	es "github.com/mattbaird/elastigo/lib"
	"unicode"
)

type Token struct {
	Type    string
	Connect string
}

func NewToken(typ, conn string) *Token {
	return &Token{typ, conn}
}

type Lex struct {
	code   []rune
	pos    int
	tokens []*Token
	index  int
}

const (
	BAD_T      = "Bad"
	ID_T       = "identifier"
	DIV_STAR_T = "/*"
	STAR_DIV_T = "*/"
	SEMI_T     = ";"
	COMMA_T    = ","
	LP_T       = "("
	RP_T       = ")"
	LB_T       = "["
	RB_T       = "]"
	LC_T       = "{"
	RC_T       = "}"
	ADD_T      = "+"
	COLON_T    = ":"
	OR_T       = "or"
	NOT_T      = "not"
	IN_T       = "in"

	MAX_OPER_LEN = 3
)

var (
	operator = map[string]string{DIV_STAR_T: DIV_STAR_T, STAR_DIV_T: STAR_DIV_T,
		SEMI_T: SEMI_T, COMMA_T: COMMA_T, LP_T: LP_T, RP_T: RP_T, LB_T: LB_T,
		RB_T: RB_T, LC_T: LC_T, RC_T: RC_T, ADD_T: ADD_T, COLON_T: COLON_T}
	keyword = map[string]string{OR_T: OR_T, NOT_T: NOT_T, IN_T: IN_T}
)

// lex : operator / keyword / identifier
func NewLex(code string) (*Lex, error) {
	lex := &Lex{[]rune(code), 0, []*Token{}, 0}
	for lex.pos < len(lex.code) {
		if unicode.IsSpace(lex.code[lex.pos]) {
			lex.pos++
			continue
		}

		if tk := lex.GetOper(); tk != nil {
			if tk.Type == DIV_STAR_T {
				for {
					end := lex.GetOper()
					if end != nil && end.Type == STAR_DIV_T {
						break
					}

					if lex.pos < len(lex.code) {
						lex.pos++
					} else {
						return nil, errors.New("miss end of remark")
					}
				}
			} else {
				lex.tokens = append(lex.tokens, tk)
			}

			continue
		}

		if tk := lex.GetKeyWord(); tk != nil {
			lex.tokens = append(lex.tokens, tk)
			continue
		}

		if tk := lex.GetId(); tk != nil {
			lex.tokens = append(lex.tokens, tk)
			continue
		}

		return nil, errors.New(fmt.Sprint("have bad token:", lex.pos, string(lex.code[lex.pos:lex.pos+10])+"..."))
	}

	return lex, nil
}

func (l *Lex) GetOper() *Token {
	for i := MAX_OPER_LEN; i > 0; i-- {
		if l.pos+i <= len(l.code) {
			conn := string(l.code[l.pos : l.pos+i])
			if typ, ok := operator[conn]; ok {
				l.pos += i
				return NewToken(typ, conn)
			}
		}
	}

	return nil
}

func (l *Lex) GetKeyWord() *Token {
	conn := []rune{}
	for i := l.pos; i < len(l.code) && unicode.IsLower(l.code[i]); i++ {
		conn = append(conn, l.code[i])
	}

	if len(conn) > 0 {
		str := string(conn)
		if typ, ok := keyword[str]; ok {
			l.pos += len(conn)
			return NewToken(typ, str)
		}
	}

	return nil
}

func (l *Lex) GetId() *Token {
	conn := []rune{}
	end2 := rune('"')
	end1 := rune('\'')
	for i := l.pos; i < len(l.code); i++ {
		conn = append(conn, l.code[i])
		if conn[0] != end1 && conn[0] != end2 {
			return nil
		}

		if len(conn) > 1 && conn[len(conn)-1] == conn[0] {
			l.pos += len(conn)
			return NewToken(ID_T, string(conn[1:len(conn)-1]))
		}
	}

	return nil
}

func (l *Lex) Pop() *Token {
	if l.index < len(l.tokens) {
		defer func() { l.index++ }()
		return l.tokens[l.index]
	}
	return nil
}

func (l *Lex) PushBack() {
	if l.index > 0 {
		l.index--
		return
	}
}

func (l *Lex) Empty() bool {
	return l.index == len(l.tokens)
}

// id : string | string + string
func ParseID(lex *Lex) *Token {
	id := lex.Pop()
	if id == nil {
		return nil
	}

	if id.Type != ID_T {
		lex.PushBack()
		return nil
	}

	add := lex.Pop()
	if add == nil {
		return id
	}

	if add.Type == ADD_T {
		if idpost := ParseID(lex); idpost != nil {
			return NewToken(ID_T, id.Connect+idpost.Connect)
		}
	}

	lex.PushBack()
	return id
}

//set: (id) | (id...)
func ParseSet(lex *Lex) ([]interface{}, error) {
	lp := lex.Pop()
	if lp == nil || lp.Type != LP_T {
		return nil, errors.New("miss start of set")
	}

	res := []interface{}{}
	for {
		if id := ParseID(lex); id != nil {
			if len(id.Connect) == 0 {
				return nil, errors.New("empty id in set")
			}

			res = append(res, id.Connect)
			continue
		}

		tk := lex.Pop()
		if tk == nil {
			return nil, errors.New("miss end of set")
		}

		switch tk.Type {
		case COMMA_T:
			//pass

		case ADD_T:
			return nil, errors.New("miss pre/post string of +")

		case RP_T:
			if len(res) == 0 {
				return res, errors.New("set is empty")
			}
			return res, nil

		default:
			return nil, errors.New("bad id in set:" + tk.Connect)
		}
	}
}

//range : [from:to] | [from:] | [:to]
func ParseRange(lex *Lex) (from, to string, err error) {
	lb := lex.Pop()
	if lb == nil || lb.Type != LB_T {
		err = errors.New("miss start of range")
		return
	}

	fid := ParseID(lex)
	if fid != nil && len(fid.Connect) > 0 {
		from = fid.Connect
	}

	split := lex.Pop()
	if split == nil || split.Type != COLON_T {
		err = errors.New("range must split by :")
		return
	}

	tid := ParseID(lex)
	if tid != nil && len(tid.Connect) > 0 {
		to = tid.Connect
	}

	rb := lex.Pop()
	if rb != nil && rb.Type == RB_T {
		if len(from) == 0 && len(to) == 0 {
			err = errors.New("range is empty")
		}
		return
	}

	err = errors.New("miss end of range")
	return
}

//id in (id) | id in (id...)
func ParseInSet(id *Token, lex *Lex) (interface{}, error) {
	set, err := ParseSet(lex)
	if err != nil {
		return nil, err
	}

	return es.Filter().Terms(id.Connect, set...), nil
}

//id not in (id) | id not in (id...)
func ParseNotInSet(id *Token, lex *Lex) (interface{}, error) {
	set, err := ParseSet(lex)
	if err != nil {
		return nil, err
	}

	flt := map[string]interface{}{"not": es.Filter().Terms(id.Connect, set...)}
	return es.CompoundFilter(flt), nil
}

//id in [from:to] | id in [from:] | id in [:to]
func ParseInRange(id *Token, lex *Lex) (interface{}, error) {
	from, to, err := ParseRange(lex)
	if err != nil {
		return nil, err
	}

	r := es.Range()
	if len(from) > 0 {
		r = r.Field(id.Connect).From(from)
	}
	if len(to) > 0 {
		r = r.Field(id.Connect).To(to)
	}

	return r, nil
}

//id not in [from:to] | id not in [from:] | id not in [:to]
func ParseNotInRange(id *Token, lex *Lex) (interface{}, error) {
	from, to, err := ParseRange(lex)
	if err != nil {
		return nil, err
	}

	r := es.Range()
	if len(from) > 0 {
		r = r.Field(id.Connect).From(from)
	}
	if len(to) > 0 {
		r = r.Field(id.Connect).To(to)
	}

	flt := map[string]interface{}{"not": r}
	return es.CompoundFilter(flt), nil
}

// id in set | id not in set | id in range | id not in range
func ParseContainer(id, op *Token, lex *Lex) (interface{}, error) {
	cot := lex.Pop()
	if cot == nil {
		return nil, errors.New(id.Connect)
	}
	lex.PushBack()

	switch op.Type {
	case IN_T:
		switch cot.Type {
		case LP_T:
			return ParseInSet(id, lex)

		case LB_T:
			return ParseInRange(id, lex)
		}

	case NOT_T:
		switch cot.Type {
		case LP_T:
			return ParseNotInSet(id, lex)

		case LB_T:
			return ParseNotInRange(id, lex)
		}
	}

	return nil, errors.New(fmt.Sprint("operator error:", cot.Connect, "or", op.Connect))
}

// id in set | id not in set | id in range | id not in range
func ParseExpress(lex *Lex) (interface{}, error) {
	id := ParseID(lex)
	if id == nil {
		return nil, errors.New("miss item in expression")
	}

	op1 := lex.Pop()
	if op1 == nil {
		return nil, errors.New(id.Connect + " miss container")
	}

	if op1.Type == NOT_T {
		op2 := lex.Pop()
		if op2 == nil || op2.Type != IN_T {
			return nil, errors.New(id.Connect + " miss in OR container")
		}
	}

	return ParseContainer(id, op1, lex)
}

// cond | cond || cond  logic-or
func ParseCond(lex *Lex) (interface{}, error) {
	res := []interface{}{"or"}
	for !lex.Empty() {
		exp, err := ParseExpress(lex)
		if err != nil {
			return nil, err
		}

		res = append(res, exp)

		if tk := lex.Pop(); tk.Type == SEMI_T {
			break
		} else if tk.Type != OR_T {
			return nil, errors.New(fmt.Sprint("invalidate operator", tk.Connect, `may be need ";" or "union"`))
		}
	}

	if len(res) == 1 {
		return nil, errors.New("expression is nil")
	}
	if len(res) == 2 {
		return res[1], nil
	}

	return es.CompoundFilter(res...), nil
}

// cond ; cond logic-and
func ParseConds(lex *Lex) (interface{}, error) {
	res := []interface{}{"and"}

	for !lex.Empty() {
		end := lex.Pop()
		if end == nil {
			return nil, errors.New("miss end of section")
		}

		lex.PushBack()

		if end.Type == RC_T {
			break
		}

		cond, err := ParseCond(lex)
		if err != nil {
			return nil, err
		}

		res = append(res, cond)
	}

	if len(res) == 1 {
		return nil, errors.New("query condition is nil")
	}

	if len(res) == 2 {
		return res[1], nil
	}

	return es.CompoundFilter(res...), nil
}

//section ; section union section
func ParseSection(lex *Lex) (interface{}, error) {
	res := []interface{}{"or"}

	for {
		start := lex.Pop()
		if start == nil || start.Type != LC_T {
			return nil, errors.New("miss start of section")
		}

		lh := lex.Pop()
		if lh == nil {
			return nil, errors.New("miss body of section")
		}

		if lh.Type == RC_T {
			break
		}
		lex.PushBack()

		if lh.Type == LC_T {
			sec, err := ParseSections(lex)
			if err != nil {
				return nil, err
			}
			res = append(res, sec)
		} else {
			conds, err := ParseConds(lex)
			if err != nil {
				return nil, err
			}
			res = append(res, conds)
		}

		end := lex.Pop()
		if end == nil || end.Type != RC_T {
			return nil, errors.New("miss end of section")
		}

		union := lex.Pop()
		if union == nil {
			break
		}
		if union.Type != OR_T {
			lex.PushBack()
			break
		}
	}

	if len(res) == 1 {
		return nil, errors.New("section is empty")
	}

	if len(res) == 2 {
		return res[1], nil
	}

	return es.CompoundFilter(res...), nil
}

func ParseSections(lex *Lex) (interface{}, error) {
	res := []interface{}{"and"}

	for !lex.Empty() {
		start := lex.Pop()
		if start == nil {
			break
		}

		lex.PushBack()
		if start.Type != LC_T {
			break
		}

		sec, err := ParseSection(lex)
		if err != nil {
			return nil, err
		}
		res = append(res, sec)
	}

	if len(res) == 1 {
		return nil, errors.New("query condition is nil")
	}

	if len(res) == 2 {
		return res[1], nil
	}

	return es.CompoundFilter(res...), nil
}

func ParseEsl(esl string) (interface{}, error) {
	if len(esl) > 65535 {
		return nil, errors.New("too large then 64KB")
	}

	lex, err := NewLex(esl)
	if err != nil {
		return nil, err
	}

	filter, err := ParseSections(lex)

	if !lex.Empty() {
		return nil, errors.New("error" + lex.Pop().Connect)
	}

	return filter, err
}
